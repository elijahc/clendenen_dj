import datajoint as dj
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc
import getpass
from .utils import create_config
print(create_config())

from .compass import User, ProcedureName, CompassFile


schema = dj.schema('clendenen_cohort')

@schema
class Cohort(dj.Manual):
    definition = """
    cohort_id                        : varchar(20)
    ---
    -> User
    # cohort_type                      : enum('procedure','encounter')
    name                             : varchar(20)
    person_id=null                   : blob
    procedures                       : blob
    cohort_description=null          : varchar(1024)
    created_at=CURRENT_TIMESTAMP     : timestamp
    """

    class Procedure(dj.Part):
        definition = """
        -> master
        -> ProcedureName
        ---
        """

    def new(self, procedures, name, user=None, description=None, person_id=None):
        user = user or getpass.getuser()
        cid = '{}/{}'.format(user,name)
        if not cid in self.fetch('cohort_id'):
            r = {
                'cohort_id':cid,
                'username':user,
                'name':name,
                'procedures':procedures,
                'cohort_description':description}

            if person_id is not None:
                r['person_id'] = person_id
                
            self.insert1(r,skip_duplicates=True)

            Cohort.Procedure.insert(
                [dict(cohort_id=cid,procedure=p) for p in procedures],
                skip_duplicates=True,
            )

    def encounters(self):
        a = Alignment() & (Cohort.Procedure & self)
        return a

    def load_procedures(self, procedures=None, person_id=None):
        procedures = procedures or self.Procedure.fetch('procedure')

        fp = (CompassFile & 'type = "procedure"').fetch1('file')
        tab = csv.read_csv(fp)
        tab = tab.filter(
            pc.is_in(tab['order_name'],options=pc.SetLookupOptions(value_set=pa.array(procedures)))
        )
        if person_id is not None:
            tab = tab.filter(
                pc.is_in(tab['person_id'],options=pc.SetLookupOptions(value_set=pa.array(person_id)))
            )
        return tab

    def load(self,name):
        (self & {'name':name}).fetch(as_type())

def load_flowsheet_dataset(path):
    dat = ds.dataset(path,format='parquet').to_table()
    return dat

@schema
class Alignment(dj.Computed):
    definition = """
    person_id                        : bigint unsigned
    encounter_id                     : bigint unsigned
    -> ProcedureName
    ---
    offset                           : smallint unsigned
    """

    key_source = ProcedureName & Cohort.Procedure

    def make(self, key):
        tab = self.load_procedures([key['procedure']])
        for b in tab.to_batches(max_chunksize=200):
            b = b.to_pandas().rename(columns={'days_from_dob_procstart':'offset'})
            b.offset = pd.to_numeric(b.offset,errors='coerce')
            recs = b[['person_id','encounter_id','order_name','offset']].dropna()
            self.insert(recs.to_numpy(),skip_duplicates=True)

    def load_procedures(self, procedures=None):
        procedures = procedures or self.fetch('procedure')

        fp = (CompassFile & 'type = "procedure"').fetch1('file')
        tab = csv.read_csv(fp)
        tab = tab.filter(
            pc.is_in(tab['order_name'],options=pc.SetLookupOptions(value_set=pa.array(procedures)))
        )
        return tab

@schema
class CohortDelirium(dj.Computed):
    definition = """
    -> Cohort.Procedure
    encounter_id                     : bigint unsigned
    ---
    day                              : smallint
    days_from_dob                    : smallint unsigned
    time                             : time
    value                            : enum('U','Y','N')
    """

    def make(self, key):
        for b in self.load_batches(key):
            b['day'] = b.days_from_dob.astype(int) - b.offset.astype(int)
            recs = b[['cohort_id','procedure','encounter_id','day','days_from_dob','time','value']]
            self.insert(recs.to_numpy(),skip_duplicates=True)

    def load_batches(self, key):
        k = Cohort.Alignment & key
        k_df = pd.DataFrame(k.fetch(as_dict=True))
        f = (CompassFile & 'type = "flowsheet"' & key).fetch1('file')
        dat = ds.dataset('./compass/flowsheet',format='parquet').to_table()
        tab = dat.filter(
            pc.is_in(dat['encounter_id'],options=pc.SetLookupOptions(value_set=pa.array(k_df.encounter_id.unique())))
        )
        for b in tab.to_batches(max_chunksize=200):
            df = b.to_pandas().rename(columns={
                'flowsheet_time':'time',
                'flowsheet_value':'value',
                'flowsheet_days_since_birth':'days_from_dob'})
            df['cohort_id'] = key['cohort_id']
            df['procedure'] = key['procedure']
            df.days_from_dob = pd.to_numeric(df.days_from_dob,errors='coerce')
            df['value'] = df['value'].replace({
                'Not delirious- CAM-':'N',
                'Unable to assess':'U',
                'Delirious- CAM+':'Y',
                '':np.nan})

            yield pd.merge(df.dropna(),k_df,on=['cohort_id','encounter_id','procedure'])

        # df.flowsheet_days_since_birth = pd.to_numeric(df.flowsheet_days_since_birth,errors='coerce')
        # df = df.dropna()
        # keys = [(key['file_id'],r['encounter_id'],int(r['flowsheet_days_since_birth']),None,r['flowsheet_time'],r['flowsheet_value']) for i,r in df.iterrows()]
        # n_iter = 100
        # while len(keys)>n_iter:
        #     chunk = keys[:n_iter]
        #     self.insert(chunk,skip_duplicates=True)
        #     del keys[:n_iter]

class Index(object):
    def __init__(self):
        self.cohorts = pd.DataFrame(Cohort().fetch('cohort_id','username','cohort_description','created_at',as_dict=True))

    def list(self):
        print('cohort_id')
        for c in self.cohorts.cohort_id.values:
            print('- ',c)

    def pull(self, cid):
        if not cid in self.cohorts.cohort_id.values:
            raise ValueError('{} not in {}'.format(cid,self.list()))

        c = (Cohort() & {'cohort_id':cid}).fetch1()
        self.cohort_id = c['cohort_id']
        self.person_id = c['person_id']
        self.procedures = c['procedures']

        a = Alignment() & (Cohort.Procedure() & {'cohort_id':cid})
        a = pd.DataFrame(a.fetch(order_by='encounter_id',as_dict=True))
        if self.person_id is not None:
            self.alignment = a[a.person_id.isin(self.person_id)]
        else:
            self.alignment = a
        
        self.encounters = self.alignment.encounter_id.unique()

        return self

    def __repr__(self):
        if hasattr(self, 'alignment'):
            return self.alignment.__repr__()
        else:
            return self.__str__()