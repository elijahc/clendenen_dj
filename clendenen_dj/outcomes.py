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
from . import cohort

schema = dj.schema('clendenen_cohort')

class ProcedureOutcome(dj.Computed):
    definition = """
    -> cohort.Cohort.Encounter
    days_from_dob                    : smallint unsigned
    ---
    day                              : smallint
    """

    def make(self,key):
        cca = self.cached_procedures()
        a = (Alignment() & key).fetch(format='frame').offset.unique()[0]
        if key['encounter_id'] in cca.encounter_id.values:
            recs = cca[cca.encounter_id==key['encounter_id']].reset_index()
            recs['day'] = recs.days_from_dob - a
            recs = recs.query(self.q)
            if len(recs)>0:
                recs['cohort_id']=key['cohort_id']
                recs = recs.reset_index().set_index(['cohort_id','days_from_dob'])
                self.insert(recs,skip_duplicates=True,ignore_extra_fields=True)

    def cached_procedures(self):
        if not hasattr(self,'procedures'):
            print('cacheing procedures...')
            cca = self.load_procedures(procedures=self.event_procedures).to_pandas()

            # Remove nan encounter ids
            cca = cca[~cca.encounter_id.isna()].rename(columns={'days_from_dob_procstart':'days_from_dob'})
            cca.encounter_id = cca.encounter_id.astype(np.int64)
            cca.days_from_dob = cca.days_from_dob.astype(np.int64)
            self.procedures = cca

        return self.procedures

    def load_procedures(self, procedures):
        fp = (CompassFile & 'type = "procedure"').fetch1('file')
        encounters  = np.unique(cohort.Cohort.Encounter().fetch('encounter_id'))
        tab = csv.read_csv(fp)
        tab = tab.filter(
            pc.is_in(tab['order_name'],options=pc.SetLookupOptions(value_set=pa.array(procedures)))
        )
        tab = tab.filter(
            pc.is_in(tab['encounter_id'],options=pc.SetLookupOptions(value_set=pa.array(encounters)))
        )
        return tab

@schema
class CohortVVECMO(ProcedureOutcome):
    event_procedures = [
        'HB ECMO/ECLS INITIAL VENO-VENOUS',
        'HB ECMO/ECLS EACH DAY VENO-VENOUS',
    ]
    q = '0 <= day < 100'

@schema
class CohortCardiacArrest(ProcedureOutcome):
    event_procedures = [
        'ED CPR PROCEDURE',
        'PR HEART/LUNG RESUSCITATION (CPR)',
        'HB CPR'
    ]
    q = '0 <= day < 100'

@schema
class CohortMechanicalSupport(ProcedureOutcome):
    event_procedures = [                                
        'HB OR-CATH INTRA-AORTIC BALLOON PUMP (06)',
        'HB ECMO/ECLS INITIAL VENO-ARTERIAL',      
        'HB ECMO/ECLS EACH DAY VENO-ARTERIAL',
        'IMPELLA DEVICE INSERTION / REPAIR'
    ] 

    q = '0 <= day < 100'

@schema
class CohortBleed(ProcedureOutcome):
    event_procedures = [
            'Control Bleeding in Mediastinum, Open Approach',
            'Control Bleeding in Chest Wall, Open Approach',
            'POST OPERATIVE BLEEDING HEART  NO BYPASS',
            'POST OP BLEEDING HEART ON BYPASS'
        ]
    q = '0 <= day'

outcomes_export = {
    'vv_ecmo':CohortVVECMO,
    'cardiac_arrest':CohortCardiacArrest,
    'mechanical_support':CohortMechanicalSupport,
    'bleed':CohortBleed,
}