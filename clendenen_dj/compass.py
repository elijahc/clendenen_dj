import datajoint as dj
import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc
import getpass
from .utils import create_config

create_config()

lab = dj.Schema('clendenen_lab')

@lab
class Host(dj.Lookup):
    definition = """
    host : varchar(20)   # unique user name
    ---
    """
    contents=[
        ['spruce.symlink.pw'],
        ['yew.symlink.pw'],
    ]

@lab
class User(dj.Lookup):
    definition = """
    username : varchar(20)   # unique user name
    ---
    """
    contents=[
        ['elijahc'],
        ['clendenen'],
        ['mack'],
        ['wickers'],
        ['shipton'],
        ['leopold'],
    ]

schema = dj.schema('compass_common')

@lab
class CompassTable(dj.Lookup):
    definition = """
    type                           : enum('encounter','procedure','flowsheet','lab','diagnosis')
    ---
    partition_col=null             : varchar(20)
    """
    contents = [
        ('diagnosis','Provenance'),
        ('procedure','order_name'),
        ('flowsheet','display_name'),
        ('lab','lab_component_name'),
    ]

@schema
class CompassFile(dj.Manual):
    definition = """
    -> CompassTable
    ---
    version                            : int unsigned
    file                               : attach@compass
    """

    def fetch_path(self, base='/data/compass/raw/', dataset='SWAN'):
        keys = self.fetch(as_dict=True)
        bpath = ['/data/compass/raw/{}_{}'.format(dataset,k['version']) for k in keys]
        return bpath

@schema
class ProcedureName(dj.Imported):
    definition = """
    procedure                        : varchar(150)
    ---
    -> CompassFile
    """
    key_source = CompassFile & 'type = "procedure"'

    def make(self, key):
        key = (CompassFile * CompassTable & key).fetch1()
        print('populating for {file}'.format(**key))
        tab = csv.read_csv(key['file'])
        u_names = list(tab[key['partition_col']].unique())
        rows = [dict(procedure=n,type=key['type']) for n in u_names]
        self.insert(rows,skip_duplicates=True)

@schema
class LabName(dj.Imported):
    definition = """
    lab                        : varchar(150)
    ---
    -> CompassFile
    """

    key_source = CompassFile & 'type = "lab"'

    def make(self, key):
        key = (CompassFile * CompassTable & key).fetch1()
        print('populating for {file}'.format(**key))
        tab = csv.read_csv(key['file'])
        u_names = list(tab[key['partition_col']].unique())
        rows = [dict(lab=str(n).upper(),type=key['type']) for n in u_names]
        self.insert(rows,skip_duplicates=True)

def load_flowsheet_dataset(path):
    dat = ds.dataset(path,format='parquet').to_table()
    return dat