import os
import getpass
import pandas as pd
import datajoint as dj
import getpass

DEV_BASE = os.path.expanduser('~/data/compass/raw')

local_config = os.path.expanduser('~/.dj_config.json')

def create_config():
    if not os.path.exists(os.path.expanduser('~/.dj_cache/compass')):
        os.makedirs(os.path.expanduser('~/.dj_cache/compass'))

    if os.path.exists(local_config):
        dj.config.load(local_config)
    else:
        passw = getpass.getpass(prompt='Please enter DataJoint password (same as spruce): ')
        dj.config['database.host'] = 'yew.symlink.pw'
        dj.config['database.user'] = '{}'.format(getpass.getuser())
        dj.config['database.password'] = passw
        dj.config['stores'] = {
            'compass': {
                'protocol': 'file',
                'location': os.path.expanduser('~/.dj_cache/compass'),
            }
        }
        print(dj.conn())
        answer = input('save config? [yes, No]: ')
        if answer == 'yes':
            dj.config.save(os.path.expanduser('~/.dj_config.json'))

from . import cohort
from . import compass

def migrate_compass(base=None):
    mbp_base = base or DEV_BASE

    compass.CompassFile().insert1((
        'flowsheet',20210210,
        os.path.join(mbp_base,'SWAN_20210210','Table2_Flowsheet.csv')),
        skip_duplicates=True)

    compass.CompassFile().insert1((
        'lab',20210210,
        os.path.join(mbp_base,'SWAN_20210210','Table3_Lab.csv')),skip_duplicates=True)

    compass.CompassFile().insert1((
        'procedure',20210210,
        os.path.join(mbp_base,'SWAN_20210210','Table6_Procedures.csv')),skip_duplicates=True)

    compass.CompassFile().insert1(('diagnosis',20210210,os.path.join('/Users/elijahc/data/compass/raw/SWAN_20210210/Table7_DX.csv')),skip_duplicates=True)

    compass.ProcedureName().populate()
    compass.LabName().populate()

    
def migrate_cohorts():
    endocarditis = [
        'TRICUSPID VALVE REPLACEMENT/REPAIR',
        'PULMONARY VALVE REPLACEMENT/REPAIR',
        'MITRAL VALVE REPLACEMENT/REPAIR',
        'AORTIC VALVE REPLACEMENT/REPAIR',
    ]

    valve_procs = endocarditis + ['CORONARY ARTERY BYPASS GRAFT TIMES 1-5 (CABG)']

    # Cohort().new(name='endocarditis', procedures=endocarditis, person_id=np.load('./endo_person_id.npy'),
        # description='Patients who have ever received an endocarditis diagnosis and valve surgery')
    cohort.Cohort().new(user='wickers', name='TEG', procedures=valve_procs)
    cohort.Alignment().populate(display_progress=True)
