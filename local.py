import os.path

from fabric import Connection, Config, transfer
import patchwork.transfers
import subprocess
from os.path import basename, join
import yaml
import pickle as pkl

USERNAME = keyring.get_password('server', 'username')
PASSWORD = keyring.get_password('server', 'password')
CONFIG = Config(overrides={'sudo': {'password': PASSWORD}})

DATA_FOLDER = '/home/jorisg/data'
PROJECTS_FOLDER = '/home/jorisg/projects'
JOB_FOLDER = '/home/jorisg/jobs'
RETURN_FOLDER = '/home/jorisg/return_data'

LOCAL_RETURN_FOLDER = '/Users/jg/projects/server/return_data'


IP = '10.0.0.1' #controller ip in cluster network
PORT = 22

#get all finished jobs back from the cluster
def get_finished_jobs():
    subprocess.run(["rsync", '-azP', f'{USERNAME}@{IP}:{RETURN_FOLDER}/',
                    LOCAL_RETURN_FOLDER])

#create the jobs for grid search and send them to the cluster
if __name__ == '__main__':
    job_id = 1 #id of the first job to be created

    settings_dict = {}
    project_folder = '/Users/jg/projects/ai/LGBM'
    data_folder = '/Users/jg/projects/test_data'
    entry_point = 'tuning_first_stage.py'
    CPU_requirement = 10
    RAM_requirement = 60
    for n_estimators in [10, 1000, 10000]:
        for learning_rate in [0.01, 0.001, 0.0001]:
            for max_depth in [6]:
                for num_leaves in [2**6]:
                    for early_stopping_rounds in [50]:
                        for max_bin in [4]:
                            for n_retrain_eras in [2, 20, 60]:
                                job_id += 1

                                settings = {'max_bin': max_bin, 'n_estimators': n_estimators,
                                             'learning_rate': learning_rate, 'max_depth': max_depth,
                                             'early_stopping_rounds': early_stopping_rounds,  'num_leaves': num_leaves,
                                            'n_retrain_eras': n_retrain_eras, 'job_ids': []}
                                settings_dict[job_id] = settings


                                settings_dict[job_id]['job_ids'].append(job_id)

                                job = f'./jobs/{job_id}.yaml'

                                arguments = {'max_bin': max_bin, 'n_estimators': n_estimators,
                                             'learning_rate': learning_rate, 'max_depth': max_depth,
                                             'early_stopping_rounds': early_stopping_rounds, 'num_leaves': num_leaves,
                                             'n_retrain_eras': n_retrain_eras, 'settingID': job_id}
                                argstring = entry_point
                                for arg, value in arguments.items():
                                    argstring += f' --{arg} {value}'
                                print(argstring)

                                job_description = {'job_id': job_id, 'project_folder': os.path.basename(project_folder),
                                                   'data_folder': os.path.basename(data_folder),
                                                   'entry_point': entry_point, 'arguments': arguments, 'CPU_requirement': CPU_requirement,
                                                   'RAM_requirement': RAM_requirement}
                                os.makedirs('./jobs', exist_ok=True)
                                with open(job, 'w') as f:
                                    yaml.dump(job_description, f)

                                c = Connection(host=IP, port=PORT, user=USERNAME, config=CONFIG)
                                patchwork.transfers.rsync(c, data_folder, DATA_FOLDER, exclude=['.git', '.idea', 'return_data'])
                                patchwork.transfers.rsync(c, project_folder, PROJECTS_FOLDER, exclude=['.git', '.idea', 'return_data', 'models', 'predictions', 'target_ensemble_nomi_v4_20_only'])
                                transfer.Transfer(c).put(job, JOB_FOLDER)
    with open('settings_first_stage.pkl', 'wb') as f:
        pkl.dump(settings_dict, f)

