import os
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import uproot as up
import glob

# ______________________________________________________________________________________________________
def count_events(file, tree_name):
    #fileup = up.open(file)
    #tree = fileup[tree_name]
    
    # TODO: remove hardcoding num entries
    return 100000

# ______________________________________________________________________________________________________
def get_file_event_counts(file_list, tree_name):
    with ThreadPoolExecutor() as executor:
        event_counts = list(executor.map(count_events, file_list, [tree_name]*len(file_list)))
    return dict(zip(file_list, event_counts))

# ______________________________________________________________________________________________________
def get_optimized_file_list(file_event_counts, max_events):
    total_events = 0
    optimized_file_list = []

    for file, event_count in file_event_counts.items():
        total_events += event_count
        optimized_file_list.append(file)

        # Print current state of variables for debugging
        #print(f'File added: {file}')
        #print(f'Event count for file: {event_count}')
        #print(f'Total events so far: {total_events}')
        if total_events + event_count > max_events:
            break

    # Print final results
    print(f'Final file list: {optimized_file_list}')
    print(f'Total number of events: {total_events}')

    return optimized_file_list



def concatenate_files(file, tree_name, variables):

    fileup = up.open(file)
    tree = fileup[tree_name]
    data = tree.arrays(variables, library="pd")
        
    if set(data.columns) != set(variables):
        print(f"Unexpected columns in DataFrame: {set(data.columns) - set(variables)}")

    return data

def concatenate_files_wrapper(args):
    return concatenate_files(*args)

# ___________________________________________________________________________________________________
class Process:
    def __init__(self, name, files, category, weight, max_events):
        self.name = name     
        # Get optimized file list
        self.files = files
        self.category = category
        self.weight = weight
        self.max_events = int(max_events)

    def df(self, variables=[]):

        # Create a list of tuples each containing (file, tree_name, variables)
        # generate dict with filename: nevents
        file_event_counts = get_file_event_counts(glob.glob(self.files), "events")
        
        self.files = get_optimized_file_list(file_event_counts, max_events)
        
        tasks = [(file, "events", variables) for file in self.files]

        #df = concatenate_files(self.files, "events", variables, self.max_events)
        
        # Use as many cores as available
        with ProcessPoolExecutor() as executor:
            results = list(executor.map(concatenate_files_wrapper, tasks))
            
        # results is a list of DataFrames
        df = pd.concat(results, ignore_index=True)       
        df["target"] = self.category

        nev = len(df.index)
        if self.max_events > nev:
            print(
                "WARNING: requested {}, but found only {} events in {}, using only {}".format(
                    self.max_events, nev, self.name, nev
                )
            )

        if self.max_events < nev:
            return df.iloc[: self.max_events]
        else:
            return df


# _____________________________________________________________________________________________________
# data


final_states = {
    "Hbb": 0,
    "Hcc": 1,
    "Hss": 2,
    "Hgg": 3,
    "Htautau": 4,
    "Huu": 5,
    "Hdd": 6,
    "Hbs": 7,
    "Hbd": 8,
    "Hsd": 9,
    "Hcu": 10,       
    "HWW": 11,
    "HZZ": 12,
}

"""
final_states = {
    "Hbb": 0,
    "Hcc": 1,
    "Hss": 2,
}
"""

tag = "ip1p0"

training_vars = [
    "jet1_scoreU_{}".format(tag),
    "jet1_scoreD_{}".format(tag),
    "jet1_scoreB_{}".format(tag),
    "jet1_scoreC_{}".format(tag),
    "jet1_scoreS_{}".format(tag),
    "jet1_scoreG_{}".format(tag),
    "jet1_scoreTAU_{}".format(tag),
    "jet2_scoreU_{}".format(tag),
    "jet2_scoreD_{}".format(tag),
    "jet2_scoreB_{}".format(tag),
    "jet2_scoreC_{}".format(tag),
    "jet2_scoreS_{}".format(tag),
    "jet2_scoreG_{}".format(tag),
    "jet2_scoreTAU_{}".format(tag),
    "d_23_{}".format(tag),
    "d_34_{}".format(tag),
    "d_45_{}".format(tag),
    "d_56_{}".format(tag),
    "d_67_{}".format(tag),
    "d_78_{}".format(tag),
    "d_89_{}".format(tag),
]

spectator_vars = [
    'jet1_scoreU_{}'.format(tag), 'jet1_scoreD_{}'.format(tag), 'jet1_scoreTAU_{}'.format(tag), 'jet1_scoreB_{}'.format(tag), 'jet1_scoreC_{}'.format(tag), 'jet1_scoreS_{}'.format(tag), 'jet1_scoreG_{}'.format(tag), 'jet2_scoreU_{}'.format(tag), 'jet2_scoreD_{}'.format(tag), 'jet2_scoreTAU_{}'.format(tag), 'jet2_scoreB_{}'.format(tag), 'jet2_scoreC_{}'.format(tag), 'jet2_scoreS_{}'.format(tag), 'jet2_scoreG_{}'.format(tag), 'U_{}'.format(tag), 'D_{}'.format(tag), 'TAU_{}'.format(tag), 'B_{}'.format(tag), 'C_{}'.format(tag), 'S_{}'.format(tag), 'G_{}'.format(tag), 'd_12_{}'.format(tag), 'd_23_{}'.format(tag), 'd_34_{}'.format(tag), 'd_45_{}'.format(tag), 'd_56_{}'.format(tag), 'd_67_{}'.format(tag), 'd_78_{}'.format(tag), 'd_89_{}'.format(tag), 'jet1_e_{}'.format(tag), 'jet1_p_{}'.format(tag), 'jet1_theta_{}'.format(tag), 'jet2_e_{}'.format(tag), 'jet2_p_{}'.format(tag), 'jet2_theta_{}'.format(tag), 'jet1_nconst_{}'.format(tag), 'jet1_nel_{}'.format(tag), 'jet1_nmu_{}'.format(tag), 'jet2_nconst_{}'.format(tag), 'jet2_nel_{}'.format(tag), 'jet2_nmu_{}'.format(tag), 'event_njet_{}'.format(tag), 'M_jj_{}'.format(tag), 'M_vis_{}'.format(tag), 'P_vis_{}'.format(tag), 'Mrec_jj_{}'.format(tag), 'M_inv_{}'.format(tag), 'P_inv_{}'.format(tag), 'costhetainv_{}'.format(tag), 'alpha1_{}'.format(tag), 'alpha2_{}'.format(tag), 'M_vis_corr1_{}'.format(tag), 'M_vis_corr2_{}'.format(tag), 'M_inv_corr1_{}'.format(tag), 'M_inv_corr2_{}'.format(tag), 'event_nmu_{}'.format(tag), 'muons_p_{}'.format(tag), 'event_nel_{}'.format(tag), 'electrons_p_{}'.format(tag)
]


max_events = 3.3e6
ncpus = 48
v = "13classes_v1"

## for training
path = "/eos/experiment/fcc/ee/analyses/case-studies/higgs/flat_trees/zh_vvjj_var_training_v3/"

## for evaluation
path = "/eos/experiment/fcc/ee/analyses/case-studies/higgs/flat_trees/zh_vvjj_var_v3/"
#path = "/eos/experiment/fcc/ee/analyses_storage/Higgs_and_TOP/hjj/vvjj/zh_vvjj_var_v3/"


train_processes = []
for proc, index in final_states.items():
    if proc not in ["WW", "ZZ", "Zqq", "qqH"]:
        train_processes.append(Process(proc, "{}/wzp6_ee_nunuH_{}_ecm240/*.root".format(path, proc), index, 1.0, max_events))
    elif proc in "qqH":
        train_processes.append(Process(proc, "{}/wzp6_ee_qqH_ecm240/*.root".format(path), index, 1.0, max_events))
    else:
        train_processes.append(Process(proc, "{}/p8_ee_{}_ecm240/*.root".format(path, proc), index, 1.0, max_events))


evaluate_processes_names = list(final_states.keys())
#evaluate_processes_names += ["qqH","WW","ZZ","Zqq"]

evaluate_processes_names += ["qqH","WW","ZZ","Zqq"]
#evaluate_processes_names = ["ZZ"]
evaluate_processes_names = ["Zqq"]

print(evaluate_processes_names)
evaluate_processes = []

for proc in evaluate_processes_names:
    #if proc not in ["Hbb"]:
    #    continue
    if proc not in ["WW", "ZZ", "Zqq", "qqH"]:
        evaluate_processes.append(Process(proc, "{}/wzp6_ee_nunuH_{}_ecm240/*.root".format(path, proc), index, 1.0, max_events))
    elif proc in "qqH":
        evaluate_processes.append(Process(proc, "{}/wzp6_ee_qqH_ecm240/*.root".format(path), index, 1.0, max_events))
    else:
        evaluate_processes.append(Process(proc, "{}/p8_ee_{}_ecm240/*.root".format(path, proc), index, 1.0, max_events))
