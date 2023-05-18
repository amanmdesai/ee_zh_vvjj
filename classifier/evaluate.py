import sys

sys.path.insert(0, "/afs/cern.ch/user/s/selvaggi/.local/lib/python3.9/site-packages")

import pandas as pd
import xgboost as xgb
import numpy as np
import pickle
import uproot as up
import os
import glob
import ROOT


import argparse
import importlib

# Parse command line arguments
parser = argparse.ArgumentParser(description="Load config file.")
parser.add_argument("config_file", help="Configuration file to load.")
args = parser.parse_args()


# Import config file
# Import config file
config_file_path = args.config_file
config_dir, config_file_name = os.path.split(config_file_path)
config_module_name = config_file_name.rstrip(".py")

# Add the config directory to sys.path to ensure the config module can be found
sys.path.append(config_dir)
config = importlib.import_module(config_module_name)

final_states = config.final_states
path = config.path
vars = config.vars
processes = config.processes
v = config.v
ncpus = config.ncpus


# Define the function to evaluate the model on a single root file
def evaluate_process(model, proc_dir, score_tag):

    files = glob.glob("{}/*.root".format(proc_dir))

    # Define the output ROOT file and tree
    score_dir = "{}_{}".format(proc_dir, score_tag)
    os.system("mkdir -p {}".format(score_dir))

    for file_in in files:

        dfs = up.iterate("{}:events".format(file_in), library="pd")

        data = pd.concat(dfs, ignore_index=True)

        # Get the variables for classification
        scores = model.predict_proba(data[vars])

        # Add the predicted class to the data
        maxval = 1e99
        minval = -1e99
        for i, name in enumerate(final_states.keys()):
            data[name] = np.log10(scores[:, i] / (1 - scores[:, i]))
            # data[name] = scores[:, i]

        file_out = "{}/{}".format(score_dir, os.path.basename(file_in))
        ufile_out = up.recreate(file_out)
        ufile_out["events"] = data

        rfile_in = ROOT.TFile(file_in)
        tparam = rfile_in.eventsProcessed

        print(score_dir)
        print("writing file {} ... ".format(file_out))

        rfile_out = ROOT.TFile(file_out, "update")
        tparam.Write()
        rfile_out.Close()


# Load the trained model from file
with open("model_{}.pkl".format(v), "rb") as f:
    model = pickle.load(f)


processes = [
    "wzp6_ee_nunuH_Hbb_ecm240",
    "wzp6_ee_nunuH_Hcc_ecm240",
    "wzp6_ee_nunuH_Hgg_ecm240",
    "wzp6_ee_nunuH_Hss_ecm240",
    "wzp6_ee_nunuH_Htautau_ecm240",
    "wzp6_ee_nunuH_HWW_ecm240",
    "wzp6_ee_nunuH_HZZ_ecm240",
    "wzp6_ee_qqH_ecm240",
    "p8_ee_WW_ecm240",
    "p8_ee_ZZ_ecm240",
    "p8_ee_Zqq_ecm240",
]


score_tag = "score_{}".format(v)
for proc in processes:
    proc_dir = "{}/{}".format(path, proc)
    print("producing process: {} .. ".format(proc))
    evaluate_process(model, proc_dir, score_tag)
