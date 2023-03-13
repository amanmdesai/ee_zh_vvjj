import os
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import pandas as pd
import uproot as up


# ___________________________________________________________________________________________________
class Process:
    def __init__(self, name, files, category, weight):
        self.name = name
        self.files = files
        self.category = category
        self.weight = weight

    def df(self, variables=[]):
        dfs = []

        ## only extract list of variables
        if len(variables) > 0:
            dfs = up.iterate("{}:events".format(self.files), branches=variables, library="pd")

        ## extract all
        else:
            dfs = up.iterate("{}:events".format(self.files), library="pd")

        # Merge the dataframes into one
        df = pd.concat(dfs, ignore_index=True)
        df["target"] = self.category

        #return df.iloc[:10000]
        return df


# _____________________________________________________________________________________________________
# data
path = "/eos/experiment/fcc/ee/analyses/case-studies/higgs/flat_trees/zh_vvjj_v2/"

final_states = {"Hbb": 0, "Hcc": 1, "Hss": 2, "Hgg": 3, "Htautau": 4, "HWW": 5, "HZZ": 5}
class_labels = ["Hbb", "Hcc", "Hss", "Hgg", "Htautau", "HWW_ZZ"]

vars = [
    "jet1_scoreQ",
    "jet2_scoreQ",
    "jet1_scoreB",
    "jet2_scoreB",
    "jet1_scoreC",
    "jet2_scoreC",
    "jet1_scoreS",
    "jet2_scoreS",
    "jet1_scoreG",
    "jet2_scoreG",
    "d_23",
    "d_34",
    "d_45",
    "d_56",
    "d_67",
    "d_78",
    "d_89",
]


proc_Hbb = Process("Hbb", "{}/wzp6_ee_nunuH_Hbb_ecm240/*.root".format(path), 0, 1.0)
proc_Hcc = Process("Hcc", "{}/wzp6_ee_nunuH_Hcc_ecm240/*.root".format(path), 1, 1.0)
proc_Hss = Process("Hss", "{}/wzp6_ee_nunuH_Hss_ecm240/*.root".format(path), 2, 1.0)
proc_Hgg = Process("Hgg", "{}/wzp6_ee_nunuH_Hgg_ecm240/*.root".format(path), 3, 1.0)
proc_Htautau = Process("Htautau", "{}/wzp6_ee_nunuH_Htautau_ecm240/*.root".format(path), 4, 1.0)
proc_HWW = Process("HWW", "{}/wzp6_ee_nunuH_HWW_ecm240/*.root".format(path), 5, 0.5)
proc_HZZ = Process("HZZ", "{}/wzp6_ee_nunuH_HZZ_ecm240/*.root".format(path), 5, 0.5)
