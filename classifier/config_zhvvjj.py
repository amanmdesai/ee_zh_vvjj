import os
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import pandas as pd
import uproot as up
import glob


# ______________________________________________________________________________________________________
def concatenate_files(file_list, tree_name, variables, max_events):
    events_count = 0
    concatenated_data = []

    for file in file_list:
        if events_count >= max_events:
            break

        # Iterate over events in the file
        for data in up.iterate("{}:{}".format(file, tree_name), branches=variables, library="pd", step_size="100 MB"):

            remaining_events = max_events - events_count
            if len(data) > remaining_events:
                data = data[:remaining_events]

            events_count += len(data)
            concatenated_data.append(data)

            if events_count >= max_events:
                break

    # Concatenate the data into a single DataFrame
    concatenated_df = pd.concat(concatenated_data, ignore_index=True)
    return concatenated_df


# ___________________________________________________________________________________________________
class Process:
    def __init__(self, name, files, category, weight, max_events):
        self.name = name
        self.files = glob.glob(files)
        self.category = category
        self.weight = weight
        self.max_events = int(max_events)

    def df(self, variables=[]):

        df = concatenate_files(self.files, "events", variables, self.max_events)
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
    "HWW": 5,
    "HZZ": 6,
    "qqH": 7,
    "WW": 8,
    "ZZ": 9,
    "Zqq": 10,
}

vars = [
    "muons_p",
    "electrons_p",
    "jet1_scoreQ",
    "jet1_scoreB",
    "jet1_scoreC",
    "jet1_scoreS",
    "jet1_scoreG",
    "jet2_scoreQ",
    "jet2_scoreB",
    "jet2_scoreC",
    "jet2_scoreS",
    "jet2_scoreG",
    "d_23",
    "d_34",
    "d_45",
    "d_56",
    "d_67",
    "d_78",
    "d_89",
    "jet1_p",
    "jet2_p",
    "jet1_theta",
    "jet2_theta",
    "M_jj",
    "Mrec_jj",
    "M_vis",
    "P_vis",
    "P_inv",
    "M_inv",
    "costhetainv",
    "M_vis_corr1",
    "M_vis_corr2",
    "event_nel",
    "event_nmu",
]

max_events = 1e6
ncpus = 48
v = "all_v1"
path = "/eos/experiment/fcc/ee/analyses/case-studies/higgs/flat_trees/zh_vvjj_v2/"


processes = []
for proc, index in final_states.items():
    if proc not in ["WW", "ZZ", "Zqq", "qqH"]:
        processes.append(Process(proc, "{}/wzp6_ee_nunuH_{}_ecm240/*.root".format(path, proc), index, 1.0, max_events))
    elif proc in "qqH":
        processes.append(Process(proc, "{}/wzp6_ee_qqH_ecm240/*.root".format(path), index, 1.0, max_events))
    else:
        processes.append(Process(proc, "{}/p8_ee_{}_ecm240/*.root".format(path, proc), index, 1.0, max_events))
