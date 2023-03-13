import ROOT
import os
import sys

procdir = sys.argv[1]

# Enable multi-threading
ROOT.ROOT.EnableImplicitMT()

sels = [
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && B > C && B > S && B > G && B > Q && B > -999 && B < 1.1",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && B > C && B > S && B > G && B > Q && B > 1.1 && B < 1.9",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && B > C && B > S && B > G && B > Q && B > 1.9 && B < 999",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && C > B && C > S && C > G && C > Q && C > -999 && C < 1.0",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && C > B && C > S && C > G && C > Q && C > 1.0 && C < 1.8",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && C > B && C > S && C > G && C > Q && C > 1.8 && C < 999",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && S > B && S > C && S > G && S > Q && S > -999 && S < 1.1",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && S > B && S > C && S > G && S > Q && S > 1.1 && S < 1.7",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && S > B && S > C && S > G && S > Q && S > 1.7 && S < 999",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && G > B && G > C && G > S && G > Q && G > -999 && G < 1.2",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && G > B && G > C && G > S && G > Q && G > 1.2 && G < 1.5",
    "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85 && G > B && G > C && G > S && G > Q && G > 1.5 && G < 999",
]

df = ROOT.RDataFrame(
    "events",
    "/eos/experiment/fcc/ee/analyses/case-studies/higgs/flat_trees/zh_vvjj_v2/{}/*.root".format(
        procdir
    ),
)

df_list = []
for i, sel in enumerate(sels):

    sel_name = "sel_{}".format(i)
    df_sel = df.Filter(sel, sel_name)
    df_sel = df_sel.Define("weight", "1.5")
    df_sel_h1 = df_sel.Histo1D(
        (
            "h_{}".format(sel_name),
            "",
            400,
            0,
            200,
        ),
        "M_jj",
        "weight",
    )
    df_list.append(df_sel_h1)

print("running {} dfs".format(len(df_list)))
ROOT.RDF.RunGraphs(df_list)
