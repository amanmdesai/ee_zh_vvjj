import ROOT
from copy import deepcopy

# ___________________________________________________________________________________________________
class Process:
    def __init__(self, name, files, label, xsec, br, lumi):
        self.name = name
        self.files = files
        self.xsec = xsec
        self.br = br
        self.lumi = lumi
        self.nevents = -1
        self.weight = -1

    def rdf(self):
        df = ROOT.RDataFrame("events", self.files)
        self.nevents = df.Count().GetValue()
        self.weight = (self.xsec * self.br * self.lumi) / self.nevents
        print("{}: ".format(self.name))
        print(" --> {}".format(int(self.nevents)))

        return df


# ___________________________________________________________________________________________________
class Selection:
    def __init__(self, value, children=None):
        self.value = value
        self.children = children or []

    def add_child(self, node):
        self.children.append(node)

    def __str__(self):
        return str(self.value)

    def traverse_tree(self):
        print(self.value)
        for child in self.children:
            self.traverse_tree(child)

    def get_all_nodes(self):
        nodes = [self]
        for child in self.children:
            nodes += child.get_all_nodes()
        return nodes


# __________________________________________________________________________________________________
## all MC processes are defined here

path = (
    "/eos/experiment/fcc/ee/analyses/case-studies/higgs/flat_trees/zh_vvjj_var"
)
#path = "/eos/experiment/fcc/ee/analyses/case-studies/higgs/flat_trees/zh_vvjj_var_v2"
path = "/eos/experiment/fcc/ee/analyses/case-studies/higgs/flat_trees/zh_vvjj_var_v3"
processes = []


hbb = Process(
    "Hbb",
    "{}/wzp6_ee_nunuH_Hbb_ecm240/*.root".format(path),
    "H #rightarrow b b",
    0.0269,
    1.0,
    5e6,
)

hcc = Process(
    "Hcc",
    "{}/wzp6_ee_nunuH_Hcc_ecm240/*.root".format(path),
    "H #rightarrow c c",
    0.001335,
    1.0,
    5e6,
)

hss = Process(
    "Hss",
    "{}/wzp6_ee_nunuH_Hss_ecm240/*.root".format(path),
    "H #rightarrow s s",
    1.109e-05,
    1.0,
    5e6,
)

hdd = Process(
    "Hdd",
    "{}/wzp6_ee_nunuH_Hdd_ecm240/*.root".format(path),
    "H #rightarrow d d",
    9.702e-09,
    1.0,
    5e6,
)

huu = Process(
    "Huu",
    "{}/wzp6_ee_nunuH_Huu_ecm240/*.root".format(path),
    "H #rightarrow u u",
    4.158e-09,
    1.0,
    5e6,
)

hbs = Process(
    "Hbs",
    "{}/wzp6_ee_nunuH_Hbs_ecm240/*.root".format(path),
    "H #rightarrow b s",
    1e-10,
    1.0,
    5e6,
)


hbd = Process(
    "Hbd",
    "{}/wzp6_ee_nunuH_Hbd_ecm240/*.root".format(path),
    "H #rightarrow b d",
    1e-12,
    1.0,
    5e6,
)

hcu = Process(
    "Hcu",
    "{}/wzp6_ee_nunuH_Hcu_ecm240/*.root".format(path),
    "H #rightarrow c u",
    1e-14,
    1.0,
    5e6,
)

hsd = Process(
    "Hsd",
    "{}/wzp6_ee_nunuH_Hsd_ecm240/*.root".format(path),
    "H #rightarrow s d",
    1e-19,
    1.0,
    5e6,
)

hgg = Process(
    "Hgg",
    "{}/wzp6_ee_nunuH_Hgg_ecm240/*.root".format(path),
    "H #rightarrow g g",
    0.003782,
    1.0,
    5e6,
)

htautau = Process(
    "Htautau",
    "{}/wzp6_ee_nunuH_Htautau_ecm240/*.root".format(path),
    "H #rightarrow #tau #tau",
    0.002897,
    1.0,
    5e6,
)
hww = Process(
    "HWW",
    "{}/wzp6_ee_nunuH_HWW_ecm240/*.root".format(path),
    "H #rightarrow W W",
    0.00994,
    1.0,
    5e6,
)
hzz = Process(
    "HZZ",
    "{}/wzp6_ee_nunuH_HZZ_ecm240/*.root".format(path),
    "H #rightarrow Z Z",
    0.00122,
    1.0,
    5e6,
)

ww = Process(
    "WW",
    "{}/p8_ee_WW_ecm240/*.root".format(path),
    "W W",
    16.4385,
    1.0,
    5e6,
)
zz = Process(
    "ZZ",
    "{}/p8_ee_ZZ_ecm240/*.root".format(path),
    "Z Z",
    1.35899,
    1.0,
    5e6,
)
zqq = Process(
    "Zqq",
    "{}/p8_ee_Zqq_ecm240/*.root".format(path),
    "Z",
    52.6539,
    1.0,
    5e6,
)
qqh = Process(
    "qqH",
    "{}/wzp6_ee_qqH_ecm240/*.root".format(path),
    "Z(had) H ",
    0.13635,
    1.0,
    5e6,
)


processes.append(hbb)
processes.append(hcc)
processes.append(hss)
processes.append(huu)
processes.append(hdd)
processes.append(hgg)
processes.append(htautau)
processes.append(hww)
processes.append(hzz)
processes.append(ww)
processes.append(zz)
processes.append(zqq)
processes.append(qqh)

### now produce selections #######


def category_selection(fs, cuts):

    scores_not_fs = [s for s in scores if s != fs]
    ortho_sel = ""
    for s in scores_not_fs:
        ortho_sel += "{} > {} && ".format(fs, s)

    purity_sel = "{} > {} && {} < {}".format(fs, cuts[0], fs, cuts[1])

    return ortho_sel + purity_sel

vars = ["ip", "res", "dndx", "tof"]
scale_factors = [1.0, 2.0, 3.0, 5.0, 10.0]
#scale_factors = [1.0]

labels = []
selection_tree_dict = dict()
h1s_dict = dict()
h2s_dict = dict()

for var in vars:
    for scale in scale_factors:
        label = "{}{}".format(var, scale).replace(".", "p")
        labels.append(label)

        scores = [
            "B_{}".format(label),
            "C_{}".format(label),
            "S_{}".format(label),
            "G_{}".format(label),
            "U_{}".format(label),
            "D_{}".format(label),
            "TAU_{}".format(label),
          
        ]
        purities = [
            "L_{}".format(label),
            "M_{}".format(label),
            "H_{}".format(label),
        ]

        sel_base = {
            "name": "base_{}".format(label),
            "label": "",
            "formula": "muons_p_{} < 20 && electrons_p_{} < 20 && costhetainv_{} < 0.85 && costhetainv_{} > -0.85".format(
                label, label, label, label
            ),
            "latex": "",
        }

        selection_tree_dict[label] = Selection(sel_base)

        purity = dict()
        ## min and max cut to define the LP, MP, and HP categories
        purity[("B_{}".format(label), "L_{}".format(label))] = (-999, 1.1)
        purity[("B_{}".format(label), "M_{}".format(label))] = (1.1, 1.9)
        purity[("B_{}".format(label), "H_{}".format(label))] = (1.9, 999)
        purity[("C_{}".format(label), "L_{}".format(label))] = (-999, 1.0)
        purity[("C_{}".format(label), "M_{}".format(label))] = (1.0, 1.8)
        purity[("C_{}".format(label), "H_{}".format(label))] = (1.8, 999)
        purity[("S_{}".format(label), "L_{}".format(label))] = (-999, 1.1)
        purity[("S_{}".format(label), "M_{}".format(label))] = (1.1, 1.7)
        purity[("S_{}".format(label), "H_{}".format(label))] = (1.7, 999)
        purity[("G_{}".format(label), "L_{}".format(label))] = (-999, 1.2)
        purity[("G_{}".format(label), "M_{}".format(label))] = (1.2, 1.5)
        purity[("G_{}".format(label), "H_{}".format(label))] = (1.5, 999)
        purity[("U_{}".format(label), "L_{}".format(label))] = (-999, 1.1)
        purity[("U_{}".format(label), "M_{}".format(label))] = (1.1, 1.7)
        purity[("U_{}".format(label), "H_{}".format(label))] = (1.7, 999)
        purity[("D_{}".format(label), "L_{}".format(label))] = (-999, 1.1)
        purity[("D_{}".format(label), "M_{}".format(label))] = (1.1, 1.7)
        purity[("D_{}".format(label), "H_{}".format(label))] = (1.7, 999)
        purity[("TAU_{}".format(label), "L_{}".format(label))] = (-999, 1.1)
        purity[("TAU_{}".format(label), "M_{}".format(label))] = (1.1, 1.7)
        purity[("TAU_{}".format(label), "H_{}".format(label))] = (1.7, 999)

        sel_dummy = {
            "name": "",
            "label": "",
            "formula": "",
            "latex": "",
        }

        ## generate final selection cuts
        # fs_categories = [s for s in scores if s != "Q_{}".format(label)]
        fs_categories = scores
        for fs in fs_categories:
            for p in purities:
                sel = deepcopy(sel_dummy)
                sel["name"] = "{}like_{}".format(
                    fs.replace("_{}".format(label), ""), p
                )

                sel["formula"] = category_selection(fs, purity[(fs, p)])
                print(sel["name"], sel["formula"])
                selection_tree_dict[label].add_child(Selection(sel))

        h1s_dict[label] = [
            {
                "name": "mjj",
                "var": "M_jj_{}".format(label),
                "nbins": 400,
                "xmin": 0,
                "xmax": 200,
                "xtitle": "m_{jj} [GeV]",
                "ytitle": "N_{events}",
                "log": True,
            }
        ]

        h2s_dict[label] = [
            {
                "name": "h2",
                "var_x": "M_jj_{}".format(label),
                "var_y": "Mrec_jj_{}".format(label),
                "nbins_x": 200,
                "xmin": 0,
                "xmax": 200,
                "nbins_y": 40,
                "ymin": 0,
                "ymax": 200,
                "xtitle": "m_{jj} [GeV]",
                "ytitle": "m_{rec} [GeV]",
                "log": True,
            }
        ]
