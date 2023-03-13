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

path = "/eos/experiment/fcc/ee/analyses/case-studies/higgs/flat_trees/zh_vvjj_v2"
processes = []

hbb = Process(
    "Hbb",
    "{}/wzp6_ee_nunuH_Hbb_ecm240_score2/*.root".format(path),
    "H #rightarrow b b",
    0.0269,
    1.0,
    5e6,
)

hcc = Process(
    "Hcc",
    "{}/wzp6_ee_nunuH_Hcc_ecm240_score2/*.root".format(path),
    "H #rightarrow c c",
    0.001335,
    1.0,
    5e6,
)

hss = Process(
    "Hss",
    "{}/wzp6_ee_nunuH_Hss_ecm240_score2/*.root".format(path),
    "H #rightarrow s s",
    1.109e-05,
    1.0,
    5e6,
)
hgg = Process(
    "Hgg",
    "{}/wzp6_ee_nunuH_Hgg_ecm240_score2/*.root".format(path),
    "H #rightarrow g g",
    0.003782,
    1.0,
    5e6,
)
htautau = Process(
    "Htautau",
    "{}/wzp6_ee_nunuH_Htautau_ecm240_score2/*.root".format(path),
    "H #rightarrow #tau #tau",
    0.002897,
    1.0,
    5e6,
)
hww = Process(
    "HWW",
    "{}/wzp6_ee_nunuH_HWW_ecm240_score2/*.root".format(path),
    "H #rightarrow W W",
    0.00994,
    1.0,
    5e6,
)
hzz = Process(
    "HZZ",
    "{}/wzp6_ee_nunuH_HZZ_ecm240_score2/*.root".format(path),
    "H #rightarrow Z Z",
    0.00122,
    1.0,
    5e6,
)

ww = Process(
    "WW",
    "{}/p8_ee_WW_ecm240_score2/*.root".format(path),
    "W W",
    16.4385,
    1.0,
    5e6,
)
zz = Process(
    "ZZ",
    "{}/p8_ee_ZZ_ecm240_score2/*.root".format(path),
    "Z Z",
    1.35899,
    1.0,
    5e6,
)
zqq = Process(
    "Zqq",
    "{}/p8_ee_Zqq_ecm240_score2/*.root".format(path),
    "Z",
    52.6539,
    1.0,
    5e6,
)
qqh = Process(
    "qqH",
    "{}/wzp6_ee_qqH_ecm240_score2/*.root".format(path),
    "Z(had) H ",
    0.13635,
    1.0,
    5e6,
)


processes.append(hbb)
processes.append(hcc)
processes.append(hss)
processes.append(hgg)
processes.append(htautau)
processes.append(hww)
processes.append(hzz)
processes.append(ww)
processes.append(zz)
processes.append(zqq)
processes.append(qqh)

### now produce selections #######

scores = ["B", "C", "S", "G", "Q"]
purities = ["L", "M", "H"]


def category_selection(fs, cuts):

    scores_not_fs = [s for s in scores if s != fs]
    ortho_sel = ""
    for s in scores_not_fs:
        ortho_sel += "{} > {} && ".format(fs, s)

    purity_sel = "{} > {} && {} < {}".format(fs, cuts[0], fs, cuts[1])

    return ortho_sel + purity_sel


sel_base = {
    "name": "base",
    "label": "",
    "formula": "muons_p < 20 && electrons_p < 20 && costhetainv < 0.85 && costhetainv > -0.85",
    "latex": "",
}

selection_tree = Selection(sel_base)

purity = dict()
## min and max cut to define the LP, MP, and HP categories
purity[("B", "L")] = (-999, 1.1)
purity[("B", "M")] = (1.1, 1.9)
purity[("B", "H")] = (1.9, 999)
purity[("C", "L")] = (-999, 1.0)
purity[("C", "M")] = (1.0, 1.8)
purity[("C", "H")] = (1.8, 999)
purity[("S", "L")] = (-999, 1.1)
purity[("S", "M")] = (1.1, 1.7)
purity[("S", "H")] = (1.7, 999)
purity[("G", "L")] = (-999, 1.2)
purity[("G", "M")] = (1.2, 1.5)
purity[("G", "H")] = (1.5, 999)


sel_dummy = {
    "name": "",
    "label": "",
    "formula": "",
    "latex": "",
}

## generate final selection cuts
fs_categories = [s for s in scores if s != "Q"]
for fs in fs_categories:
    for p in purities:
        sel = deepcopy(sel_dummy)
        sel["name"] = "{}like_{}".format(fs, p)
        sel["formula"] = category_selection(fs, purity[(fs, p)])
        selection_tree.add_child(Selection(sel))


h1s = [
    {
        "name": "mjj",
        "var": "M_jj",
        "nbins": 400,
        "xmin": 0,
        "xmax": 200,
        "xtitle": "m_{jj} [GeV]",
        "ytitle": "N_{events}",
        "log": True,
    }
]


h2s = [
    {
        "name": "h2",
        "var_x": "M_jj",
        "var_y": "Mrec_jj",
        "nbins_x": 400,
        "xmin": 0,
        "xmax": 200,
        "nbins_y": 400,
        "ymin": 0,
        "ymax": 200,
        "xtitle": "m_{jj} [GeV]",
        "ytitle": "m_{rec} [GeV]",
        "log": True,
    }
]
