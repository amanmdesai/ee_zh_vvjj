import sys
import ROOT


def check_compression(file_name):
    # Open the ROOT file
    root_file = ROOT.TFile.Open(file_name)

    # Get the list of keys in the file
    key_list = root_file.GetListOfKeys()

    # Loop over the keys and print the compression settings for each object
    for key in key_list:
        obj = key.ReadObj()
        if obj:
            if isinstance(obj, (ROOT.TH1, ROOT.TH2, ROOT.TGraph, ROOT.TTree)):
                settings = obj.GetCompressionSettings()
                if settings:
                    name = obj.GetName()
                    class_name = obj.ClassName()
                    level = settings[0].GetUniqueID()
                    algorithm = settings[1].GetUniqueID()
                    print(
                        f"{name} ({class_name}): level={level}, algorithm={algorithm}"
                    )
                del settings
            del obj

    # Close the ROOT file
    root_file.Close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_compression.py <file_name>")
        sys.exit(1)

    file_name = sys.argv[1]
    check_compression(file_name)
