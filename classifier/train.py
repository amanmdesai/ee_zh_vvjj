import glob
import sys

sys.path.insert(0, "/afs/cern.ch/user/s/selvaggi/.local/lib/python3.9/site-packages")

# import necessary libraries
import numpy as np
import os
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import xgboost as xgb
import seaborn as sns
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import matplotlib.pyplot as plt
import pickle

import pandas as pd

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

print(xgb.__version__)

nclass = max(list(final_states.values())) + 1

## produce dataframes
list_df = []
for proc in processes:
    list_df.append(proc.df(vars))

# concatenate data from all three trees
df = pd.concat(list_df)

# extract feature and target arrays from dataframe
X = df[vars].values
Y = df["target"].values

# split data into training and testing sets
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.1, random_state=42)

os.environ["OMP_NUM_THREADS"] = "{}".format(ncpus)
# define xgboost classifier
xgb_model = xgb.XGBClassifier(objective="multi:softproba", num_classes=nclass, nthread=ncpus)
# xgb_model = xgb.XGBClassifier(objective="multi:softprob", num_classes=nclass, nthread=ncpus)

# train the classifier on the training set
eval_set = [(X_train, Y_train), (X_test, Y_test)]
xgb_model.fit(
    X_train, Y_train, early_stopping_rounds=5, eval_metric=["merror", "mlogloss"], eval_set=eval_set, verbose=True
)

# save the model to a file using pickle
with open("model_{}.pkl".format(v), "wb") as f:
    pickle.dump(xgb_model, f)

# evaluate the classifier on the testing set
Y_pred = xgb_model.predict(X_test)

accuracy = xgb_model.score(X_test, Y_test)

predictions = [round(value) for value in Y_pred]
# evaluate predictions
accuracy = accuracy_score(Y_test, predictions)
print("Accuracy: %.2f%%" % (accuracy * 100.0))
# retrieve performance metrics

results = xgb_model.evals_result()
epochs = len(results["validation_0"]["merror"])
x_axis = range(0, epochs)
# plot log loss
fig, ax = plt.subplots()
ax.plot(x_axis, results["validation_0"]["mlogloss"], label="Train")
ax.plot(x_axis, results["validation_1"]["mlogloss"], label="Test")
ax.legend()
plt.ylabel("Log Loss")
plt.title("XGBoost Log Loss")
fig.tight_layout()
# save plot to file
fig.savefig("/eos/user/s/selvaggi/www/analysis/zh_vvjj/logloss_{}.png".format(v), dpi=300)

# plot classification error
fig, ax = plt.subplots()
ax.plot(x_axis, results["validation_0"]["merror"], label="Train")
ax.plot(x_axis, results["validation_1"]["merror"], label="Test")
ax.legend()
plt.ylabel("Classification Error")
plt.title("XGBoost Classification Error")
fig.tight_layout()
# save plot to file
fig.savefig("/eos/user/s/selvaggi/www/analysis/zh_vvjj/error_{}.png".format(v), dpi=300)


# compute confusion matrix
cm_unnorm = confusion_matrix(Y_test, Y_pred)
# cm = (cm / np.sum(cm)) * 100

# normalize confusion matrix
cm = cm_unnorm.astype("float") / cm_unnorm.sum(axis=1)[:, np.newaxis]

# plot confusion matrix
fig, ax = plt.subplots(figsize=(10, 10))
im = ax.imshow(cm, interpolation="nearest", cmap=plt.cm.Blues)
ax.figure.colorbar(im, ax=ax)
ax.set(
    xticks=np.arange(cm.shape[1]),
    yticks=np.arange(cm.shape[0]),
    xticklabels=final_states.keys(),
    yticklabels=final_states.keys(),
    ylabel="True label",
    xlabel="Predicted label",
)

plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

# loop over data and create text annotations
for i in range(cm.shape[0]):
    for j in range(cm.shape[1]):
        ax.text(
            j,
            i,
            format(cm[i, j], ".2f"),
            ha="center",
            va="center",
            color="white" if cm[i, j] > cm.max() / 2.0 else "black",
        )

fig.tight_layout()
# save plot to file
fig.savefig("/eos/user/s/selvaggi/www/analysis/zh_vvjj/confusion_matrix_{}.png".format(v), dpi=300)
