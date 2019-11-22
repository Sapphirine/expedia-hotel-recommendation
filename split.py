"""This script splits a csv file into training and test datasets.

Usage: python split.py <location> [test_size]
"""
import sys
import pandas as pd

from sklearn.model_selection import train_test_split

if len(sys.argv) < 2:
  raise Exception("At least 2: <location> [test_size]")

# Set filenames, weights and seed
inputUri = sys.argv[1]
path = inputUri.split('.csv')[0]
outputTrainUri = path + "_train.csv"
outputTestUri = path + "_test.csv"

test_size = 0.25

if len(sys.argv) >= 3:
  test_size = float(sys.argv[2])

# Load the csv into dataframe
print("Loading input csv file.")
df = pd.read_csv(inputUri)

# Split dataframe into training and test
print("Splitting dataframe into training and test.")
training, test = train_test_split(df, test_size=test_size)

# Write the two csvs to file
print("Writing training dataframe to csv.")
training.to_csv(outputTrainUri, index=False)
print("Writing test dataframe to csv.")
test.to_csv(outputTestUri, index=False)
