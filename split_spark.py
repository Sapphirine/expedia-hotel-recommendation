"""This script splits a csv file into training and test datasets using spark. Note the output csv will be partitioned by spark.

Usage: gcloud dataproc jobs submit pyspark split_spark.py --cluster=${CLUSTER} -- gs://input_csv_location.csv [training_weight] [testing_weight] [seed]
"""
import sys
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
  raise Exception("At least 3 arguments: <location> [training_weight] [testing_weight] [seed]")

# Set filenames, weights and seed
inputUri = sys.argv[1]
path = inputUri.split('.csv')[0]
outputTrainUri = path + "_train.csv"
outputTestUri = path + "_test.csv"

weights = [0.7, 0.3]
seed = 100

if len(sys.argv) >= 4:
  weights[0] = float(sys.argv[2])
  weights[1] = float(sys.argv[3])
if len(sys.argv) >= 5:
  seed = int(sys.argv[4])

spark = SparkSession.builder.appName("Split").getOrCreate()

# Read csv into dataframe
df = spark.read.csv(inputUri, inferSchema=True)

# Split dataframe into training and test
training, test = df.randomSplit(weights, 100)

# Write the two csvs to file
training.write.csv(outputTrainUri, mode='overwrite')
test.write.csv(outputTestUri, mode='overwrite')