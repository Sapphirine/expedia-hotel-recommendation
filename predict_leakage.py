"""This script predicts the hotel cluster for a test csv, based on the training csv. The leakage solution is used.

This script should work for both a real test csv (as from Kaggle) or a test csv partitioned from a training file. 

Reference:
https://www.kaggle.com/zfturbo/leakage-solution
https://github.com/Sapphirine/hotel_recommender/blob/master/recommender.py

Usage: gcloud dataproc jobs submit pyspark predict_leakage.py --cluster=${CLUSTER} -- gs://training_csv_location.csv gs://test_csv_location.csv [count]
"""
from pyspark.sql import SparkSession
import gcsfs
from operator import add
import sys

if len(sys.argv) < 3:
    raise Exception("At least 2: <training_csv_location> <test_csv_location> [predict_count]")

# Set filenames and count
trainingUri = sys.argv[1]
testUri = sys.argv[2]
path = testUri.split('.csv')[0]
outputUri = path + "_predict.csv"
count = 50
if len(sys.argv) >= 4:
    count = int(sys.argv[3])

fs = gcsfs.GCSFileSystem(project='eecs6893-253116')


spark = SparkSession.builder.appName("Predict Leakage").getOrCreate()

df = spark.read.csv(trainingUri, header=True)
rdd = df.rdd.cache()


def get_best_hotels_od_ulc(row):
    if row['user_location_city'] != '' and row['orig_destination_distance'] != '':
        return ((row['user_location_city'], row['orig_destination_distance'],row['hotel_cluster']),1)
    else:
        return ((row['user_location_city'], row['orig_destination_distance'],row['hotel_cluster']),0)

def get_best_hotels_search_dest(row):
    if row['srch_destination_id'] != '' and row['hotel_country'] != '' and row['hotel_market'] != '' and int(row['date_time'][:4]) == 2014:
        return ((row['srch_destination_id'], row['hotel_country'], row['hotel_market'], row['hotel_cluster']), int(row['is_booking']) * 17 + 3)
    else:
        return ((row['srch_destination_id'], row['hotel_country'], row['hotel_market'], row['hotel_cluster']), 0)

def get_best_hotels_search_dest1(row):
    if row['srch_destination_id'] != '':
        return ((row['srch_destination_id'], row['hotel_cluster']) ,int(row['is_booking']) * 17 + 3)
    else:
        return ((row['srch_destination_id'], row['hotel_cluster']), 0)

def get_best_hotel_country(row):
    if row['hotel_country'] != '':
        return ((row['hotel_country'], row['hotel_cluster']), 1 + 5 * int(row['is_booking']))
    else:
        return ((row['hotel_country'], row['hotel_cluster']), 0)

def get_popular_hotel_cluster(row):
    return (row['hotel_cluster'], 1)


best_hotels_od_ulc = rdd.map(lambda row: get_best_hotels_od_ulc(row)).foldByKey(0, add).cache()

best_hotels_search_dest = rdd.map(lambda row: get_best_hotels_search_dest(row)).foldByKey(0, add).cache()

best_hotels_search_dest1 = rdd.map(lambda row: get_best_hotels_search_dest1(row)).foldByKey(0, add).cache()

best_hotel_country = rdd.map(lambda row: get_best_hotel_country(row)).foldByKey(0, add).cache()

popular_hotel_cluster = rdd.map(lambda row: get_popular_hotel_cluster(row)).foldByKey(0, add).cache()


output_file = fs.open(outputUri, "w")
test_file = fs.open(testUri, "r")
schema = test_file.readline().strip().split(",")
total = 0
output_file.write("id,hotel_cluster\n")
topclusters = popular_hotel_cluster.sortBy(lambda x: x[1], ascending=False).map(lambda x:x[0]).take(5)

while 1:
    line = test_file.readline().strip()
    arr = line.split(",")
    row = dict(zip(schema, arr))
    id = total
    if 'id' in row:  # This is a real test file.
        id = row['id']
    user_location_city = row['user_location_city']
    orig_destination_distance = row['orig_destination_distance']
    srch_destination_id = row['srch_destination_id']
    hotel_country = row['hotel_country']
    hotel_market = row['hotel_market']

    output_file.write(str(id) + ',')
    filled = []


    topitems = best_hotels_od_ulc.filter(lambda x: x[0][0] == user_location_city and x[0][1] == orig_destination_distance)
    topitems = topitems.sortBy(lambda x: x[1], ascending=False).map(lambda x:x[0][2]).take(5)
    for i in range(len(topitems)):
        if len(filled) == 5:
            break
        if topitems[i] in filled:
            continue
        output_file.write(' ' + topitems[i])
        filled.append(topitems[i])

    if len(filled) < 5:
        topitems = best_hotels_search_dest.filter(lambda x: x[0][0] == srch_destination_id and x[0][1] == hotel_country and x[0][2] == hotel_market)
        topitems = topitems.sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0][3]).take(5)
        for i in range(len(topitems)):
            if len(filled) == 5:
                break
            if topitems[i] in filled:
                continue
            output_file.write(' ' + topitems[i])
            filled.append(topitems[i])

    if len(filled) < 5:
        if len(topitems) != 0:
            topitems = best_hotels_search_dest1.filter(lambda x: x[0][0] == srch_destination_id)
            topitems = topitems.sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0][1]).take(5)
            for i in range(len(topitems)):
                if len(filled) == 5:
                    break
                if topitems[i] in filled:
                    continue
                output_file.write(' ' + topitems[i])
                filled.append(topitems[i])

    if len(filled) < 5:
        topitems = best_hotel_country.filter(lambda x: x[0][0] == hotel_country)
        topitems = topitems.sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0][1]).take(5)
        for i in range(len(topitems)):
            if len(filled) == 5:
                break
            if topitems[i] in filled:
                continue
            output_file.write(' ' + topitems[i])
            filled.append(topitems[i])

    if len(filled) < 5:
        for i in range(5):
            if len(filled) == 5:
                break
            if topclasters[i] in filled:
                continue
            output_file.write(' ' + topclusters[i])
            filled.append(topclusters[i])
    output_file.write("\n")
    
    total += 1
    if total % 10 == 0:
        print('Write {} lines...'.format(total))

    if total >= count:
        break

output_file.close()
print('Completed!')
