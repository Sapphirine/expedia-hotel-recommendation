"""This script compares the prediction result csv file with a test csv file with actual results, and outputs an accuracy.

Reference:
https://github.com/Sapphirine/hotel_recommender/blob/master/accuracy.py

Usage: python accuracy.py <result_csv_location> <test_csv_location>
"""
import sys

if len(sys.argv) < 3:
    raise Exception("At least 2: <result_csv_location> <test_csv_location>")

# Set filenames and count
resultUri = sys.argv[1]
testUri = sys.argv[2]

with open(resultUri, 'r') as result, open(testUri, 'r') as testdata:
    k = 0.0
    b = 0.0
    result.readline()
    testdata.readline()
    while 1:
        b = b + 1
        r1 = result.readline()
        if  r1 == '':
            break
        else:
            r = r1.strip().split(" ")
            t = testdata.readline().strip().split(",")
            print(t[23], r[1:], t[23] in r[1:])
            if t[23] in r[1:]:
                k += 1
    print("accuracy:", k / b)
