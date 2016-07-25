#!/usr/bin/python
from __future__ import division
import MySQLdb
from dateutil import parser
from collections import defaultdict
import re

db = MySQLdb.connect(host="10.47.85.51",    # your host, usually localhost
                     user="root",         # your username
                     passwd="test123",  # your password
                     db="test")        # name of the data base

def querydb(query):
    d_dataset = defaultdict(int)
    sum_dataset = 0
    cur = db.cursor()
    cur.execute(query)
    i = 0
    j = 0
    # print all the first cell of all the rows
    for row in cur.fetchall():
        comments = row[0].split("?#+@")
        for comment in comments:
                try:
                    dt = parser.parse(comment[:19])
                    i = dt.hour
                    j = 0
                    if(dt.year >= 2013 and dt.year<=2014):
                        #print dt.year
                        if(dt.minute>=0 and dt.minute<=14):
                            j = 1
                        elif(dt.minute>=15 and dt.minute<=29):
                            j = 2
                        elif(dt.minute>=30 and dt.minute<=44):
                            j = 3
                        elif(dt.minute>=45 and dt.minute<60):
                            j = 4
                        bucket = i*4+j
                        d_dataset[bucket] +=1
                except ValueError:
                    dt = None

    for k,v in d_dataset.iteritems():
        sum_dataset = sum_dataset + int(v)
    return d_dataset, sum_dataset

def computeqk(d_dataset,f_wp_data):
    dataset_df = defaultdict(float)
    for key in d_dataset:
        dataset_df[key] = float(d_dataset[key])*f_wp_data
    return dataset_df

d_Flipkart = defaultdict(int)
sum_Flipkart = 0

d_Amazon = defaultdict(int)
sum_Amazon = 0

d_Snapdeal = defaultdict(int)
sum_Snapdeal = 0

d_Myntra = defaultdict(int)
sum_Myntra = 0

d_Flipkart, sum_Flipkart = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Flipkart';")

d_Amazon, sum_Amazon = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Amazon India';")

d_Snapdeal, sum_Snapdeal = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Snapdeal';")

d_Myntra, sum_Myntra = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Myntra';")

Flipkart_df = defaultdict(float)
Amazon_df = defaultdict(float)
Snapdeal_df = defaultdict(float)
Myntra_df = defaultdict(float)

f_wp_Flipkart =  0.0
f_wp_Flipkart = sum_Flipkart/(sum_Flipkart+sum_Amazon+sum_Snapdeal+sum_Myntra)

f_wp_Amazon =  0.0
f_wp_Amazon = sum_Amazon/(sum_Flipkart+sum_Amazon+sum_Snapdeal+sum_Myntra)

f_wp_Snapdeal =  0.0
f_wp_Snapdeal = sum_Snapdeal/(sum_Flipkart+sum_Amazon+sum_Snapdeal+sum_Myntra)

f_wp_Myntra =  0.0
f_wp_Myntra = sum_Myntra/(sum_Flipkart+sum_Amazon+sum_Snapdeal+sum_Myntra)

Flipkart_df = computeqk(d_Flipkart,f_wp_Flipkart)
Amazon_df = computeqk(d_Amazon,f_wp_Amazon)
Snapdeal_df = computeqk(d_Snapdeal,f_wp_Snapdeal)
Myntra_df = computeqk(d_Myntra,f_wp_Myntra)

results_df = defaultdict(float)	

for key in range(1,97):
	results_df[key] = float(Flipkart_df[key]) + float(Amazon_df[key]) + float(Snapdeal_df[key]) + float(Myntra_df[key])

print results_df

text_file = open("D:\Output_Exercise4_Ecommerce_Output.dat", "w")
for k,v in results_df.iteritems():
    print k, v
    text_file.write(str(k) + "\t" + str(v) +"\n")
text_file.close()

