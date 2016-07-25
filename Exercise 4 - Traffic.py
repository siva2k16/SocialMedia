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

d_Hyderabad = defaultdict(int)
sum_Hyderabad = 0

d_Kolkatta = defaultdict(int)
sum_Kolkatta = 0

d_Delhi = defaultdict(int)
sum_Delhi = 0

d_BTP = defaultdict(int)
sum_BTP = 0

d_Hyderabad, sum_Hyderabad = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Hyderabad Traffic Police';")

d_Kolkatta, sum_Kolkatta = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Kolkata Traffic Police';")

d_Delhi, sum_Delhi = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Delhi Traffic Police';")

d_BTP, sum_BTP = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Bengaluru Traffic Police';")

BTP_df = defaultdict(float)
Delhi_df = defaultdict(float)
Kolkatta_df = defaultdict(float)
Hyderabad_df = defaultdict(float)

f_wp_BTP =  0.0
f_wp_BTP = sum_BTP/(sum_BTP+sum_Kolkatta+sum_Delhi+sum_Hyderabad)

f_wp_Kolkatta =  0.0
f_wp_Kolkatta = sum_Kolkatta/(sum_BTP+sum_Kolkatta+sum_Delhi+sum_Hyderabad)

f_wp_Hyderabad =  0.0
f_wp_Hyderabad = sum_Hyderabad/(sum_BTP+sum_Kolkatta+sum_Delhi+sum_Hyderabad)

f_wp_Delhi =  0.0
f_wp_Delhi = sum_Delhi/(sum_BTP+sum_Kolkatta+sum_Delhi+sum_Hyderabad)

BTP_df = computeqk(d_BTP,f_wp_BTP)
Kolkatta_df = computeqk(d_Kolkatta,f_wp_Kolkatta)
Hyderabad_df = computeqk(d_Hyderabad,f_wp_Hyderabad)
Delhi_df = computeqk(d_Delhi,f_wp_Delhi)

print f_wp_BTP
print f_wp_Kolkatta
print f_wp_Hyderabad
print f_wp_Delhi

results_df = defaultdict(float)	

for key in range(1,97):
	results_df[key] = float(BTP_df[key]) + float(Delhi_df[key]) + float(Hyderabad_df[key]) + float(Kolkatta_df[key])
	print float(BTP_df[key])
	print float(Delhi_df[key])
	print float(Hyderabad_df[key])
	print float(Kolkatta_df[key]) 

print results_df

text_file = open("D:\Output_Exercise4_Traffic_Output.dat", "w")
for k,v in results_df.iteritems():
    print k, v
    text_file.write(str(k) + "\t" + str(v) +"\n")
text_file.close()

