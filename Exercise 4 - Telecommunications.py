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

d_Idea = defaultdict(int)
sum_Idea = 0

d_Docomo = defaultdict(int)
sum_Docomo = 0

d_Aircel = defaultdict(int)
sum_Aircel = 0

d_Idea, sum_Idea = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Idea';")

d_Docomo, sum_Docomo = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Tata Docomo';")

d_Aircel, sum_Aircel = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Aircel India';")

Idea_df = defaultdict(float)
Docomo_df = defaultdict(float)
Aircel_df = defaultdict(float)

f_wp_Idea =  0.0
f_wp_Idea = sum_Idea/(sum_Idea+sum_Docomo+sum_Aircel)

f_wp_Docomo =  0.0
f_wp_Docomo = sum_Docomo/(sum_Idea+sum_Docomo+sum_Aircel)

f_wp_Aircel =  0.0
f_wp_Aircel = sum_Aircel/(sum_Idea+sum_Docomo+sum_Aircel)

Idea_df = computeqk(d_Idea,f_wp_Idea)
Docomo_df = computeqk(Docomo_df,f_wp_Docomo)
Aircel_df = computeqk(Aircel_df,f_wp_Aircel)

results_df = defaultdict(float)	

for key in range(1,97):
	results_df[key] = float(Idea_df[key]) + float(Docomo_df[key]) + float(Aircel_df[key]) 

print results_df

text_file = open("D:\Output_Exercise4_Telecommunication_Output.dat", "w")
for k,v in results_df.iteritems():
    print k, v
    text_file.write(str(k) + "\t" + str(v) +"\n")
text_file.close()

