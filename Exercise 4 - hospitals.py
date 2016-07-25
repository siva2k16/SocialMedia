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

d_Fotris = defaultdict(int)
sum_Fortis = 0

d_Ambani = defaultdict(int)
sum_Ambani = 0

d_Apollo = defaultdict(int)
sum_Apollo = 0

d_Fotris, sum_Fortis = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Fortis Healthcare';")

d_Ambani, sum_Ambani = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Kokilaben Dhirubhai Ambani Hospital';")

d_Apollo, sum_Apollo = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Apollo Hospitals';")

Fortis_df = defaultdict(float)
Ambani_df = defaultdict(float)
Apollo_df = defaultdict(float)

f_wp_Fortis =  0.0
f_wp_Fortis = sum_Fortis/(sum_Fortis+sum_Ambani+sum_Apollo)

f_wp_Ambani =  0.0
f_wp_Ambani = sum_Ambani/(sum_Fortis+sum_Ambani+sum_Apollo)

f_wp_Apollo =  0.0
f_wp_Apollo = sum_Apollo/(sum_Fortis+sum_Ambani+sum_Apollo)

Fortis_df = computeqk(d_Fotris,f_wp_Fortis)
Ambani_df = computeqk(d_Ambani,f_wp_Ambani)
Apollo_df = computeqk(d_Apollo,f_wp_Apollo)

results_df = defaultdict(float)	

for key in range(1,97):
	results_df[key] = float(Fortis_df[key]) + float(Ambani_df[key]) + float(Apollo_df[key]) 

print results_df

text_file = open("D:\Output_Exercise4_Hospitals_Output.dat", "w")
for k,v in results_df.iteritems():
    print k, v
    text_file.write(str(k) + "\t" + str(v) +"\n")
text_file.close()

