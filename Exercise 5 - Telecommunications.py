#!/usr/bin/python
from __future__ import division
import MySQLdb
from dateutil import parser
from collections import defaultdict
from decimal import Decimal
import re
import numpy as np

db = MySQLdb.connect(host="10.47.85.51",    # your host, usually localhost
                     user="root",         # your username
                     passwd="test123",  # your password
                     db="test")        # name of the data base

def querydb(query):
    d_dataset = defaultdict(int)
    sum_dataset = 0
    reccount = 0
    cur = db.cursor()
    cur.execute(query)
    i = 0
    j = 0
    reccount = cur.rowcount
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
    return d_dataset, sum_dataset, reccount

d_telecom = defaultdict(int)
sum_telecom = 0
reccount = 0

d_telecom_traffic = defaultdict(int)
sum_telecom_traffic = 0
rec_time_count = 0

d_telecom, sum_telecom, reccount = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where category = 'Telecommunication';")

d_telecom_traffic, sum_telecom_traffic, rec_time_count = querydb("select createdTime from comments c1 join postsummary p1 on c1.pid = p1.pid  where category = 'Telecommunication' and createdTime >= '2013-01-01' and createdTime < '2015-01-01';")

rpm_p =  0.0
rpm_p = sum_telecom/rec_time_count

results_df = defaultdict(float)	

for key in range(1,97):
    if(d_telecom_traffic[key] > 0):
        step1 = float(d_telecom[key]/d_telecom_traffic[key])
        step2 = float(step1/rpm_p)
        results_df[key] = float(step2)
    else:
        results_df[key] = 0

text_file = open("D:\\Output_Exercise5_Telecom_Output.dat", "w")
for k,v in results_df.iteritems():
    print k, v
    text_file.write(str(k) + "\t" + str(v) +"\n")
text_file.close()
