#!/usr/bin/python
import MySQLdb
from dateutil import parser
from collections import defaultdict
import re
import datetime
import csv
db = MySQLdb.connect(host="10.47.85.51",    # your host, usually localhost
                     user="root",         # your username
                     passwd="test123",  # your password
                     db="test")        # name of thbase

def querydb(query):
    d_dataset = defaultdict(int)
    cur = db.cursor()
    cur.execute(query)
    i = 0
    j = 0
    # print all the first cell of all the rows
    for row in cur.fetchall():
        comments = row[0].split("?#+@")
        for comment in comments:
            #if '+0000' in comment:
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
    return d_dataset

d_Kolkatta_Traffic = defaultdict(int)
d_Bangalore_Traffic = defaultdict(int)
d_Hyderabad_Traffic = defaultdict(int)
d_Delhi_Traffic = defaultdict(int)

d_Kolkatta_Traffic = querydb("select createdTime from postsummary where organizattionName = 'Kolkata Traffic Police'");
d_Bangalore_Traffic = querydb("select createdTime from postsummary where organizattionName = 'Bengaluru Traffic Police'");
d_Hyderabad_Traffic = querydb("select createdTime from postsummary where organizattionName = 'Hyderabad Traffic Police'");
d_Delhi_Traffic = querydb("select createdTime from postsummary where organizattionName = 'Delhi Traffic Police'");

text_file = open("D:\Output_Exercise2_KLK_Traffic_Output.dat", "w")
for k,v in d_Kolkatta_Traffic.iteritems():
    print k, v
    text_file.write(str(k) + " " + str(v) +"\n")
text_file.close()

text_file = open("D:\Output_Exercise2_BLR_Traffic_Output.dat", "w")
for k,v in d_Bangalore_Traffic.iteritems():
    print k, v
    text_file.write(str(k) + " " + str(v) +"\n")
text_file.close()

text_file = open("D:\Output_Exercise2_DELHI_Traffic_Output.dat", "w")
for k,v in d_Delhi_Traffic.iteritems():
    print k, v
    text_file.write(str(k) + " " + str(v) +"\n")
text_file.close()

text_file = open("D:\Output_Exercise2_HYD_Traffic_Output.dat", "w")
for k,v in d_Hyderabad_Traffic.iteritems():
    print k, v
    text_file.write(str(k) + "\t" + str(v) +"\n")
text_file.close()
