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

d_Flipkart = defaultdict(int)
d_Amazon = defaultdict(int)
d_Snapdeal = defaultdict(int)
d_Myntra = defaultdict(int)

d_Flipkart = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Flipkart';");
d_Amazon = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Amazon India';");
d_Snapdeal = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Snapdeal';");
d_Myntra = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where organizattionName = 'Myntra';");

text_file = open("D:\Output_Exercise3_Flipkart_Output.dat", "w")
for k,v in d_Flipkart.iteritems():
    print k, v
    text_file.write(str(k) + " " + str(v) +"\n")
text_file.close()

text_file = open("D:\Output_Exercise3_Amazon_Output.dat", "w")
for k,v in d_Amazon.iteritems():
    print k, v
    text_file.write(str(k) + " " + str(v) +"\n")
text_file.close()

text_file = open("D:\Output_Exercise3_Snapdeal_Output.dat", "w")
for k,v in d_Snapdeal.iteritems():
    print k, v
    text_file.write(str(k) + " " + str(v) +"\n")
text_file.close()

text_file = open("D:\Output_Exercise3_Myntra_Output.dat", "w")
for k,v in d_Myntra.iteritems():
    print k, v
    text_file.write(str(k) + "\t" + str(v) +"\n")
text_file.close()
