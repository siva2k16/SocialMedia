#!/usr/bin/python
from __future__ import division
import MySQLdb
from dateutil import parser
from collections import defaultdict
import re
import datetime

db = MySQLdb.connect(host="10.47.85.51",    # your host, usually localhost
                     user="root",         # your username
                     passwd="test123",  # your password
                     db="test")        # name of thbase

def querydb(query):
    d_dataset = {}
    d_dataset = defaultdict(int)
    cur = db.cursor()
    cur.execute(query)
    i = 0
    j = 0
    dayofweek = 0
    for row in cur.fetchall():
        comments = row[0].split("?#+@")
        for comment in comments:
                try:
                    dt = parser.parse(comment[:19])
                    i = dt.hour
                    j = 0
                    dayofweek = dt.weekday()
                    #dayofweek = dt.isoweekday()
                    if(dt.year >= 2013 and dt.year<=2014):
                        if(dt.minute>=0 and dt.minute<=14):
                            j = 1
                        elif(dt.minute>=15 and dt.minute<=29):
                            j = 2
                        elif(dt.minute>=30 and dt.minute<=44):
                            j = 3
                        elif(dt.minute>=45 and dt.minute<60):
                            j = 4
                        bucket = (dayofweek*96)+(i*4+j)
                        d_dataset[bucket] +=1
                except ValueError:
                    dt = None

    return d_dataset

d_ecommerce = defaultdict(int)
d_traffic = defaultdict(int)
d_hospitals = defaultdict(int)
d_telecommunication = defaultdict(int)

d_ecommerce = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where p1.category = 'e-commerce';");
d_traffic = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where p1.category = 'Traffic';");
d_telecommunication = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where p1.category = 'Telecommunication';");
d_hospitals = querydb("select processedMessageandComment from comments c1 join postsummary p1 on c1.pid = p1.pid where p1.category = 'Hospital';");

print d_ecommerce
print d_traffic
print d_telecommunication
print d_hospitals

text_file = open("D:\Output_Exercise6_ECommerce_Output.dat", "w")
try:
    for k,v in d_ecommerce.iteritems():
        print k, v
        text_file.write(str(k) + " " + str(v) +"\n")
    text_file.close()
except ValueError:
    print 'No Ecommerce Data'
    text_file.close()

text_file = open("D:\Output_Exercise6_Traffic_Output.dat", "w")
try:
    for k,v in d_traffic.iteritems():
        print k, v
        text_file.write(str(k) + " " + str(v) +"\n")
    text_file.close()
except ValueError:
    print 'No Traffic Data'
    text_file.close()
    
text_file = open("D:\Output_Exercise6_Hospitals_Output.dat", "w")
try:
    for k,v in d_hospitals.iteritems():
        print k, v
        text_file.write(str(k) + " " + str(v) +"\n")
    text_file.close()
except ValueError:
    print 'No Hospital Data'
    text_file.close()
    
text_file = open("D:\Output_Exercise6_Telecommunication_Output.dat", "w")
try:
    for k,v in d_telecommunication.iteritems():
        print k, v
        text_file.write(str(k) + "\t" + str(v) +"\n")
    text_file.close()
except ValueError:
    print 'No Telecom Data'
    text_file.close()
    
