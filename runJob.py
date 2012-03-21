#!/usr/bin/python
#
# author: Amy Zhang
#
#

import os

search = ['keywords','locations','years','months','days-of-the-month',
          'days-of-the-week','weekday','date-range','time-range',
	  'date-time-range','time-increment','average-over']

querykey = ['keyw','locs','yr','mnth','domnth','dowk','wkdy','dtrng','tmrng','dttmrng','tmincr','avg']
query = [0] * 12

nfile = open("job.properties.txt","r")
lines = nfile.readlines()

for line in lines:
    ans = line.split("=")
    c = 0
    for elem in search:
        if ans[0] == elem:
            query[c] = ans[1].replace('\n','')
            break
        c+=1

str = "./twitFilter2.py -l "

c2 = 0
for result in query:
    str+=querykey[c2] + "="
    if result == 0:
        str+="n "
    else:
        str+=result + " "
    c2+=1

str+= "< /home/shared/sm/location_twitterset/9-2010-04"

print str

os.system(str)
