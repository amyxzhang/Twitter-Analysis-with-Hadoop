#!/usr/bin/env python
#
# file: twitFilter.py
# 
# author: Amy Zhang
#
#
#
#

from hstream import HStream
import sys
from collections import defaultdict
import calendar
from datetime import datetime, date, time

# main mapreduce class
class Histogram(HStream):
    
    # 1 - if line contains keyw
    def containsKeyword(self, line, keyw):
	if line.find(keyw) > -1:
		return 1
	return 0

    # 1 - if date is a weekday
    def isWeekday(self, date):
	dates = date.split('-')
	if len(dates) == 3:
	#	if calendar.weekday(int(dates[0]),int(dates[1]),int(dates[2])) < 5:
                if self.isDayOfWeek(date,5) == 0 and self.isDayOfWeek(date,6) == 0:
			return 1
	return 0
    
    # 1 - if locid is equal to loc
    def isLocation(self, locid, loc):
	if locid == loc:
		return 1
	return 0

    # 1 - if date contains year
    def isYear(self, date, year):
	dates = date.split('-')
	if len(dates) == 3:
		if dates[0] == year:
			return 1
	return 0

    # 1 - if date contains month (01-12 scale)
    def isMonth(self, date, month):
	dates = date.split('-')
	if len(dates) == 3:
		if dates[1] == month:
			return 1
	return 0

    # 1 - if date contains date (01-31 scale)
    def isDate(self, date, day):
	dates = date.split('-')
	if len(dates) == 3:
		if dates[2] == day:
			return 1
	return 0

    # 1 - if date is in day of the week (0 - M, 6 - Sun)
    def isDayOfWeek(self, date, dayOW):
	dates = date.split('-')
	if len(dates) == 3:
		if calendar.weekday(int(dates[0]),int(dates[1]),int(dates[2])) == int(dayOW):
			return 1
	return 0

    # 1 - if date is within range of dates
    def isDateRange(self, daten, begindate, enddate):
	dates = daten.split('-')
	bdates = begindate.split('-')
	edates = enddate.split('-')
	if len(dates) == 3 and len(bdates) == 3 and len(edates) == 3:
		tweetdate = date(int(dates[0]),int(dates[1]),int(dates[2]))
		bdate = date(int(bdates[0]),int(bdates[1]),int(bdates[2]))
		edate = date(int(edates[0]),int(edates[1]),int(edates[2]))
		if bdate < edate or bdate == edate:
			if bdate < tweetdate or bdate == tweetdate:
				if tweetdate < edate or tweetdate == edate:
					return 1
	return 0

    # 1 if time is within range of times
    def isTimeRange(self, timen, begintime, endtime):
	times = timen.split(':')
	btimes = begintime.split(':')
	etimes = endtime.split(':')
	if len(times) == 3 and len(btimes) == 3 and len(etimes) == 3:
		tweettime = time(int(times[0]),int(times[1]),int(times[2]))
		btime = time(int(btimes[0]),int(btimes[1]),int(btimes[2]))
		etime = time(int(etimes[0]),int(etimes[1]),int(etimes[2]))
		if btime < etime or btime == etime:
			if btime < tweettime or btime == tweettime:
				if tweettime < etime or tweettime == etime:
					return 1
	return 0

    # 1 if datetime is within range of datetimes
    def isDateAndTimeRange(self, daten, timen, begindate, begintime, enddate, endtime):
	dates = daten.split('-')
	times = timen.split(':')
	bdates = begindate.split('-')
	btimes = begintime.split(':')
	edates = enddate.split('-')
	etimes = endtime.split(':')
	if len(dates) == 3 and len(times) == 3 and len(bdates) == 3 and len(btimes) == 3 and len(edates) == 3 and len(etimes) == 3:
		tweetdt = datetime(int(dates[0]),int(dates[1]),int(dates[2]),int(times[0]),int(times[1]),int(times[2]))
		beforedt = datetime(int(bdates[0]),int(bdates[1]),int(bdates[2]),int(btimes[0]),int(btimes[1]),int(btimes[2]))
		enddt = datetime(int(edates[0]),int(edates[1]),int(edates[2]),int(etimes[0]),int(etimes[1]),int(etimes[2]))
		if beforedt < enddt or beforedt == enddt:
			if beforedt < tweetdt or beforedt == tweetdt:
				if tweetdt < enddt or tweetdt == enddt:
					return 1
	return 0

    # return 1 if tweet passes the filter, 0 if it fails the filter
    def filter(self, idfull, id, pub, cont, contclean, source, lang, userid, sn, crawlid, locid, google, publish):
       
	datetime = pub.split()
	tweetpasses = 1
        
#------------ restrict by location
        if self.locs != 'n':
            bloc = 0
            for location in self.locations:
                if self.isLocation(locid, location) == 1:
                    self.bucket[0] = locid
                    bloc = 1
                    break
            if bloc == 0:
                tweetpasses = 0
#------------ restrict by keyword
        if self.keyw != 'n' and tweetpasses == 1:
            bkey = 0
            keys = []
            for keyword in self.keywords:
                if self.containsKeyword(contclean,keyword) == 1:
                    keys.append(keyword)
                    bkey = 1
            self.bucket[1] = keys
            if bkey == 0:
                tweetpasses = 0
#------------ restrict by year
        if self.yr != 'n' and tweetpasses == 1:
            byr = 0
            for year in self.years:
                if self.isYear(datetime[0],year) == 1:
                    self.bucket[2] = year
                    byr = 1
                    break
            if byr == 0:
                tweetpasses = 0
#------------ restrict by month
        if self.mnth != 'n' and tweetpasses == 1:
            bmn = 0
            for month in self.months:
                if self.isMonth(datetime[0],month) == 1:
                    self.bucket[3] = month
                    bmn = 1
                    break
            if bmn == 0:
                tweetpasses = 0
#------------ restrict by day of month
        if self.domnth != 'n' and tweetpasses == 1:
            bdom = 0
            for day in self.days:
                if self.isDate(datetime[0],day) == 1:
                    self.bucket[4] = day
                    bdom = 1
                    break
            if bdom == 0:
                tweetpasses = 0
#------------ restrict by day of week
        if self.dowk != 'n' and tweetpasses == 1:
            bdow = 0
            for dow in self.dows:
                if self.isDayOfWeek(datetime[0],dow) == 1:
                    self.bucket[5] = dow
                    bdow = 1
                    break
            if bdow == 0:
                tweetpasses = 0
#------------ restrict by weekday/weekend
        if self.wkdy != 'n' and tweetpasses == 1:
            for wkd in self.weekday:
                if wkd == 'yes':
                    if self.isWeekday(datetime[0]) == 0:
                        tweetpasses = 0
                    else:
                        self.bucket[6] = "yes"
                if wkd == 'no':
                    if self.isWeekday(datetime[0]) == 1:
                        tweetpasses = 0
                    else:
                        self.bucket[6] = "no"
            if len(self.weekday) == 2:
                tweetpasses = 1
#------------ restrict by date range
        if self.dtrng != 'n' and tweetpasses == 1:
            if self.isDateRange(datetime[0],self.daterange[0],self.daterange[1]) == 0:
                tweetpasses = 0
            else:
                self.bucket[7] = str(self.daterange[0]) + ' ' + str(self.daterange[1])
#------------ restrict by time range
        if self.tmrng != 'n' and tweetpasses == 1:
            if self.isTimeRange(datetime[1],self.timerange[0],self.timerange[1]) == 0:
                tweetpasses = 0
            else:
                self.bucket[8] = str(self.timerange[0]) + ' ' + str(self.timerange[1])
#------------ restrict by datetime range
        if self.dttmrng != 'n' and tweetpasses == 1:
            if self.isDateAndTimeRange(datetime[0],datetime[1],self.datetimerange[0],self.datetimerange[1],self.datetimerange[2],self.datetimerange[3]) == 0:
                tweetpasses = 0
            else:
                self.bucket[9] = str(self.datetimerange[0]) + ' ' + str(self.datetimerange[1]) + ' ' + str(self.datetimerange[2]) + ' ' + str(self.datetimerange[3])

        
        return tweetpasses

    def increment(self, pub):
	datetime = pub.split()
	date = datetime[0].split('-')
	time = datetime[1].split(':')
	year = date[0]
	month = date[1]
	day = date[2]
	hour = time[0]
	min = time[1]

	if self.tmincr == 'mi':
		return "time-increment=" + datetime[0] + "_" + hour + ":" + min
	if self.tmincr == 'h':
		return "time-increment=" + datetime[0] + "_" + hour + ":00"
	if self.tmincr == 'd':
		return "time-increment=" + datetime[0]
	if self.tmincr == 'mo':
		return "time-increment=" + year + "-" + day 
	if self.tmincr == 'y':
		return "time-increment=" + year
	return ""

    def evaluate(self):
        sentence = ""

        if self.bucket[0] != 0:
            x = 1
            while x < len(self.columns[0]):
                if self.bucket[0] == self.columns[0][x]:
                    sentence += " location=" + self.bucket[0]
                    break
                x+=1
                
        if self.bucket[1] != []:
            x = 1
            y = 0
            z = 0
            while x < len(self.columns[1]):
                z = y
                while z < len(self.bucket[1]):
                    if self.bucket[1][z] == self.columns[1][x]:
                        sentence += " keyword=" + self.bucket[1][z]
                        y+=1
                    z+=1
                x+=1

        if self.bucket[2] != 0:
            x = 1
            while x < len(self.columns[2]):
                if self.bucket[2] == self.columns[2][x]:
                    sentence += " year=" + self.bucket[2]
                    break
                x+=1

        if self.bucket[3] != 0:
            x = 1
            while x < len(self.columns[3]):
                if self.bucket[3] == self.columns[3][x]:
                    sentence += " month=" + self.bucket[3]
                    break
                x+=1

        if self.bucket[4] != 0:
            x = 1
            while x < len(self.columns[4]):
                if self.bucket[4] == self.columns[4][x]:
                    sentence += " dayOfTheMonth=" + self.bucket[4]
                    break
                x+=1

        if self.bucket[5] != 0:
            x = 1
            while x < len(self.columns[5]):
                if self.bucket[5] == self.columns[5][x]:
                    sentence += " dayOfTheWeek=" + self.bucket[5]
                    break
                x+=1

        if self.bucket[6] != 0:
            x = 1
            while x < len(self.columns[6]):
                if self.bucket[6] == self.columns[6][x]:
                    sentence += " weekday=" + self.bucket[6]
                x+=1

        if self.bucket[7] != 0:
            x = 1
            while x < len(self.columns[7]):
                if self.bucket[7] == self.columns[7][x]:
                    sentence += " dateRange=" + self.bucket[7]
                x+=1

        if self.bucket[8] != 0:
            x = 1
            while x < len(self.columns[8]):
                if self.bucket[8] == self.columns[8][x]:
                    sentence += " timeRange=" + self.bucket[8]
                x+=1

        if self.bucket[9] != 0:
            x = 1
            while x < len(self.columns[9]):
                if self.bucket[9] == self.columns[9][x]:
                    sentence += " dateTimeRange=" + self.bucket[9]
                x+=1
	
        return sentence
        
    def checkaverage(self, pub):
	datetime = pub.split()
	date = datetime[0].split('-')
	time = datetime[1].split(':')
	year = date[0]
	month = date[1]
	day = date[2]
	hour = time[0]
	min = time[1]
	avginfo = str(year)

	if self.avg == 'y':
	    return avginfo
	avginfo += '-' + str(month)
	if self.avg == 'mo':
	    return avginfo
	avginfo += '-' + str(day)
	if self.avg == 'd':
	    return avginfo
	avginfo += '-' + str(hour)
	if self.avg == 'h':
	    return avginfo
	avginfo += '-' + str(min)
	if self.avg == 'mi':
	    return avginfo
	return ''

    def mapper(self, record):
        if len(record) == 13:
                idfull, id, pub, cont, contclean, source, lang, userid, sn, crawlid, locid, google, publish = record

                self.bucket = [0] * 10
		
		datetime = pub.split()

		if len(datetime) == 2:
                    tweetpasses = self.filter(idfull, id, pub, cont, contclean, source, lang, userid, sn, crawlid, locid, google, publish)
		    sentence = self.evaluate()
		    incr = self.increment(pub)
                    
		    sentence += ' ' + incr
		    if self.avg != 'n':
			    avginfo =  self.checkaverage(pub)
			    self.write_output((sentence, avginfo, tweetpasses))
		    else:
                    	    self.write_output((sentence, '', tweetpasses))
		else:
			self.write_output(("error", "not a line of data", 1))
	else:
		self.write_output(("error", "not correctly formatted data", 1))

 
    def reducer(self, key, records):

        total = 0
        count = 0
	dates = ''
	datecount = 0
	if self.avg == 'n':
        	for record in records:
                	info, extra, count = record
                	total += 1
                	count += int(counts)
        	self.write_output( (info, extra, count, total) )
	else:
		for record in records:
			info, datetime, counts = record
			total += 1
			count += int(counts)
			if dates.find(datetime) == -1:
				dates += datetime
				datecount += 1
		avg = float(count/datecount)
		self.write_output((info, avg, datecount, count, total))





if __name__ == '__main__':

    Histogram()
