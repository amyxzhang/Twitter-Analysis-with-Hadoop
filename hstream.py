#!/usr/bin/env python
#
# file: hstream.py
#
# description: simple class for implementing hadoop streaming jobs in python
#   adapted from: 
#     http://www.michael-noll.com/wiki/Writing_An_Hadoop_MapReduce_Program_In_Python
#
# usage: inherit the HStream class and define the mapper and reducer
# functions, e.g. in myjob.py:
#
#   from hstream import HStream
#
#   class MyJob(Hstream):
#     def mapper(self, record):
#       # ...
#       self.write_output( (key, value) )
#
#     def reducer(self, key, records):
#       # ...
#       for record in records:
#           # ...
#       self.write_output( (key, value) )
#
#   if __name__=='__main__':
#     MyJob()
#
# to run mapper: ./myjob.py -m < input
# to run reducer: ./myjob.py -m < input
# to run full job: ./myjob.py -l < input
#
# to specify additional command line arguments, append arg=val after
# the -[m|r|l] switch, e.g. ./myjob.py -m max=10. you can then
# retrieve these arguments using self.args,
# e.g. self.args['max']. this is often useful for loading a parameter
# when initializing the mapper or reducer, e.g. in mapper_init,
# self.max = self.args['max'], which will then be acessible as
# self.max in mapper().
# 
# see examples for more details.
#
# author: jake hofman (hofman@yahoo-inc.com)
#

from itertools import groupby
from operator import itemgetter
import sys
from optparse import OptionParser
from StringIO import StringIO


class HStream:
    """
    simple wrapper class to facilitate writing hadoop streaming jobs
    in python. inherit the class and define mapper and reducer functions.
    see header of hstream.py for more details.
    """
    default_delim='\t'
    default_istream=sys.stdin
    default_ostream=sys.stdout
    default_estream=sys.stderr
    
    def __init__(self,
                 delim=default_delim,
                 istream=default_istream,
                 ostream=default_ostream):
        self.delim=delim
        self.istream=istream
        self.ostream=ostream

        self.parse_args()
        
    def read_input(self): 
        for line in self.istream:
            yield line.rstrip('\n').split(self.delim)

    def write_output(self,s):
        if type(s) is str:
            self.ostream.write(s + '\n')
        else:
            self.ostream.write(self.delim.join(map(str,s)) + '\n')

    def map(self):
        self.mapper_init()

        for record in self.read_input():
            self.mapper(record)

        self.mapper_end()

    def reduce(self):
        self.reducer_init()

        data = self.read_input()
        for key, records in groupby(data, itemgetter(0)):
            self.reducer(key, records)

        self.reducer_end()

    def combine(self):
        data = self.read_input()
        self.combiner( data )

    def mapper_init(self):
	
	# code insert for Twitter Term Analysis
	# written by Amy X Zhang
      	self.locs = self.args['locs']
        self.keyw = self.args['keyw']
		self.yr = self.args['yr']
		self.mnth = self.args['mnth']
		self.domnth = self.args['domnth']
		self.dowk = self.args['dowk']
		self.wkdy = self.args['wkdy']
		self.dtrng = self.args['dtrng']
		self.tmrng = self.args['tmrng']
		self.dttmrng = self.args['dttmrng']
		self.tmincr = self.args['tmincr']
		self.avg = self.args['avg']

			self.columns = [['locs'],['keyw'],['yr'],['mnth'],['domnth'],
							['dowk'],['wkdy'],['dtrng'],['tmrng'],['dttmrng']]
		
		self.locations = self.locs.split(',')
			
		if self.locations[0] == 'all*':
			self.locations = [0] * 57
			for i in range(1, 58):
				self.locations[i-1] = str(i)
				self.columns[0].append(self.locations[i-1])
		else:
		
			x = 0
				while x < len(self.locations):
						if self.locations[x].endswith('*'):
							self.locations[x] = self.locations[x].replace('*','')
							self.columns[0].append(self.locations[x])
						x+=1
				
		

		self.keywords = self.keyw.split(',')

			x = 0
			while x < len(self.keywords):
				if self.keywords[x].endswith('*'):
					self.keywords[x] = self.keywords[x].replace('*','')
					self.columns[1].append(self.keywords[x])
				x+=1
			
		self.years = self.yr.split(',')

			x = 0
			while x < len(self.years):
				if self.years[x].endswith('*'):
					self.years[x] = self.years[x].replace('*','')
					self.columns[2].append(self.years[x])
				x+=1

				
		self.months = self.mnth.split(',')

		if self.months[0] == 'all*':
			self.months = [0] * 12
			for i in range(0, 12):
						if i < 9:
							self.months[i] = '0' + str(i+1)
						else:
							self.months[i] = str(i+1)
						self.columns[3].append(self.months[i])
		else:
				x = 0
				while x < len(self.months):
					if self.months[x].endswith('*'):
						self.months[x] = self.months[x].replace('*','')
						self.columns[3].append(self.months[x])
					x+=1

			
		self.days = self.domnth.split(',')

		if self.days[0] == 'all*':
			self.days = [0] * 31
			for i in range(0, 31):
						if i < 9:
							self.days[i] = '0' + str(i+1)
						else:
							self.days[i] = str(i+1)
						self.columns[4].append(self.days[i])
		else:
				x = 0
				while x < len(self.days):
					if self.days[x].endswith('*'):
						self.days[x] = self.days[x].replace('*','')
						self.columns[4].append(self.days[x])
					x+=1


		self.dows = self.dowk.split(',')

		if self.dows[0] == 'all*':
			self.dows = [0] * 7
			for i in range(0, 7):
						self.dows[i] = '0' + str(i)
						self.columns[5].append(self.dows[i])
		else:
				x = 0
				while x < len(self.dows):
					if self.dows[x].endswith('*'):
						self.dows[x] = self.dows[x].replace('*','')
						self.columns[5].append(self.dows[x])
					x+=1

			self.weekday = self.wkdy.split(',')
			x = 0
			while x < len(self.weekday):
				if self.weekday[x].endswith('*'):
					self.weekday[x] = self.weekday[x].replace('*','')
					self.columns[6].append(self.weekday[x])
				x+=1

		self.daterange = self.dtrng.split(',')

			if len(self.daterange) == 2:
				if self.daterange[1].endswith('*'):
					self.daterange[1] = self.daterange[1].replace('*','')
					self.columns[7].append(self.daterange[0] + ' ' +
									self.daterange[1])
			
		self.timerange = self.tmrng.split(',')

			if len(self.timerange) == 2:
				if self.timerange[1].endswith('*'):
					self.timerange[1] = self.timerange[1].replace('*','')
					self.columns[8].append(self.timerange[0] + ' ' +
									self.timerange[1])
			
		self.datetimerange = self.dttmrng.split(',')

			if len(self.datetimerange) == 4:
				if self.datetimerange[3].endswith('*'):
					self.datetimerange[3] = self.datetimerange[3].replace('*','')
					self.columns[9].append(self.datetimerange[0] + ' ' +
									self.datetimerange[1] + ' ' + 
									self.datetimerange[2] + ' ' +
									self.datetimerange[3])

			print self.columns
		return

	#end inserted code
	
    def mapper(self, record):
        self.write_output(record[0])

    def mapper_end(self):
        return

    def reducer_init(self):
        return

    def reducer(self, key, records):
        for record in records:
            self.write_output(self.delim.join(record))

    def reducer_end(self):
        return

    def combiner(self, records):
        for record in records:
            self.write_output(self.delim.join(record))

    def parse_args(self):
        
        parser=OptionParser()
        parser.add_option("-m","--map",
                          help="run mapper",
                          action="store_true",
                          dest="run_map",
                          default="False")
        parser.add_option("-r","--reduce",
                          help="run reduce",
                          action="store_true",
                          dest="run_reduce",
                          default="False")
        parser.add_option("-c","--combine",
                          help="run combiner",
                          action="store_true",
                          dest="run_combine",
                          default="False")
        parser.add_option("-l","--local",
                          help="run local test of map | sort | reduce",
                          action="store_true",
                          dest="run_local",
                          default="False")

        opts, args = parser.parse_args()

        self.args=dict([s.split('=',1) for s in args])

        if opts.run_map is True:
            self.map()
        elif opts.run_reduce is True:
            self.reduce()
        elif opts.run_combine is True:
            self.combine()
        elif opts.run_local is True:
            self.run_local()
            

    def run_local(self):
        map_output=StringIO()

        # map stdin to temporary string stream
        self.istream=sys.stdin
        self.ostream=map_output
        self.map()

        # sort string stream
        map_output.seek(0)        
        reduce_input=StringIO(''.join(sorted(map_output)))

        # reduce string stream to stdout
        self.istream=reduce_input
        self.ostream=sys.stdout
        self.reduce()

        
if __name__=='__main__':
    
    pass
