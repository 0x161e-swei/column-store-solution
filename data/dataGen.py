#!/usr/bin/python
# -*- coding: utf-8 -*-

import math
import random
import numpy
import ConfigParser
from array import *

# number of workload
lines = 100
datasize = 10000
columnNum = 1 
dataDomain = 1000000000

columnNameDict = 	{	
						0: 'a', 1: 'b', 2: 'c', 3: 'd', 4: 'e', 5: 'f',
						6: 'g', 7: 'h', 8: 'i', 9: 'j',	10: 'k', 11: 'l',
						12: 'm', 13: 'n', 14: 'o', 15: 'p', 16: 'q', 17: 'r',
						18: 's', 19: 't', 20: 'u', 21: 'v', 22: 'w', 23: 'x',
						24: 'y', 25: 'z', 26: 'A', 27: 'B', 28: 'C', 29: 'D',
						30: 'E', 31: 'F', 32: 'G', 33: 'H', 34: 'I', 35: 'J',
						36: 'K', 37: 'L', 38: 'M', 39: 'N', 40: 'O', 41: 'P',
						42: 'Q', 43: 'R', 44: 'S', 45: 'T', 46: 'U', 47: 'V',
						48: 'W', 49: 'X', 50: 'Y', 51: 'Z', 52: 'aa', 53: 'bb',
						54: 'cc', 55: 'dd', 56: 'ee', 57: 'ff', 58: 'gg', 59: 'hh'
					}
col_name = 'foo.tb1.a'
tbl_name = 'foo.tb1'
configSection = 'dataset'
data_filename = 'dataset'
setup_filename = 'setupddl'
config = ConfigParser.ConfigParser(allow_no_value=True)
data = []

def readInt(option, default):
	target = default
	try:
		value = config.getint(configSection, option)
		target = value
		print option + ' specified as ' + str(target)
	except TypeError:
		value = config.get(configSection, option)
		if value is None:
			print 'empty value for ' + option + ' default to ' + str(default)
		else:
			print 'wrong format, ' + option + 'should be int'
	return target


def readFloat(option, default):
	target = default
	try:
		value = config.getfloat(configSection, option)
		target = value
		print option + ' specified as ' + str(target)
	except TypeError:
		value = config.get(configSection, option)
		if value is None:
			print 'empty value for ' + option + ' default to ' + str(default)
		else:
			print 'wrong format, ' + option + 'should be float'
	return target


def dataGen(dis):
	global data
	global data_filename
	global setup_filename
	zipf_s = 1.2
	gauss_mean = int(dataDomain / 2)
	# uniform distribution
	if dis == 'uniform':
		data = random.sample(xrange(dataDomain), datasize)
		data_filename += '_unif_'
		setup_filename += '_unif_'
	# zipf 
	elif dis == 'zipf':
		zipf_s = readFloat('zipf_s', zipf_s)
		data = numpy.random.zipf(zipf_s, datasize)
		data_filename += '_zipf_'
		setup_filename += 'zipf_'
	elif dis == 'gauss':
		gauss_mean = readInt('gauss_mean', gauss_mean)
		data = numpy.random.normal(gauss_mean, dataDomain / 4, datasize)
		data_filename += '_gaus_'
		setup_filename += '_gaus_'
	data_filename = data_filename + str(datasize) + '_' + str(columnNum)
	print "gen done"


def genText():
	fdata = open(data_filename, 'w')
	fdata.write(str(datasize) + ',' + str(columnNum) + '\n')
	# write the column names
	# foo.tb1.a is always going to be the column to partition
	fdata.write(col_name)
	for i in range(1, columnNum):
		fdata.write(',' + tbl_name + '.' + columnNameDict[i])
	fdata.write('\n')
	# write data
	for i in range(datasize  / 10):
		w = ''
		for k in range(10):
			w += str(int(data[i * 10 + k]))
			# write data in other column
			for j in range(1, columnNum):
				w = w + ',' + str(data[i * 10 + k])
			w += '\n'
		fdata.write(w)
	fdata.close()		


def genBin():
	fp = open('dbinfo', 'wb')
	nInt = array('i')
	nInt.append(3)	# length of database name for foo
	fp.write(nInt)
	fp.write('foo') # database name
	nInt.remove(3)
	nSizet = array('l')
	nSizet.append(1) # table count
	nInt.append(7) # length of table name for foo.tb1
	fp.write(nSizet)
	fp.write(nInt)
	nSizet.remove(1)
	nInt.remove(7)
	fp.write('foo.tb1') # table name 
	nSizet.append(datasize)  # table length
	nSizet.append(columnNum) # column count
	fp.write(nSizet)
	nSizet.remove(datasize)
	nSizet.remove(columnNum)
	for i in range(columnNum):
		nameCol = tbl_name + '.' + columnNameDict[i]
		l = len(nameCol)
		nInt.append(l)
		fp.write(nInt)
		nInt.remove(l)
		fp.write(nameCol)
	fp.close()
	arr = array('i', data)
	for i in range(columnNum):
		filname = tbl_name + '.' + columnNameDict[i]
		fdata = open(filname, 'wb')
		arr.tofile(fdata)
		fdata.close()


def ddlGen():
	global setup_filename
	# generate the ddl to setup the database
	setup_filename = setup_filename + str(datasize) + '_' + str(columnNum)
	fddl = open(setup_filename, 'w')
	fddl.write('create(db,\"foo\")\n')
	fddl.write('create(tbl,\"tb1\",foo,' + str(columnNum) +')\n')
	for i in range(columnNum):
		fddl.write('create(col,\"' + columnNameDict[i]+ '\",foo.tb1,unsorted)\n')
	fddl.write('load(\"data/' + data_filename + '\")\n')
	fddl.write('quit\n')
	fddl.close()


if __name__ == '__main__':
	config.read('gen.cfg')
	datasize = readInt('col_len', datasize)
	columnNum = readInt('col_num', columnNum)
	dis = config.get(configSection, 'distribution')
	print 'distribution specified as ' + dis
	if dis is None:
		print 'empty distribution option, default to uniform'
		dis = 'uniform'
	dataGen(dis)
	ddlGen()
	binary = 1
	binary = readInt('binary', binary)
	if binary == 1:
		genBin()
	else:
		genText()






