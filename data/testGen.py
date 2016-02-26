#!/usr/bin/python
# -*- coding: utf-8 -*-
import math
import random 
import numpy
# point query distribution parameter
pq_alpha = 2
pq_beta = 5

# range query distribution parameter
rq_alpha = 1
rq_beta = 3

# insert distribution parameter
i_alpha = 5
i_beta = 1

# update distribution parameter
u_alpha = 2
u_beta = 2

# d distribution parameter
d_alpha = 0.5
d_beta = 0.5

# number of workload
lines = 100
datasize = 10000
columnNum = 2

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

def gauss_num():
	x = random.gauss(2, 1)
	while x < 0 or x > 4:
		x = random.gauss(2, 1)
	return int(x * dataDomain / 4)


def dataGen(t):
	data2 = range(0, datasize)
	# uniform distribution
	if t == 0:
		data = random.sample(xrange(dataDomain), datasize)
	# zipf 
	elif t == 1:
		print 'input parameter a'
		a = input()
		data = random.zipf(a, datasize)
	elif t == 2:
		print 'input parameter a'
		a = input()
		data = random.normal(a, datasize)
	data_filename = 'dataset_' + str(datasize) + '_' + str(columnNum)
	fdata = open(data_filename, 'w')
	fdata.write(str(datasize) + ',' + str(columnNum) + '\n')
	# write the column names
	# foo.tb1.a is always going to be the column to partition
	fdata.write(col_name)
	for i in range(1, columnNum):
		fdata.write(',' + tbl_name + '.' + columnNameDict[i])
	fdata.write('\n')
	# write data
	for i in range(datasize):
		fdata.write(str(data[i]))
		# write data in other column
		for j in range(1, columnNum):
			fdata.write(',' + str(data2[i]))
		fdata.write('\n')
	fdata.close()
	# generate the ddl to setup the database
	setup_filename = 'setupddl_' + str(datasize) + '_' + str(columnNum)
	fddl = open(setup_filename, 'w')
	fddl.write('create(db,\"foo\")\n')
	fddl.write('create(tbl,\"tb1\",foo,' + str(columnNum) +')\n')
	for i in range(columnNum):
		fddl.write('create(col,\"' + columnNameDict[i]+ '\",foo.tb1,unsorted)\n')
	fddl.write('load(\"data/' + data_filename + '\")\n')
	fddl.write('quit\n')
	fddl.close()


def workGen(d):
	filename = 'workload' + str(d)
	fwork = open(filename, 'w')
	for i in range(lines):
		t = random.randint(0, 3)
		if t == 0:	# point query
			num = int(random.betavariate(pq_alpha, pq_beta) * dataDomain)
			num_1 = num + 1
			fwork.write('p=s('+ col_name + ',' + str(num) + ',' + str(num_1) + ')\n')
		elif t == 1:	# range query
			low = int(random.betavariate(rq_alpha, rq_beta) * dataDomain)
			high = int(random.betavariate(rq_alpha, rq_beta) * dataDomain)
			if low < high:
				fwork.write('p=s('+ col_name + ',' + str(low) + ',' + str(high) + ')\n')
			else:
				fwork.write('p=s('+ col_name + ',' + str(high) + ',' + str(low) + ')\n')
		elif t == 2: # insert
			i_num = int(random.betavariate(i_alpha, i_beta) * dataDomain)
			fwork.write('i('+ tbl_name + ',' + str(i_num))
			for j in range(columnNum - 1):
				fwork.write(',1')	# other columns
			fwork.write(')\n')
		elif t == 3: # update
			old = int(random.betavariate(u_alpha, u_beta) * dataDomain)
			new = int(random.betavariate(u_alpha, u_beta) * dataDomain)
			fwork.write('u('+ col_name + ',' + str(old) + ',' + str(new) + ')\n')
		elif t == 4: # delte
			d_num = int(random.betavariate(d_alpha, d_beta) * dataDomain)
			fwork.write('d('+ col_name + ',' + str(d_num) + ')\n')
	fwork.close()



if __name__ == '__main__':
	print "input 0 for workload 1 for data generator"
	d = input()
	if d == 0:	#workload
		print 'input number of workload'
		lines = input()
		print 'input number of columns'
		columnNum = input()
		print 'generating workload...'
		workGen(lines)
	else:
		print 'input size of dataset'
		datasize = input()
		print 'input number of columns'
		columnNum = input()
		print 'input type of distribution'
		print '0: Uniform; 1: Zipfian; 2: Gaussian; 3: i dont konw yet..' 
		t = input()
		print 'generating data with gauss distribution...'
		dataGen(t)

