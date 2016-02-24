#!/usr/bin/python
# -*- coding: utf-8 -*-
import math
import random

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
dataDomain = 50000
datasize = 10000

def gauss_num():
	x = random.gauss(2, 1)
	while x < 0 or x > 4:
		x = random.gauss(2, 1)
	return int(x / 4 * dataDomain)

def dataGen(t):
	data1 = range(0, dataDomain)
	data2 = range(0, datasize)
	data = []
	random.shuffle(data1)
	for i in range(datasize):
		num = gauss_num()
		while num in data:
			num = gauss_num()
		data.append(num)
	fp = open('dataset', 'w')
	fp.write("foo.tb1.a,foo.tb1.b\n")
	for i in range(datasize):
		fp.write(str(data[i]) + ',' + str(data2[i]) + '\n')

def workGen():
	col_name = 'foo.tb1.a'
	tbl_name = 'foo.tb1'
	fp = open('workload', 'w')
	for i in range(lines):
		t = random.randint(0, 3)
		if t == 0:	# point query
			num = int(random.betavariate(pq_alpha, pq_beta) * dataDomain)
			num_1 = num + 1
			fp.write('p=s('+ col_name + ',' + str(num) + ',' + str(num_1) + ')\n')
		elif t == 1:	# range query
			low = int(random.betavariate(rq_alpha, rq_beta) * dataDomain)
			high = int(random.betavariate(rq_alpha, rq_beta) * dataDomain)
			if low < high:
				fp.write('p=s('+ col_name + ',' + str(low) + ',' + str(high) + ')\n')
			else:
				fp.write('p=s('+ col_name + ',' + str(high) + ',' + str(low) + ')\n')
		elif t == 2: # insert
			i_num = int(random.betavariate(i_alpha, i_beta) * dataDomain)
			fp.write('i('+ tbl_name + ',' + str(i_num) + ',1)\n')	# two columns 
		elif t == 3: # update
			old = int(random.betavariate(u_alpha, u_beta) * dataDomain)
			new = int(random.betavariate(u_alpha, u_beta) * dataDomain)
			fp.write('u('+ col_name + ',' + str(old) + ',' + str(new) + ')\n')
		elif t == 4: # delte
			d_num = int(random.betavariate(d_alpha, d_beta) * dataDomain)
			fp.write('u('+ col_name + ',' + str(old) + ',' + str(new) + ')\n')


if __name__ == '__main__':
	print "input 0 for workload and otherwise for data generator"
	d = input()
	if d == 0:	#workload
		print 'generating workload...'
		workGen()
	else:
		print 'generating data with gauss distribution...'
		dataGen(d)

