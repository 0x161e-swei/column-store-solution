#! /usr/bin/python
# -*- coding: utf-8 -*-
import math
import numpy
import ConfigParser

Ppq = 0.2
Prq = 0.2
Pin = 0.2
Pup = 0.2
Pde = 0.2
pq_dis = 'uniform'
rq_sel_dis = 'uniform'
rq_end_dis = 'uniform'
in_dis = 'uniform'
up_dis = 'uniform'
de_dis = 'uniform'
pq_para_0 = 0
pq_para_1 = 0
rq_sel_para_0 = 0
rq_sel_para_1 = 0
rq_end_para_0 = 0
rq_end_para_1 = 0
in_para_0 = 0
in_para_1 = 0
up_para_0 = 0
up_para_1 = 0
de_para_0 = 0
de_para_1 = 0

config = ConfigParser.ConfigParser(allow_no_value=True)
configSection = 'dataset'
lines = 100
columnNum = 1
dataDomain = 1000000000
tbl_name = 'foo.tb1'
col_name = 'foo.tb1.a'


def numberGen(dis, para_0, para_1, domain):
	num = 0
	if dis == 'beta':
		num = numpy.random.beta(para_0, para_1) * domain
	elif dis == 'uniform':
		num = numpy.random.uniform(para_0, para_1) * domain
	elif dis == 'zipf':
		num = numpy.random.zipf(para_0) * domain
	elif dis == 'gauss':
		num = numpy.random.normal(para_0, para_1) * domain
	return num


def workGen(d):
	filename = 'workload' + str(d)
	fwork = open(filename, 'w')
	for i in range(lines):
		t = numpy.random.random()
		if t < Ppq:	# point query
			num = int(numberGen(pq_dis, pq_para_0, pq_para_1, dataDomain))
			num_1 = num + 1
			fwork.write('p=s('+ col_name + ',' + str(num) + ',' + str(num_1) + ')\n')
		elif t < (Ppq + Prq):	# range query
			end = int(numberGen(rq_end_dis, rq_end_para_0, rq_end_para_1, dataDomain))
			sel = numberGen(rq_sel_dis, rq_sel_para_0, rq_sel_para_1, 1)
			while ((end + dataDomain * sel) > dataDomain and (end - dataDomain * sel) < 0):
				sel = numberGen(rq_sel_dis, rq_sel_para_0, rq_sel_para_1, 1)
			if (end + dataDomain * sel) <  dataDomain:
				fwork.write('p=s('+ col_name + ',' + str(end) + ',' + str(int(end + dataDomain * sel)) + ')\n')
			else:
				fwork.write('p=s('+ col_name + ',' + str(int(end - dataDomain * sel)) + ',' + str(end) + ')\n')
		elif t < (Ppq + Prq + Pin): # insert
			i_num = int(numberGen(in_dis, in_para_0, in_para_1, dataDomain))
			fwork.write('i('+ tbl_name + ',' + str(i_num))
			for j in range(columnNum - 1):
				fwork.write(',1')	# other columns
			fwork.write(')\n')
		elif t < (Ppq + Prq + Pin + Pup): # update
			old = int(numberGen(up_dis, up_para_0, up_para_1, dataDomain))
			new = int(numberGen(up_dis, up_para_0, up_para_1, dataDomain))
			fwork.write('u('+ col_name + ',' + str(old) + ',' + str(new) + ')\n')
		else: # delte
			d_num = int(numberGen(de_dis, de_para_0, de_para_1, dataDomain))
			fwork.write('d('+ col_name + ',' + str(d_num) + ')\n')
	fwork.close()


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


def readPara(dis, option_prefix, ):
	global pq_para_0
	global pq_para_1
	global rq_sel_para_0
	global rq_sel_para_1
	global rq_end_para_0
	global rq_end_para_1
	global in_para_0
	global in_para_1
	global up_para_0
	global up_para_1
	global de_para_0
	global de_para_1
	para_0 = 0
	para_1 = 0
	# read parameter according the distribution and prefix
	if dis == 'beta':
		para_0 = readFloat(option_prefix + 'beta_a', para_0)
		para_1 = readFloat(option_prefix + 'beta_b', para_1)
	elif dis == 'uniform':
		para_0 = readInt(option_prefix + 'uniform_low', para_0)
		para_1 = raedInt(option_prefix + 'uniform_low', para_1)
	elif dis == 'zipf':
		para_0 = readFloat(option_prefix + 'zipf_s', para_0)
	elif dis == 'gauss':
		para_0 = readFloat(option_prefix + 'gauss_mean', para_0)
		para_1 = readFloat(option_prefix + 'gauss_devia', para_1)
	# assign parameter
	if option_prefix == 'pq_':
		pq_para_0 = para_0
		pq_para_1 = para_1
	elif option_prefix == 'rq_sel_':
		rq_sel_para_0 = para_0
		rq_sel_para_1 = para_1
	elif option_prefix == 'rq_end_':
		rq_end_para_0 = para_0
		rq_end_para_1 = para_1
	elif option_prefix == 'in_':
		in_para_0 = para_0
		in_para_1 = para_1
	elif option_prefix == 'up_':
		up_para_0 = para_0
		up_para_1 = para_1
	elif option_prefix == 'de_':
		de_para_0 = para_0
		de_para_1 = para_1


def readDis(option):
	dis = config.get(configSection, option)
	if dis is None:
		print 'empty value for ' + option + 'defualt to uniform'
		dis = 'uniform'
	return dis


def readConfig():
	global lines 
	global Ppq
	global Prq
	global Pin
	global Pup
	global Pde
	global pq_dis
	global rq_sel_dis
	global rq_end_dis
	global in_dis
	global up_dis
	global de_dis
	lines = readInt('lines', lines)
	Ppq = readFloat('pq_percentage', Ppq)
	Prq = readFloat('rq_percentage', Prq)
	Pin = readFloat('in_percentage', Pin)
	Pup = readFloat('up_percentage', Pup)
	Pde = readFloat('de_percentage', Pde)
	pq_dis = readDis('pq_dis')
	rq_sel_dis = readDis('rq_sel_dis')
	rq_end_dis = readDis('rq_end_dis')
	in_dis = readDis('in_dis')	
	up_dis = readDis('up_dis')
	de_dis = readDis('de_dis')
	readPara(pq_dis, 'pq_')
	readPara(rq_sel_dis, 'rq_sel_')
	readPara(rq_end_dis, 'rq_end_')
	readPara(in_dis, 'in_')
	readPara(up_dis, 'up_')
	readPara(de_dis, 'de_')


if __name__ == '__main__':
	config.read('gen.cfg')
	columnNum = readInt('col_num', columnNum)
	configSection = 'workload'
	readConfig()
	workGen(lines)
