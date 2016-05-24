# args: [type,workPath,pivots,sizes,size,c_pram]
from scipy.stats import norm
from scipy.stats import uniform
from scipy.stats import zipf
import numpy as np
import sys
import random

# Pram Define
# Tpye define
# ["norm", mean, devia]
# ["uniform", low, high]
# ["zipf",zipf_s]

def myuniform(num,low,high):
    return num/(high - low)

# calculate the cdf
def normcdfArr(arr1,arr2,mean,devia):
    s = 0
    for i in range(0,len(arr1)):
        prb = abs(norm.cdf(arr2[i], mean, devia) - norm.cdf(arr1[i], mean, devia))
        s = s + prb
    return s

def normcdf(arr, mean, devia):
    s = 0
    for i in range(0,len(arr)):
        prb = 1 - norm.cdf(arr[i], mean, devia)
        s = s + prb
    return s

def uniformcdfArr(arr1,arr2,low,high):
    s = 0
    for i in range(0,len(arr1)):
        #prb = (uniform.cdf(arr2[i], low, high)) - (uniform.cdf(arr1[i], low, high))
        prb = abs((myuniform(arr2[i], low, high)) - (myuniform(arr1[i], low, high)))
        s = s + prb
        #print(sum)
    return s

def uniformcdf(arr, low, high):
    s = 0
    for i in range(0,len(arr)):
        #prb = (1 - uniform.cdf(num, low, high)) * arr[i]
        prb = 1 - myuniform(arr[i], low, high)
        s = s + prb
        #print(sum)
    return s

def zipfcdfArr(arr1,arr2,zipf_s):
    s = 0
    for i in range(0,len(arr1)):
        prb = abs(zipf.cdf(arr2[i],zipf_s) - zipf.cdf(arr1[i], zipf_s))
        s = s + prb
    return s

def zipfcdf(arr, zipf_s):
    s = 0
    for i in range(0,len(arr)):
        prb = 1 - zipf.cdf(arr[i],zipf_s)
        s = s + prb
    return s   

def sortedw_pq(lgs_cost, pq_N):
    return (lgs_cost * pq_N)

def sorted_rq(type, lgs_cost, sr_cost, rw_cost, sw_cost, arr1, arr2, size):
    dist = type[0]
    if dist == "norm":
        rq_r = 2 * lgs_cost * len(arr1) + sr_cost * normcdfArr(arr1,arr2,type[1],type[2]) * size
    elif dist == "uniform":
        #print("uniform")
        #print(uniformcdfArr(arr1,arr2,type[1],type[2]))
        rq_r = 2 * lgs_cost * len(arr1) + sr_cost * uniformcdfArr(arr1,arr2,type[1],type[2]) * size
    else:
        rq_r = 2 * lgs_cost * len(arr1) + sr_cost * zipfcdfArr(arr1,arr2,type[1]) * size
    return rq_r

def sorted_up(type, lgs_cost, sr_cost, rw_cost, sw_cost, arr1, arr2, size):
    dist = type[0]
    prob_sum = 0
    if dist == "norm":
        prob_sum = normcdfArr(arr1,arr2,type[1],type[2])
    elif dist == "uniform":
        prob_sum = uniformcdfArr(arr1,arr2,type[1],type[2])
    else:
        prob_sum = zipfcdfArr(arr1,arr2,type[1])
    up_r = 2 * lgs_cost * len(arr1) + sr_cost * prob_sum * size
    up_w = rw_cost * len(arr1) + sw_cost * prob_sum * size
    return [up_r,up_w]

def sorted_di(type, lgs_cost, sr_cost, rw_cost, sw_cost, arr, size):
    dist = type[0]
    prob_sum = 0
    if dist == "norm":
        prob_sum = normcdf(arr, type[1], type[2])
    elif dist == "uniform":
        #print(uniformcdf(interval,arr,type[1],type[2]))
        prob_sum = uniformcdf(arr,type[1],type[2])
    else:
        prob_sum = zipfcdf(arr,type[1])
    di_r = lgs_cost * len(arr) + sr_cost * prob_sum * size
    di_w = rw_cost * len(arr) + sw_cost * prob_sum * size
    return [di_r,di_w]


# lgs_cost is different from lgs_cost for sorted order
def which_partition(alist,item):
    first = 0
    last = len(alist) - 1
    found = False
    loc = 0
    while first <= last and not found:
        midpoint = (first + last) //2
        if item <= alist[midpoint] and 0 == midpoint:
            loc = midpoint
            found = True
        elif item <= alist[midpoint] and item > alist[midpoint-1]:
            loc = midpoint
            found = True
        elif item > alist[midpoint] and item <= alist[midpoint+1]:
            loc = midpoint + 1
            found = True
        else:
            if item > alist[midpoint]:
                first = midpoint
            else:
                last = midpoint
    return loc


def partition_pq(lgs_cost, sr_cost, arr, pivots, sizes):
    s = 0
    for i in range(0,len(arr)):
        loc = which_partition(pivots,arr[i])
        s = s + sizes[loc]
    read = lgs_cost * len(arr) + sr_cost * s
    return read

def partition_rq(lgs_cost, sr_cost, arr1, arr2, pivots, sizes):
    read = 0
    s = 0
    lgs = 0
    for i in range(0, len(arr1)):
        lgs = 2 * lgs_cost + lgs
        loc1 = int(which_partition(pivots,arr1[i]))
        loc2 = int(which_partition(pivots,arr2[i]))
        for j in range(loc1, loc2+1):
            s = s + sizes[j]
    read = lgs + sr_cost * s
    return read

def partition_up(lgs_cost, sr_cost, rr_cost, rw_cost, arr1, arr2, pivots, sizes):
    read = 0
    write = 0
    for i in range(0, len(arr1)):
        loc1 = which_partition(pivots,arr1[i])
        loc2 = which_partition(pivots,arr2[i])
        read = read + 2 * lgs_cost + sr_cost * sizes[loc1] / 2 + rr_cost * abs(loc2 - loc1)
        write = write + rw_cost * (abs(loc2 - loc1) + 1)
    return [read, write]


def partition_di(lgs_cost, sr_cost, rr_cost, rw_cost, arr, pivots, sizes):
    read = 0
    write = 0
    loc2 = len(sizes) - 1
    for i in range(0, len(arr)):
        loc1 = which_partition(pivots,arr[i])
        read = read + lgs_cost + sr_cost * sizes[loc1] / 2 + rr_cost * (loc2 - loc1)
        write = write + rw_cost * (loc2 - loc1)
    return [read, write]



# cost for stored order
def stored_cost(sr_cost, rw_cost, pq_N, rq_N, in_N, de_N, up_N, size):
    pq_r = sr_cost * (size/2) * pq_N
    rq_r = sr_cost * size * rq_N
    in_w = rw_cost * in_N
    de_r = sr_cost * (size/2) * de_N
    de_w = rw_cost * de_N
    up_r = sr_cost * (size/2) * up_N
    up_w = rw_cost * up_N
    read = pq_r + rq_r + de_r + up_r
    write = in_w + de_w + up_w
    return [int(read),int(write)]

# cost for sorted order
def sorted_cost(type, lgs_cost, sr_cost, rw_cost, sw_cost, pq_N, arr_rq1, arr_rq2, arr_up1, arr_up2, arr_in, arr_de, size):
    pq = sortedw_pq(lgs_cost, pq_N)
    rq = sorted_rq(type, lgs_cost, sr_cost, rw_cost, sw_cost, arr_rq1, arr_rq2, size)
    up = sorted_up(type, lgs_cost, sr_cost, rw_cost, sw_cost, arr_up1, arr_up2, size)
    ins = sorted_di(type, lgs_cost, sr_cost, rw_cost, sw_cost, arr_in, size)
    de = sorted_di(type, lgs_cost, sr_cost, rw_cost, sw_cost, arr_de, size)
    read = pq + rq + up[0] + ins[0] + de[0]
    write = up[1] + ins[1] + de[1]
    return [int(read),int(write)]

# cost for partition
def partition_cost(lgs_cost, sr_cost, rr_cost, rw_cost, sw_cost, pq_arr, arr_rq1, arr_rq2, arr_up1, arr_up2, arr_in, arr_de, pivots, sizes):
    pq = partition_pq(lgs_cost, sr_cost, pq_arr, pivots, sizes)
    rq = partition_rq(lgs_cost, sr_cost, arr_rq1, arr_rq2, pivots, sizes)
    up = partition_up(lgs_cost, sr_cost, rr_cost, rw_cost, arr_up1, arr_up2, pivots, sizes)
    ins = partition_di(lgs_cost, sr_cost, rr_cost, rw_cost, arr_in, pivots, sizes)    
    de = partition_di(lgs_cost, sr_cost, rr_cost, rw_cost, arr_de, pivots, sizes)
    read = pq + rq + up[0] + ins[0] + de[0]
    write = up[1] + ins[1] + de[1]
    return [int(read),int(write)]


# args: [type,workPath,pivots,sizes,size]
# Parse args





type = sys.argv[1].split(',')
type[1] = int(type[1])
if type[0] != "zipf":
    type[2] = int(type[2])
workPath = sys.argv[2]
pivots = sys.argv[3].split(',')
pivots = [int(x) for x in pivots]
sizes = sys.argv[4].split(',')
sizes = [int(x) for x in sizes]
size = int(sys.argv[5])
c_pram = sys.argv[6].split(',')
c_pram = [float(x) for x in c_pram]



sr_cost = c_pram[0]
rr_cost = c_pram[1]
sw_cost = c_pram[2]
rw_cost = c_pram[3]
lgs_cost1 = c_pram[4]
lgs_cost2 = c_pram[5]

f1 = open( workPath+"/pq_cost", "r")
pq_arr = np.fromfile(f1, dtype=np.uint32).tolist()
del pq_arr[0]
#pq_arr = pq_arr[200000:210000]
pq_N = len(pq_arr)


f2 = open( workPath+"/rq_cost_beg", "r")
arr_rq1 = np.fromfile(f2, dtype=np.uint32).tolist()
del arr_rq1[0]
#arr_rq1 = arr_rq1[10000:20000]
rq_N = len(arr_rq1)

f3 = open( workPath+"/rq_cost_end", "r")
arr_rq2 = np.fromfile(f3, dtype=np.uint32).tolist()
del arr_rq2[0]
#arr_rq2 = arr_rq2[10000:20000]


f4 = open( workPath+"/up_cost_beg", "r")
arr_up1 = np.fromfile(f4, dtype=np.uint32).tolist()
del arr_up1[0]
#arr_up1 = arr_up1[10000:20000]
up_N = len(arr_up1)

f5 = open( workPath+"/up_cost_end", "r")
arr_up2 = np.fromfile(f5, dtype=np.uint32).tolist()
del arr_up2[0]
#arr_up2 = arr_up2[10000:20000]


f6 = open( workPath+"/in_cost", "r")
arr_in = np.fromfile(f6, dtype=np.uint32).tolist()
del arr_in[0]
#arr_in = arr_in[200000:210000]
in_N = len(arr_in)

f7 = open( workPath+"/de_cost", "r")
arr_de = np.fromfile(f7, dtype=np.uint32).tolist()
del arr_de[0]
#arr_de = arr_de[200000:210000]
de_N = len(arr_de)



st_cost = stored_cost(sr_cost, rw_cost, pq_N, rq_N, in_N, de_N, up_N, size)
so_cost = sorted_cost(type, lgs_cost1, sr_cost, rw_cost, sw_cost, pq_N, arr_rq1, arr_rq2, arr_up1, arr_up2, arr_in, arr_de, size)
pa_cost = partition_cost(lgs_cost2, sr_cost, rr_cost, rw_cost, sw_cost, pq_arr, arr_rq1, arr_rq1, arr_up1, arr_up2, arr_in, arr_de, pivots, sizes)
#so_cost = sorted_cost(interval,type, lgs_cost1, sr_cost, rw_cost, sw_cost, 100, [1,2,4,5,6,7], [2,4,6,7,8,20], [1,2,4,5,6,7], [2,4,6,7,8,20], [222,3,3334,25,6], [3,4,6666,7,9999], 1000000)

#print(len(arr_rq1))

print(st_cost)
print(so_cost)
print(pa_cost)


#print(partition_rq(lgs_cost2, sr_cost, arr_rq1, arr_rq2, pivots, sizes))
#print(uniform.cdf(1222222, 0, 1000000000))
#print(cost_arr)





#arr0 = [1,3,6,8]
#arr1 = [2,4,8,9]
#print(sum(arr1))
#type = ["uniform",0,1000000000]
#var = sorted_up(type,1,2,3,4,arr0,arr1,100000)
#print(var)

#arr2=np.random.randint(1,100,size=20)
#arr3=np.random.randint(101,200,size=20)
#pivots = [14,34,78,145,179,200]
#sizes = [34,5,75,74,67,78]
#print(partition_pq(1, 2, arr0, pivots, sizes))

#print(partition_cost(1, 2, 3, 4, 5, arr1, arr2, arr3, arr2, arr3, arr1, arr1, pivots, sizes))







#testlist = [ 1, 2, 8, 13, 17, 19, 32, 42,79]
#print(which_partition(testlist, 1))
#print(which_partition(testlist, 3))
#print(which_partition(testlist, 8))
#print(which_partition(testlist, 12))
#print(which_partition(testlist, 15))
#print(which_partition(testlist, 18))
#print(which_partition(testlist, 23))
#print(which_partition(testlist, 34))
#print(which_partition(testlist, 42))
#print(which_partition(testlist, 65))
#print(which_partition(testlist, 79))


# 0 2 2 3 4 5 6 7 7 8 8





#print(which_partition(testlist, 13))





    

























	




