#!/usr/bin/env python

import random
from datetime import datetime
import copy
import numpy as np
import cPickle as pickle

#import choice
#import weightedpick

skala=364*24*3600
multiple_of = range(100000)
interval = (24*3600) 
counter = 0
magic_number_1 = 364
video_id = range(10000)
dict_for_resume={}
results_p = {}
TIMELINE_LENGTH = 1*3600

#ujung=364*10000
#step=364
#temp = np.arange(0,ujung,step) #array utk posisi 
#tambah = np.arange(1,2,1)

cview=0
rview=0
content_id=0

#if __name__ == '__main__':
#baca file dari flat-table utk pdf
print 'loading catalog pdf file'
pdfl = np.loadtxt('pdfmulti.dat')
print 'finish loading catalog file'

print 'loading catalog cdf file'
cdfl = np.loadtxt('cdfmulti.dat')
print 'finish loading catalog file'

print 'loading catalog terminus file'
terminusl = np.loadtxt('terminusmulti.dat')
print 'finish loading catalog file'

#jumlahvideo = 10000

"""
print 'generating uploads'
upload_time = range(jumlahvideo)
time_cur = -skala/2
for i in range(jumlahvideo):
    tx = random.expovariate(1)
    time_cur = int(time_cur + tx*3153)
    upload_time[i] =  time_cur
"""


print 'baca file uploadtime'
fd=open('uploadtime.pickle','rb')
upload_time = pickle.load(fd)
fd.close()


print 'baca file request time'
fd=open('requestime.pickle','rb')
time_request = pickle.load(fd)
fd.close()


def weightedpick(d):
    r = random.uniform(0, sum(d.itervalues()))
    s = 0.0
    for k, w in d.iteritems():
        s += w
        if r < s: return k
    return k


awal=str(datetime.now())
print awal
jumlahvideo=10000
video_count = [0 for r in range(jumlahvideo)]

request_time = []
time_cur = 0
counters = 0
pilihan=[]
for j in time_request[0:3000000]:

    selection_pool = {}
    
    count_idx = 0
    for u in upload_time:
        count_idx+=1
        time_diff = j - u # request time -  upload time
        if time_diff < 0:
            continue
        idx=count_idx -1 
        day_diff = int(time_diff/(24*3600*1000))
        if day_diff > 363:
            day_diff = 363 
        view_count_t = cdfl[day_diff][idx] * terminusl[0][idx]
        view_rate_t = pdfl[day_diff][idx] * terminusl[0][idx]
        new_val = 7*view_count_t+3*view_rate_t
        selection_pool[idx] = new_val
    try:
        p = weightedpick(selection_pool)
        video_count[p] += 1
        #print p, len(selection_pool), counters
        if (counters%86400)==86400-1:
            print counters, str(datetime.now())
        pilihan.append(p)
        counters+=1
    except:
        print p, selection_pool

print 'akhir save pilihan'
fd=open('pilihan_1.pickle','wb')
pickle.dump(pilihan,fd,-1)
fd.close()



"""        
for r in xrange(skala):

    tx = random.expovariate(1)
    time_cur = time_cur + (tx*3153)/100
    rv = [time_cur, -1 ]    

    selection_pool = {}
    # hanya proses video dengan upload time > request time
    count_idx = 0
    for u in upload_time:

	count_idx += 1

        time_diff = rv[0] - u # request time -  upload time
        if time_diff < 0:
            continue

	idx = count_idx - 1

        day_diff = int(time_diff/(24*3600))

	if day_diff > 363:
	    day_diff = 363

        view_count_t = cdfl[day_diff][idx] * terminusl[0][idx]
        view_rate_t = pdfl[day_diff][idx] * terminusl[0][idx]

        new_val = 7*view_count_t+3*view_rate_t
        selection_pool[idx] = new_val

    try:
        p = weightedpick(selection_pool)

        rv[1] = p
        video_count[p] += 1
        print rv[0], rv[1], len(selection_pool), counters
        if (counters%86400)==86400-1:
            print counters, str(datetime.now())
        request_time.append(rv)
        counters+=1
    except:
        print r[0], selection_pool
        #:raw_input()
""" 



"""
print 'akhir'
#save upload time video
fd=open('upload_time_video.pickle','wb')
pickle.dump(upload_time,fd,-1)
fd.close()
"""
"""
#save request time dan pilihan video 
fd=open('daftarvideochoicereplica.pickle','wb')
pickle.dump(request_time,fd,-1)
fd.close()
"""

"""
print results_p
print awal, str(datetime.now())
pickle.dump(pilihan,fd,-1)
fd.close()

"""
