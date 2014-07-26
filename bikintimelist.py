#!/usr/bin/env python

import cPickle as pickle

fd=open('timelistcontentidreplica.pickle','rb')
data=pickle.load(fd)
fd.close()

timelist = []
for k,v in data.iteritems():
    for j in v:
        timelist.append(j)

timelist = sorted(timelist)
print len(timelist)
print type(timelist)
print timelist[0]

temp=[]
for i in range(len(timelist)): 
    temp.append(timelist[i][1])

print len(temp), type(temp)
print temp[0:10]

fd=open('timelist.pickle','wb')
pickle.dump(temp,fd,-1)
fd.close()
