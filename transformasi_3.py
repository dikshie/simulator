#!/usr/bin/env python
import cPickle as pickle

print 'open'
filename='timelistcontentid-sortversion.pickle'
with open(filename,'rb') as fd:
    data=pickle.load(fd)
fd.close()
print 'close'

kamus = {}
print type(data)
for k,v in data.iteritems():
    for i in v:
        if i:
            if not kamus.has_key(k):
                kamus[k]=[]
            kamus[k].append(i)

print 'open'
filename='timelistcontentid-newversion.pickle'
with open(filename,'wb') as fd:
    pickle.dump(kamus,fd)
fd.close()
print 'close'
