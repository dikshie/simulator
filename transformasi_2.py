#!/usr/bin/env python

import cPickle as pickle

print 'open'
file='timelistcontentidreplica.pickle'
with open(file,'rb') as fs:
    kamus = pickle.load(fs)
fs.close()
print 'close'

kunci = sorted(kamus.keys())
#hasilnya berupa list kunci

print 'iteration'
kamus2 = {}
for x in kunci:
    arr = kamus[x]
    list1=[i for i in arr if i[1]==-1]
    list2=[i for i in arr if i[1]==0]
    list3=[i for i in arr if i[1]==1]
    if list1:
        t_a_be = list1[-1]
    else:
        t_a_be = ()

    if list2:
        t_a_at = list2[0]
        t_b_at = list2[-1]
    else:
        t_a_at = ()
        t_b_at = ()

    if list3:
        t_a_af = list3[0]
    else:
        t_a_af = ()

    if not kamus2.has_key(x):
        kamus2[x]=[]
        kamus2[x]=[t_a_be, t_a_at,  t_b_at , t_a_af]


print 'open'
filename='timelistcontentid-sortversion.pickle'
with open(filename,'wb') as fd:
    pickle.dump(kamus2,fd)
fd.close()
print 'close'

    

