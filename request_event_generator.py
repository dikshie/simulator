#!/usr/bin/env python

from datetime import datetime
import time
import random
import math
import cPickle as pickle 
import peer
import event
import cdn

hari = 86400
skala = 365*24*3600 #364 hari
multiple_of = range(100000)
interval = (24*3600)
counter = 0
TIMELINE_LENGTH= 24*3600
numpeer = 10000
cache_size = 1000
upload_bw = 1000
download_bw = 1000
if __name__ == '__main__':

	#open request time 
	print 'baca request time'
	with open('requestime.pickle', 'rb') as handle:
		catalog = pickle.load(handle)
	handle.close()
	print 'selesai request time'

	#open videochoice
	print 'loading video choice'
	with open('pilihan.pickle', 'rb') as handle:
		videochoice = pickle.load(handle)
	handle.close()
	print 'finished loading catalog'

        panjang = len(videochoice)

	#generate peers
	this_cdn = cdn.CDN(catalog)
	peer_list = []
	for p in range(numpeer):
		np = peer.Peer(p,this_cdn, cache_size, upload_bw, download_bw)
		peer_list.append(np)
	this_cdn.set_peer_list(peer_list)

	file_list = [ 'request_events-1t-'+str(TIMELINE_LENGTH*(i+1))+'.pickle' for i in range(skala/TIMELINE_LENGTH) ]
	print file_list
        event_list = event.Timeline()
	for i in range(panjang):
		content_id = videochoice[i]
		time_cur = catalog[i]
		actor = random.choice(peer_list)
                #print content_id, time_cur, actor

		ev = event.Event(event.REQUEST, time_cur, actor, actor.request_to_cdn, [content_id, time_cur])
		event_list.append_event(ev)

		if (counter%TIMELINE_LENGTH)==TIMELINE_LENGTH-1:
                	filename='request_events-1t-'+str(counter+1)+'.pickle'
                	file_list.append(filename)
                	with open(filename, 'wb') as f:
                		event_list.marshall()
                		pickle.dump(event_list, f)
			event_list.timeline = []
			print counter/hari, str(datetime.now())
		counter+=1

	# dump the last timelines
	filename='request_events-1t-'+str(counter)+'.pickle'
	file_list.append(filename)
	with open(filename, 'wb') as f:
		event_list.marshall()
		pickle.dump(event_list, f)
	event_list.timeline = []
	print counter/hari, str(datetime.now())
	print 'total requests: ', counter   

""" 
	#mulai generate time dalam poisson
	for ev in range(skala):
		tx = random.expovariate(expected)
		time_cur = time_cur + (tx*expected)
		actor = random.choice(peer_list)
		
		#content_id di assigned = 0
		content_id = videochoice[counter]

		#event request
		ev = event.Event(event.REQUEST, time_cur, actor, actor.request_to_cdn, [content_id, time_cur])
		event_list.append_event(ev)
		
		#segmentasi hasil tiap 24 jam
		if (counter%TIMELINE_LENGTH)==TIMELINE_LENGTH-1:
			filename='request_events-1t-'+str(counter+1)+'.pickle'
			file_list.append(filename)
			with open(filename, 'wb') as f:
				event_list.marshall()
				pickle.dump(event_list, f)
			event_list.timeline = []
			print counter/hari, str(datetime.now())

		counter+=1

	# dump the last timelines
	filename='request_events-1t-'+str(counter)+'.pickle'
	file_list.append(filename)
	with open(filename, 'wb') as f:
		event_list.marshall()
		pickle.dump(event_list, f)
	event_list.timeline = []
	print counter/hari, str(datetime.now())
	print 'total requests: ', counter
"""
