#!/usr/bin/env python

import random
#import cProfile
import bisect 
import cPickle as pickle
from operator import itemgetter
import shutil
#from multiprocessing import Pool
#tandai ini model 

print 'open file'
timelist_of_estimate = []
fd=open('timelist.pickle', "rb")
timelist_of_estimate = pickle.load(fd)
fd.close()
print 'close file'
timelist_of_estimate2 = timelist_of_estimate[::-1]
del timelist_of_estimate

print 'open file'
timelist_of_estimate = []
fd=open('timelistcontentid-newversion.pickle', "rb")
timelist_of_estimate = pickle.load(fd)
fd.close()
print 'close file'


class CDN(object):

    def __init__(self, catalog):
        self.type = 'CDN'
        self.upload_bandwidth = 0
        self.peer_list = None
        self.content_catalog = catalog
        self.peer_tracking = dict()
        self.peer_tracking_contents = dict()
        self.video_tracking_time_requested = dict()
        self.video_tracking_number_requested = dict()
        self.cdnhitcounter = 0
        self.peerhitcounter = 0
        self.peertraffic = 0
        self.cdntraffic = 0
        self.p_min = 0
        self.p_max = 0
        self.jumlah = 0
        self.cache_entries = dict()
        self.cache_size = dict()
        self.list_replica = []
        self.counter = 0
        self.first_time = {}
        self.mingguan = {}
        self.counter_2 = range(34)
        self.first = {}
        self.list_redirect = []


        
    def set_peer_list(self, peer_list):
        self.peer_list = peer_list



    def get_peer_from_id(self, id):
        #numpeer ternyata berurutan: range(100000)
        #lihat di simulator.py
        return self.peer_list[id]
        

    def get_upload_time(self,content_id):
        """
        ambil upload time
        """
        return self.content_catalog[content_id][1]

    def get_estimate(self):
        """
        get estimation video
        hasilnya -1, 1, atau 0
        """
        return timelist_of_estimate2.pop()


    def get_estimate_dict(self, time_cur, content_id):
        """
        """
        #bisect
        arr=sorted(timelist_of_estimate[content_id]) 
        #hasilnya arr berupa list of tuple
        timelist = [i[0] for i in arr]  
        upload_time = self.get_upload_time(content_id)
        selisih = abs(time_cur - upload_time)/1000 #milisecont to second
        indeks = bisect.bisect_left(timelist, selisih)
        panjang = len(arr)
        if indeks > panjang-1:
            h = arr[indeks-1][1]
            return h
        else:
            h = arr[indeks][1]
            return h


    def get_number_requested_video(self, content_id):
        """
        ambil jumlah video yng telah di request
        """
        #print self.video_tracking_number_requested
        return self.video_tracking_number_requested[content_id]

    def get_first_access(self, content_id):
        """
        get first access time
        """
        return self.first[content_id]


    def get_video_last_time_requested(self, content_id):
        """
        ambil waktu terakhir video direquest
        """
        return self.video_tracking_time_requested[content_id]['last']


    def get_first_time_requested(self, content_id):
        """
        ambil jumlah pertama kali video diakses
        """
        if self.video_tracking_number_requested[content_id] < 2:
            return 1  #positif pertama kali akses
        else:
            return -1 #negatif, sudah ada yng akses

    def hitung_pmax_min(self,time_cur):
        """
        hitung p_min utk semua video yng ada
        """
        #temp1 = []
        #hitung utk semua video yng beredar di sistem
        con = self.peer_tracking_contents.keys()
        panjang = len(con)
        time_list = [time_cur for i in xrange(panjang)]

        hasil = map(self.loop_con_pmax_min, time_list, con)
        #hasilnya berupa list dalam var hasil
        #cari mininimum
        self.p_min = min(hasil)
        self.p_max = max(hasil)
        return self.p_min, self.p_max #hasilnya berupa tuple

    def loop_con_pmax_min(self, time_cur, con):
        n_ir = self.get_number_requested_video(con)
        t_ir = self.get_video_last_time_requested(con)
        a_i = self.get_first_access(con)
        if abs(time_cur - t_ir)/1000.0 == 0:
            kanan = 0.0
        else:
            kanan = 1000*(1.0)/abs(time_cur - t_ir)
        pembagi = abs(t_ir - a_i)/1000.0
        if pembagi == 0:
            pembagi = abs(t_ir - 0)/1000.0
        kiri = (n_ir)/pembagi
        return min(kiri,kanan)



    def get_replica(self, content_id):
        """
        dapatkan jumlah replica utk sebuah video id
        """
        self.jumlah=0
        if not self.peer_tracking_contents.has_key(content_id):
            self.jumlah=0
        else:
            self.jumlah = len(self.peer_tracking_contents[content_id].keys())

        return self.jumlah


    def get_log(self):
        """
        log 
        """
        temp3 = {}
        #hitung replica utk semua video yng beredar di sistem
        con = self.peer_tracking_contents.keys()
        for i in con:
            temp3[i]=self.get_replica(i)
        r_max = max(temp3.values())
        r_min = min(temp3.values())
        if r_min == 0:
            r_min = 1
        optimum = (2*r_max*r_min)/(r_max+r_min)
        return (temp3, optimum)



    def get_content(self, peer_id, content_id, time_cur):
        """
        reply dengan content atau redirect
        """
        this_content=self.content_catalog[content_id]
        tracked_peer = self.get_peer_tracking(content_id)

        #rekam waktu video direquested
        if not self.video_tracking_time_requested.has_key(content_id):
            self.video_tracking_time_requested[content_id]={}
            self.video_tracking_time_requested[content_id]['new']=time_cur
        
        self.video_tracking_time_requested[content_id]['last']=self.video_tracking_time_requested[content_id]['new']
        self.video_tracking_time_requested[content_id]['new']=time_cur

        if not self.first.has_key(content_id):
            self.first[content_id]=time_cur


        #results_p[p] = results_p.get(p, 0) + 1
        #rekam jumlah akses video
        self.video_tracking_number_requested[content_id] = self.video_tracking_number_requested.get(content_id,0)+1
        #print self.video_tracking_number_requested
        
        #catatan view count per minggu
        #ambil waktu pertama kali video di upload
        upload_time=this_content[1]

        #hitung selisih antara time_cur dng waktu upload video
        selisih = abs(time_cur - upload_time)/1000.0  #mili second to second

        jumlah_request = self.video_tracking_number_requested[content_id]
        if not self.mingguan.has_key(content_id):
            self.mingguan[content_id]={}
            #self.mingguan[content_id]['pertamaupload']=upload_time

        #self.mingguan[content_id]['timerequest']=time_cur
        #self.mingguan[content_id]['jumlah']=jumlah_request

        minggu,second = divmod(selisih, 7*24*60*60)
        minggu = int(minggu)
        self.mingguan[content_id][minggu]=jumlah_request

        dst='temp2'
        jumlah_replica=self.get_replica(content_id)
        self.list_redirect.append((time_cur,jumlah_replica))

        if tracked_peer:
            if (self.counter%86400)==(86400-1):
                filename = 'replica-log-redirect' + str(self.counter) + '.pickle'
                with open(filename,'wb') as fd:
                    pickle.dump(self.list_redirect,fd)
                fd.close()
                self.list_redirect=[]
                shutil.move(filename,dst)
            self.counter+=1  


            #print 'tracked_peer', tracked_peer , 'content_id', content_id
            #print 'p'
            self.peerhitcounter += 1
            return [None, tracked_peer]
            
        #jika content tidak ada di peer (None) maka kembalikan langsung dari CDN.
        else:
            #print 'cdnhit', self.cdnhitcounter
            self.cdnhitcounter += 1

            #check cache_size
            jumlah = sum(self.cache_size.values())
            ukuran_video_baru = this_content[2]
            #print jumlah+ukuran_video_baru
            return [this_content, None]


    def loop_con_cdn(self, time_cur, con):
        n_ir = self.get_number_requested_video(con)
        t_ir = self.get_video_last_time_requested(con)
        a_i = self.get_first_access(con)
        if (time_cur - t_ir)/1000.0 == 0:
            kanan = 0.0
        else:
            kanan = 1000*(1.0)/abs(time_cur - t_ir)
        pembagi = abs(t_ir - a_i)/1000.0
        if pembagi == 0:
            pembagi = abs(t_ir - 0)/1000.0
        kiri = (n_ir)/pembagi
        P_i = min(kiri,kanan)
        return P_i
    

    def receive_report_from_peer(self, peer_id, content_id, type, time_cur, yng_request, log_replica=None):
        """
        report to cdn (model baru)
        type = CACHE, REMOVE_CACHE, UPLOADING, IDLE
        
        peer_tracking adalah dictionary dengan peer_id sebagai key, dan tiap value adalah dictionary dengan key peer_id, status, dan cache_content
        """

        dst = 'temp2'

        #init dict
        if not self.peer_tracking_contents.has_key(content_id):
            self.peer_tracking_contents[content_id] = dict()

        #if not self.log_replica.has_key(content_id):
        #    self.log_replica[time_cur]= dict()
        #    self.log_replica[time_cur] = {'video-id': 'NONE', 'self-id':'NONE',  't_di_cache':'NONE',  't_di_remove':'NONE', 't_di_access':[]}
            #self.log_replica[content_id] = dict()
            #self.log_replica[content_id] = {'self-id':'NONE',  't_di_cache':'NONE',  't_di_remove':'NONE', 't_di_access':[]}

        #if self.peer_list[peer_id].upload_state:
            #print 'PIDD', self.peer_list[peer_id].upload_state
        #if type == 'UPLOADING':
        #    self.log_replica[time_cur]['video-id'] = content_id
        #    self.log_replica[time_cur]['self-id'] = yng_request
        #    self.log_replica[time_cur]['t_di_access'].append(  (time_cur, peer_id)  ) 

        if type == 'CACHE':
            self.peer_tracking_contents[content_id][peer_id]=peer_id
            #print self.peer_tracking_contents
            #self.log_replica[time_cur]['video-id'] = content_id
            #self.log_replica[time_cur]['self-id']=peer_id
            #self.log_replica[time_cur]['t_di_cache']=time_cur
            

        if type == 'REMOVE_CACHE':
            del self.peer_tracking_contents[content_id][peer_id]            
            self.list_replica.append(log_replica)
            
            if (self.counter%86400)==(86400-1):
                filename = 'temp/replica-log-' + str(self.counter) + '.pickle'
                with open(filename,'wb') as fd:
                    pickle.dump(self.list_replica,fd)
                fd.close()
                shutil.move(filename,dst)
                self.list_replica=[]
            self.counter+=1  


    def store_last_state(self):
        dst = 'temp2'

        print 'data store last'
        #sisa dari 'REMOVE_CACHE', yng masih didalam memori belum ditulis.
        filename = 'temp/replica-log-' + str(self.counter) + '.pickle'
        with open(filename,'wb') as fd:
            pickle.dump(self.list_replica,fd)
        fd.close()
        shutil.move(filename,dst)


        #log last states
        list_replica_local=[]
        for i in self.peer_list:
            for k,v in i.log_replica.iteritems():
                list_replica_local.append(v)

            if len(list_replica_local)>50000:
                filename = 'temp/replica-log-akhir-' + str(self.counter) + '.pickle'
                with open(filename,'wb') as fd:
                    pickle.dump(list_replica_local,fd)
                fd.close()
                shutil.move(filename,dst)
                list_replica_local=[]
                self.counter+=1

        filename = 'temp/replica-log-akhir-' + str(self.counter) + '.pickle'
        with open(filename,'wb') as fd:
            pickle.dump(list_replica_local,fd)
        fd.close()
        shutil.move(filename,dst)
        list_replica_local=[]
        self.counter+=1


        
    def get_peer_tracking(self, content_id):
        """
        CDN harus punya catatan peer mana saja yng sudah punya content
        content request yng ada di peer di-redirect ke peer yng sudah punya content
        """
        #if not self.peer_tracking_contents.has_key(content_id):
        #    return None
        #else:
        #    if self.peer_tracking_contents[content_id]:
        #        #print 'True', self.peer_tracking_contents
        #        daftar_peer_idle = []
        #        for key, status in self.peer_tracking_contents[content_id].iteritems():
        #            if status == 'IDLE':
        #                daftar_peer_idle.append(key)
        #        #print content_id, daftar_peer_idle
        #        if daftar_peer_idle:
        #            k = random.choice(daftar_peer_idle)
        #            #print k
        #            return self.get_peer_from_id(k)
        #    else:
        #        print 'False'
        #        return None

        daftar_peer_idle = []

        
        if content_id in self.peer_tracking_contents:

            #kunci=self.peer_tracking_contents[content_id].keys()
            #for p in kunci:
            #    print '--->', self.peer_list[p].upload_state
            daftar_peer_idle = [ pid for pid in self.peer_tracking_contents[content_id].keys() if self.peer_list[pid].upload_state == 'IDLE']
            if daftar_peer_idle:
                #print daftar_peer_idle
                k=random.choice(daftar_peer_idle)
                return self.get_peer_from_id(k)
            else:
                return None

        else:
            return None
        # for k,v in self.peer_tracking.iteritems():
        #     if content_id in v['cache_content'] and v['status']=='IDLE':
        #         daftar_peer_idle.append(k)
            
        #     #kalau ada hasil baik banyak element atau cuma satu element:
        # if daftar_peer_idle:  
        #     k = random.choice(daftar_peer_idle)
        #     return self.get_peer_from_id(k)
        # else:
        #     return None
                    
                    
                
