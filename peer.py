#!/usr/bin/env python

import random 
import event
import math
#import cProfile
from collections import OrderedDict
from operator import itemgetter
#tandai ini model

# class properties:  
# cache: cache entry (file_id, size, start, stop, download count), expire, request, upload

class Peer(object):
    
    #method __init__
    def __init__(self, id, cdn, size, up_bw, dn_bw):
        self.cache_entries=dict() #// file_id, size/len, expire
        self.size=size
        self.cdn = cdn
        self.id = id
        self.up_bw = up_bw
        self.dn_bw = dn_bw
        self.type = 'Peer'
        self.upload_bandwidth = 0
        self.upload_state = 'IDLE'
        self.cache_size=dict()
        self.log_replica = dict()
             
    def __repr__(self):
        return self.id
        
    def __str__(self):
        return str(self.id)
    
    def request_to_cdn(self, content_id, time_cur):
        """
        request ke CDN
        CDN reply dengan content atau redirect ke peer lain
        buat event untuk cache content dari CDN jika CDN reply dengan content
        buat event untuk request_to_peer jika CDN redirect ke peer lain
        """
        
                
        # cek cache entries dulu utk mencegah kasus peer yng sama me-request content id yng sama
        #
        # jika cache_entries[content_id] sudah ada return []
        if self.cache_entries.has_key(content_id):
            return [],[]
        
        other_reply = self.cdn.get_content(self.id, content_id, time_cur)
        if other_reply[0]:
            content = other_reply[0]
       
                        
            #pada prinsipnya cache_event ini adalah mendownload kemudian menyimpan 
            #cache_content event, download duration, actor, actor action, action parameters
            # tandai bahwa cache entry akan di-isi content sebenarnya
            #self.cache_entries[content_id] = 1
            #contoh: {0: [0, 5.0775931306168065, 499.0, 3600.0, week_peak]}
            durasi = content[2]*8/(self.dn_bw/1000.0) * 1000 #mili second to second 
            cache_event_cdn = event.Event(event.CACHE_CONTENT, time_cur+durasi, self, self.cache_content_cdn, [content_id, content, time_cur+durasi])

            return [cache_event_cdn],[]
            
        else:
            #event request to peer
            request_to_peer_event = event.Event(event.REQUEST, time_cur, self, self.request_to_peer, [content_id, other_reply[1], time_cur])
            return [request_to_peer_event],[]
            
        
    def request_to_peer(self, content_id, other, time_cur):
        """
        request ke peer lain (other)
        """

        yng_request = self.id
        #ambil content ke peer, hasilnya content 
        content = other.get_content(content_id,time_cur,yng_request) 
        #peer_origin = other
        
        #ekstrak nilai panjang file
        #print content
        #contoh: {0: [0, 5.0775931306168065, 499.0, 3600.0, week]}
        durasi_up = (content[2]*8)/(self.up_bw/1000.0) * 1000 #  second to milisecond

        
        #event utk upload done
        upload_done_event = event.Event(event.UPLOAD_DONE, time_cur+durasi_up, other, other.upload_done, [content_id, other.id, 'BUSY', time_cur, yng_request])
        #print upload_done_event
        #print 'hhh', content
        #new_remove_content_event, old_remove_content_event = other.change_content_expiry_time(content, time_cur)
              
        #hitung durasi
        #time_duration = (content[1]*8)/(other.up_bw/1000.0)
        durasi_down = (content[2]*8)/(self.dn_bw/1000.0) * 1000 # second to milisecond
        # tandai bahwa cache entry akan di-isi content sebenarnya
        #self.cache_entries[content_id] = 1
        
        # bikin cache_content event:
        cache_event = event.Event(event.CACHE_CONTENT, time_cur+durasi_down, self, self.cache_content, [content_id, content, time_cur+durasi_down])
        
        #kembalikan event
        #return [cache_event, upload_done_event, new_remove_content_event] , [old_remove_content_event]
        return [cache_event,upload_done_event],[]
        
        
        
    def upload_done(self, content_id, peer_id, type, time_cur, yng_request):
        """
        Method utk report upload done
        """
        self.upload_state = 'IDLE'
        #print 'masuk'
        self.cdn.receive_report_from_peer(peer_id, content_id, 'IDLE', time_cur, yng_request)
        return [],[]
        
        
            
    def get_content(self, content_id, time_cur, yng_request):
        """
        ambil content dari peer
        """
        #peer_uploader = self.id
        #upload ke peer lalu lapor state jadi UPLOADING
        self.upload_state = 'UPLOADING'
        self.log_replica[content_id]['t_di_access'].append( (time_cur,yng_request) )
        self.cdn.receive_report_from_peer(self.id, content_id, 'UPLOADING', time_cur, yng_request)
        #print 'PEER',self.id, 'selfcache' , self.cache_entries[content_id]
        return self.cache_entries[content_id]
        
    
    #def clean_up_cache(self):
    #    for k,v in self.log_replica.iteritems():
            #k = key = content_id
            #lapor remove ke CDN
            #self.cdn.receive_report_from_peer(self.id, k, 'REMOVE_CACHE', 0, self.id, v)
            #
            #temp_list.append(v)
    #       self.cdn.receive_report_from_peer(self.id, k, 'REMOVE_CACHE_AKHIR', 0, self.id, v)

    def loop_con_dicache(self, time_cur, con, P_max, P_min):
        """
        function for finding utility function
        """
        p_before =0.15 
        p_at = 0.47
        p_after = 0.38

        n_ir = float(self.cdn.get_number_requested_video(con))
        t_ir = float(self.cdn.get_video_last_time_requested(con))
        a_i = self.cdn.get_first_access(con)
        if abs(time_cur - t_ir)/1000.0 == 0:
            kanan = 0.0
        else:
            kanan = 1000*(1.0)/abs(time_cur - t_ir)
        pembagi = abs(t_ir - a_i)/1000.0
        if pembagi == 0:
            pembagi = abs(t_ir - 0)/1000.0
        kiri = (n_ir)/pembagi
        P_i = min(kiri,kanan)
        hasilss = self.cdn.get_estimate_dict(time_cur,con)
        if hasilss == -1:
            bobot = p_before
        elif hasilss == 0:
            bobot = p_at
        else:
            bobot = p_after
        #ambil catatan jumlah replica tiap con dari cdn
        r = self.cdn.get_replica(con)
        r = float(r)
        if P_i == 0:
            P_i = 0.1
        if P_min == 0:
            P_min = 0.1
        if P_max == 0:
            P_max = 0.1
        if r == 0:
            utility = 0
        else:
            utility = (abs(math.log(P_i)) - abs(math.log(P_min))) * (abs(math.log(P_max))-abs(math.log(P_i)))/(r*r)
        utility = abs(utility) + bobot

        return utility #as list/array



    def cache_content_cdn(self, content_id, content, time_cur):
        """
        cache the content in the peer
        and report to CDN
        karena membutuhkan cdn class dng method report_to_cdn
        """
        size_video_baru = content[2]
        jumlah=sum(self.cache_size.values())
        content_baru=[ content[0], content[1], content[2], time_cur ]

        #posisi disini diganti dng estimasi t
        #kalau belum pernah diakses akan estimasi dari t
        #kalau sudah pernah diakses akan estimasi dar viewrate
        #posisi_minggu_video_baru = content[4]
        yng_request=self.id

        #cek_pertama_kali = self.cdn.get_first_time_requested(content_id)
        #0.149538787758 0.470040393021 0.380420819221
        #p_before =  0.1
        #p_at = 1.3 
        #p_after = 0.1
        p_before =0.15
        p_at = 0.47
        p_after = 0.38


        hasil = self.cdn.get_estimate()


        #check capacity
        if (jumlah+size_video_baru) <= 500: #kurang dari 500MB langsung dicache
            #langsung masuk dicache
            content_baru=[ content[0], content[1], content[2], time_cur ]
            self.cache_entries[content_id]=content_baru
            self.cache_size[content_id]=size_video_baru
            self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
            self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)


        else:
            #before
            if hasil == -1:
                n_ir = float(self.cdn.get_number_requested_video(content_id))
                t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                a_i = self.cdn.get_first_access(content_id)
                if abs(time_cur - t_ir)/1000.0 == 0:
                    kanan = 0.0
                else:
                    kanan = 1000*(1.0)/abs(time_cur - t_ir)
                pembagi = abs(t_ir - a_i)/1000.0
                if pembagi == 0:
                    pembagi = abs(t_ir - 0)/1000.0
                kiri = (n_ir)/pembagi
                P_i = min(kiri,kanan)
                #ambil catatan P_max dan P_min semua video yng ada di sistem dari cdn 
                
                p_max_min = self.cdn.hitung_pmax_min(time_cur)
                P_max = p_max_min[1]
                P_min = p_max_min[0]


                #ambil catatan jumlah replica content_id ini dari cdn
                r = self.cdn.get_replica(content_id) 
                r = float(r)
                    
                if P_i == 0:
                    P_i = 0.1
                    #hitung utility video baru
                    #utility_video_baru = abs(math.log(P_max))/r
                if P_min == 0:
                    P_min = 0.1
                if P_max == 0:
                    P_max = 0
                    
                if r == 0:
                    utility_video_baru = 0.0
                else:
                    utility_video_baru = (abs(math.log(P_i)) - abs(math.log(P_min))) * (abs(math.log(P_max))-abs(math.log(P_i)))/(r*r)
                utility_video_baru = abs(utility_video_baru) + p_before
                #hitung p utk video didalam cache
                #ambil content id yng sudah ada didalam cache
                temp_1={}
                utility=0
                con=self.cache_entries.keys()
                panjang = len(con)
                time_list = [time_cur for i in xrange(panjang)]
                p_mx = [P_max for i in xrange(panjang)]
                p_mi = [P_min for i in xrange(panjang)]
                result = map(self.loop_con_dicache, time_list, con, p_mx, p_mi)
                #ubah ke dictionary
                temp_1 = dict(zip(con,result))

                
                #cari minimum utility video didalam cache
                #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                #sorted dictionary by values (utility):
                list_sorted_utility = sorted(temp_1.items(), key=itemgetter(1))
                #hasilnya berupa list of tuple
                utility_min_video_dicache = list_sorted_utility[0][1]

                list_sorted_utility2 = list_sorted_utility[::-1]
                del list_sorted_utility
                
                #bandingkan utility video didalam cache dng video yng akan masuk
                #bila utility min dalam cache lebih kecil:
                if utility_min_video_dicache < utility_video_baru:
                    jumlah=sum(self.cache_size.values())
                    #disini secaraiterative hapus cache
                    while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                        tup = list_sorted_utility2.pop()
                        video_id_dng_u_min = tup[0]
                        video_size_dng_u_min = tup[1]
                        del self.cache_size[video_id_dng_u_min]
                        del self.cache_entries[video_id_dng_u_min]
                        self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                        self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                        del self.log_replica[video_id_dng_u_min]
                        jumlah=sum(self.cache_size.values())
                    #setelah jumlah+size_video_baru <= 500 maka
                    #cache utk video baru yng masuk
                    self.cache_entries[content_id]=content_baru
                    self.cache_size[content_id]=size_video_baru
                    self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                    self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                else:
                    pass



            #at
            elif hasil == 0:
                n_ir = float(self.cdn.get_number_requested_video(content_id))
                t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                a_i = self.cdn.get_first_access(content_id)
                if abs(time_cur - t_ir)/1000.0 == 0:
                    kanan = 0.0
                else:
                    kanan = 1000*(1.0)/abs(time_cur - t_ir)
                pembagi = abs(t_ir - a_i)/1000.0
                if pembagi == 0:
                    pembagi = abs(t_ir - 0)/1000.0
                kiri = (n_ir)/pembagi
                P_i = min(kiri,kanan)
                #ambil catatan P_max semua video yng ada di sistem dari cdn 
                p_max_min = self.cdn.hitung_pmax_min(time_cur)
                P_max = p_max_min[1]
                P_min = p_max_min[0]

                #ambil catatan jumlah replica content_id ini dari cdn
                r = self.cdn.get_replica(content_id) 
                r = float(r)
                    
                if P_i == 0:
                    P_i = 0.1
                    #hitung utility video baru
                    #utility_video_baru = abs(math.log(P_max))/r
                if P_min == 0:
                    P_min = 0.1

                if P_max == 0:
                    P_max = 0.1
                    
                if r == 0:
                    utility_video_baru = 0.0
                else:
                    utility_video_baru = (abs(math.log(P_i)) - abs(math.log(P_min))) * (abs(math.log(P_max))-abs(math.log(P_i)))/(r*r)
                utility_video_baru = abs(utility_video_baru) + p_at
                #hitung p utk video didalam cache
                #ambil content id yng sudah ada didalam cache
                temp_2={}
                utility=0
                con=self.cache_entries.keys()
                panjang = len(con)
                time_list = [time_cur for i in xrange(panjang)]
                p_mx = [P_max for i in xrange(panjang)]
                p_mi = [P_min for i in xrange(panjang)]

                result = map(self.loop_con_dicache, time_list, con, p_mx, p_mi)
                #ubah ke dictionary
                temp_2 = dict(zip(con,result))

                #cari minimum utility video didalam cache
                #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                #sorted dictionary by values (utility):
                list_sorted_utility = sorted(temp_2.items(), key=itemgetter(1))
                #hasilnya berupa list of tuple
                utility_min_video_dicache = list_sorted_utility[0][1]
                
                list_sorted_utility2 = list_sorted_utility[::-1]
                del list_sorted_utility 

                #bandingkan utility video didalam cache dng video yng akan masuk
                #bila utility min dalam cache lebih kecil:
                if utility_min_video_dicache < utility_video_baru:
                    jumlah=sum(self.cache_size.values())
                    #disini secaraiterative hapus cache
                    while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                        tup = list_sorted_utility2.pop()
                        video_id_dng_u_min = tup[0]
                        video_size_dng_u_min = tup[1]
                        del self.cache_size[video_id_dng_u_min]
                        del self.cache_entries[video_id_dng_u_min]
                        self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                        self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                        del self.log_replica[video_id_dng_u_min]
                        jumlah=sum(self.cache_size.values())
                    #setelah jumlah+size_video_baru <= 500 maka
                    #cache utk video baru yng masuk
                    self.cache_entries[content_id]=content_baru
                    self.cache_size[content_id]=size_video_baru
                    self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                    self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                else:
                    pass


            #after
            else: 
                n_ir = float(self.cdn.get_number_requested_video(content_id))
                t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                a_i = self.cdn.get_first_access(content_id)
                if abs(time_cur - t_ir)/1000.0 == 0:
                    kanan = 0.0
                else:
                    kanan = 1000*(1.0)/abs(time_cur - t_ir)
                pembagi = abs(t_ir - a_i)/1000.0
                if pembagi == 0:
                    pembagi = abs(t_ir - 0)/1000.0
                kiri = (n_ir)/pembagi
                P_i = min(kiri,kanan)
                #ambil catatan P_max semua video yng ada di sistem dari cdn 
                p_max_min = self.cdn.hitung_pmax_min(time_cur)
                P_max = p_max_min[1]
                P_min = p_max_min[0]

                #ambil catatan jumlah replica content_id ini dari cdn
                r = self.cdn.get_replica(content_id) 
                r = float(r)
                    
                if P_i == 0:
                    P_i = 0.1
                    #hitung utility video baru
                    #utility_video_baru = abs(math.log(P_max))/r
                if P_min == 0:
                    P_min = 0.1
                if P_max == 0:
                    P_max = 0.1
                if r == 0:
                    utility_video_baru = 0.0
                else:
                    utility_video_baru = (abs(math.log(P_i)) - abs(math.log(P_min))) * (abs(math.log(P_max))-abs(math.log(P_i)))/(r*r)
                utility_video_baru = abs(utility_video_baru) + p_after
                #hitung p utk video didalam cache
                #ambil content id yng sudah ada didalam cache
                temp_3={}
                utility=0
                con=self.cache_entries.keys()
                panjang = len(con)
                time_list = [time_cur for i in xrange(panjang)]
                p_mx = [P_max for i in xrange(panjang)]
                p_mi = [P_min for i in xrange(panjang)]

                result = map(self.loop_con_dicache, time_list, con, p_mx, p_mi)
                #ubah ke dictionary
                temp_3 = dict(zip(con,result))


                #cari minimum utility video didalam cache
                #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                #sorted dictionary by values (utility):
                list_sorted_utility = sorted(temp_3.items(), key=itemgetter(1))
                #hasilnya berupa list of tuple
                utility_min_video_dicache = list_sorted_utility[0][1]

                list_sorted_utility2 = list_sorted_utility[::-1]
                del list_sorted_utility 
                
                #bandingkan utility video didalam cache dng video yng akan masuk
                #bila utility min dalam cache lebih kecil:
                if utility_min_video_dicache < utility_video_baru:
                    jumlah=sum(self.cache_size.values())
                    #disini secaraiterative hapus cache
                    while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                        tup = list_sorted_utility2.pop()
                        video_id_dng_u_min = tup[0]
                        video_size_dng_u_min = tup[1]
                        del self.cache_size[video_id_dng_u_min]
                        del self.cache_entries[video_id_dng_u_min]
                        self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                        self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                        del self.log_replica[video_id_dng_u_min]
                        jumlah=sum(self.cache_size.values())
                    #setelah jumlah+size_video_baru <= 500 maka
                    #cache utk video baru yng masuk
                    self.cache_entries[content_id]=content_baru
                    self.cache_size[content_id]=size_video_baru
                    self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                    self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                else:
                    pass
        return [],[]




    def cache_content(self, content_id, content, time_cur):
        """
        cache the content in the peer
        and report to CDN
        karena membutuhkan cdn class dng method report_to_cdn
        """
        size_video_baru = content[2]
        jumlah=sum(self.cache_size.values())
        content_baru=[ content[0], content[1], content[2], time_cur ]

        #posisi disini diganti dng estimasi t
        #kalau belum pernah diakses akan estimasi dari t
        #kalau sudah pernah diakses akan estimasi dar viewrate
        #posisi_minggu_video_baru = content[4]
        yng_request=self.id

        cek_pertama_kali = self.cdn.get_first_time_requested(content_id)
        #0.149538787758 0.470040393021 0.380420819221
        p_before =0.15 
        p_at = 0.47
        p_after = 0.38


        hasil = self.cdn.get_estimate()


        #check capacity
        if (jumlah+size_video_baru) <= 500: #kurang dari 500MB langsung dicache
            #langsung masuk dicache
            content_baru=[ content[0], content[1], content[2], time_cur ]
            self.cache_entries[content_id]=content_baru
            self.cache_size[content_id]=size_video_baru
            self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
            self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)


        else:
            #before
            if hasil == -1:
                n_ir = float(self.cdn.get_number_requested_video(content_id))
                t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                a_i = self.cdn.get_first_access(content_id)
                if abs(time_cur - t_ir)/1000.0 == 0:
                    kanan = 0.0
                else:
                    kanan = 1000*(1.0)/abs(time_cur - t_ir)
                pembagi = abs(t_ir - a_i)/1000.0
                if pembagi == 0:
                    pembagi = abs(t_ir - 0)/1000.0 #mili second to second
                kiri = (n_ir)/pembagi
                P_i = min(kiri,kanan)
                #ambil catatan P_max semua video yng ada di sistem dari cdn 
                p_max_min = self.cdn.hitung_pmax_min(time_cur)
                P_max = p_max_min[1]
                P_min = p_max_min[0]

                #ambil catatan jumlah replica content_id ini dari cdn
                r = self.cdn.get_replica(content_id) 
                r = float(r)
                    
                if P_i == 0:
                    P_i = 0.1
                    #hitung utility video baru
                    #utility_video_baru = abs(math.log(P_max))/r
                if P_min == 0:
                    P_min = 0.1
                if P_max == 0:
                    P_max = 0.1
                    
                if r == 0:
                    utility_video_baru = 0.0
                else:
                    utility_video_baru = (abs(math.log(P_i)) - abs(math.log(P_min))) * (abs(math.log(P_max))-abs(math.log(P_i)))/(r*r)
                utility_video_baru = abs(utility_video_baru) + p_before
                #hitung p utk video didalam cache
                #ambil content id yng sudah ada didalam cache
                temp_1={}
                utility=0
                con=self.cache_entries.keys()
                panjang = len(con)
                time_list = [time_cur for i in xrange(panjang)]
                p_mx = [P_max for i in xrange(panjang)]
                p_mi = [P_min for i in xrange(panjang)]

                result = map(self.loop_con_dicache, time_list, con, p_mx, p_mi)
                #ubah ke dictionary
                temp_1 = dict(zip(con,result))

                #cari minimum utility video didalam cache
                #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                #sorted dictionary by values (utility):
                list_sorted_utility = sorted(temp_1.items(), key=itemgetter(1))
                #hasilnya berupa list of tuple
                utility_min_video_dicache = list_sorted_utility[0][1]

                list_sorted_utility2 = list_sorted_utility[::-1]
                del list_sorted_utility
                
                #bandingkan utility video didalam cache dng video yng akan masuk
                #bila utility min dalam cache lebih kecil:
                if utility_min_video_dicache < utility_video_baru:
                    jumlah=sum(self.cache_size.values())
                    #disini secaraiterative hapus cache
                    while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                        tup = list_sorted_utility2.pop()
                        video_id_dng_u_min = tup[0]
                        video_size_dng_u_min = tup[1]
                        del self.cache_size[video_id_dng_u_min]
                        del self.cache_entries[video_id_dng_u_min]
                        self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                        self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                        del self.log_replica[video_id_dng_u_min]
                        jumlah=sum(self.cache_size.values())
                    #setelah jumlah+size_video_baru <= 500 maka
                    #cache utk video baru yng masuk
                    self.cache_entries[content_id]=content_baru
                    self.cache_size[content_id]=size_video_baru
                    self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                    self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                else:
                    pass



            #at
            elif hasil == 0:
                n_ir = float(self.cdn.get_number_requested_video(content_id))
                t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                a_i = self.cdn.get_first_access(content_id)
                if abs(time_cur - t_ir)/1000.0 == 0:
                    kanan = 0.0
                else:
                    kanan = 1000*(1.0)/abs(time_cur - t_ir)
                pembagi = abs(t_ir - a_i)/1000.0
                if pembagi == 0:
                    pembagi = abs(t_ir - 0)/1000.0
                kiri = (n_ir)/pembagi
                P_i = min(kiri,kanan)
                #ambil catatan P_max semua video yng ada di sistem dari cdn 
                p_max_min = self.cdn.hitung_pmax_min(time_cur)
                P_max = p_max_min[1]
                P_min = p_max_min[0]

                #ambil catatan jumlah replica content_id ini dari cdn
                r = self.cdn.get_replica(content_id) 
                r = float(r)
                    
                if P_i == 0:
                    P_i = 0.1
                    #hitung utility video baru
                    #utility_video_baru = abs(math.log(P_max))/r
                if P_min == 0:
                    P_min = 0.1
                
                if P_max == 0:
                    P_max = 0.1
                if r == 0:
                    utility_video_baru = 0.0
                else:
                    utility_video_baru = (abs(math.log(P_i)) - abs(math.log(P_min))) * (abs(math.log(P_max))-abs(math.log(P_i)))/(r*r)
                utility_video_baru = abs(utility_video_baru) + p_at
                #hitung p utk video didalam cache
                #ambil content id yng sudah ada didalam cache
                temp_2={}
                utility=0
                con=self.cache_entries.keys()
                panjang = len(con)
                time_list = [time_cur for i in xrange(panjang)]
                p_mx = [P_max for i in xrange(panjang)]
                p_mi = [P_min for i in xrange(panjang)]

                result = map(self.loop_con_dicache, time_list, con, p_mx, p_mi)
                #ubah ke dictionary
                temp_2 = dict(zip(con,result))


                #cari minimum utility video didalam cache
                #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                #sorted dictionary by values (utility):
                list_sorted_utility = sorted(temp_2.items(), key=itemgetter(1))
                #hasilnya berupa list of tuple
                utility_min_video_dicache = list_sorted_utility[0][1]

                list_sorted_utility2 = list_sorted_utility[::-1]
                del list_sorted_utility


                
                #bandingkan utility video didalam cache dng video yng akan masuk
                #bila utility min dalam cache lebih kecil:
                if utility_min_video_dicache < utility_video_baru:
                    jumlah=sum(self.cache_size.values())
                    #disini secaraiterative hapus cache
                    while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                        tup = list_sorted_utility2.pop()
                        video_id_dng_u_min = tup[0]
                        video_size_dng_u_min = tup[1]
                        del self.cache_size[video_id_dng_u_min]
                        del self.cache_entries[video_id_dng_u_min]
                        self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                        self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                        del self.log_replica[video_id_dng_u_min]
                        jumlah=sum(self.cache_size.values())
                    #setelah jumlah+size_video_baru <= 500 maka
                    #cache utk video baru yng masuk
                    self.cache_entries[content_id]=content_baru
                    self.cache_size[content_id]=size_video_baru
                    self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                    self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                else:
                    pass


            #after
            else: 
                n_ir = float(self.cdn.get_number_requested_video(content_id))
                t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                a_i = self.cdn.get_first_access(content_id)
                if abs(time_cur - t_ir)/1000.0 == 0:
                    kanan = 0.0
                else:
                    kanan = 1000*(1.0)/abs(time_cur - t_ir)
                pembagi = abs(t_ir - a_i)/1000.0
                if pembagi == 0:
                    pembagi = abs(t_ir - 0)/1000.0
                kiri = (n_ir)/pembagi
                P_i = min(kiri,kanan)
                #ambil catatan P_max & P_min semua video yng ada di sistem dari cdn 
                p_max_min = self.cdn.hitung_pmax_min(time_cur)
                P_max = p_max_min[1]
                P_min = p_max_min[0]

                #ambil catatan jumlah replica content_id ini dari cdn
                r = self.cdn.get_replica(content_id) 
                r = float(r)
                    
                if P_i == 0:
                    P_i = 0.1
                    #hitung utility video baru
                    #utility_video_baru = abs(math.log(P_max))/r
                if P_min == 0:
                    P_min = 0.1
                if P_max == 0:
                    P_max = 0.1
                if r == 0:
                    utility_video_baru = 0.0
                else:
                    utility_video_baru = (abs(math.log(P_i)) - abs(math.log(P_min))) * (abs(math.log(P_max))-abs(math.log(P_i)))/(r*r)
                utility_video_baru = abs(utility_video_baru) + p_after
                #hitung p utk video didalam cache
                #ambil content id yng sudah ada didalam cache
                temp_3={}
                utility=0

                con=self.cache_entries.keys()
                panjang = len(con)
                time_list = [time_cur for i in xrange(panjang)]
                p_mx = [P_max for i in xrange(panjang)]
                p_mi = [P_min for i in xrange(panjang)]

                result = map(self.loop_con_dicache, time_list, con, p_mx, p_mi)
                #ubah ke dictionary
                temp_3 = dict(zip(con,result))

                #cari minimum utility video didalam cache
                #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                #sorted dictionary by values (utility):
                list_sorted_utility = sorted(temp_3.items(), key=itemgetter(1))
                #hasilnya berupa list of tuple
                utility_min_video_dicache = list_sorted_utility[0][1]
                list_sorted_utility2 = list_sorted_utility[::-1]
                del list_sorted_utility
                
                #bandingkan utility video didalam cache dng video yng akan masuk
                #bila utility min dalam cache lebih kecil:
                if utility_min_video_dicache < utility_video_baru:
                    jumlah=sum(self.cache_size.values())
                    #disini secaraiterative hapus cache
                    while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                        tup = list_sorted_utility2.pop()
                        video_id_dng_u_min = tup[0]
                        video_size_dng_u_min = tup[1]
                        del self.cache_size[video_id_dng_u_min]
                        del self.cache_entries[video_id_dng_u_min]
                        self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                        self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                        del self.log_replica[video_id_dng_u_min]
                        jumlah=sum(self.cache_size.values())
                    #setelah jumlah+size_video_baru <= 500 maka
                    #cache utk video baru yng masuk
                    self.cache_entries[content_id]=content_baru
                    self.cache_size[content_id]=size_video_baru
                    self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                    self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                else:
                    pass
        return [],[]
    
    def remove_content(self, content_id):
        """
        menghapus cache
        """
        #print self.id, content_id
        yng_request=self.id
        self.cdn.receive_report_from_peer(self.id, content_id, 'REMOVE_CACHE',  time_cur, yng_request)
        #print self.id, content_id
        del self.cache_size[content_id]
        del self.cache_entries[content_id]
        return [],[]


    def remove_content_extend(self, content_id):
        """
        menghapus cache
        """
        #print self.id, content_id
        yng_request=self.id
        self.cdn.receive_report_from_peer(self.id, content_id, 'REMOVE_CACHE', time_cur, yng_request)
        #print self.id, content_id
        del self.cache_entries[content_id]
        return [],[]

    def remove_content_old(self, content_id):
        """
        menghapus cache
        """
        #print self.id, content_id
        #self.cdn.receive_report_from_peer(self.id, content_id, 'REMOVE_CACHE')
        #print self.id, content_id
        #del self.cache_entries[content_id]
        return [],[]


    
    def change_content_expiry_time(self, content, time_cur):
        """
        mengubah content expiry time, misalnya saat upload content yang akan expire waktu content sedang diupload
        """
        content_id = content[0]
        time_lama = self.cache_entries[content_id][3]

        #hitung durasi dan time_baru
        #contoh: {0: [0, 5.0775931306168065, 499.0, 3600.0]}
        durasi = (content[2]*8)/(self.dn_bw/1000.0) * 1000
        time_baru = (time_cur*1000) + durasi
        #print '--->', time_cur, content[0], content[1], content[2], self.dn_bw, durasi, time_baru, time_lama
        if time_baru > time_lama:
            new_expire_event = event.Event(event.REMOVE_CONTENT, time_baru, self, self.remove_content_extend, [content[0]])
            old_expire_event = event.Event(event.REMOVE_CONTENT, time_lama, self, self.remove_content, [content[0]])
            #print 'change',  new_expire_event, old_expire_event
            #update cache entries dng time baru.
            self.cache_entries[content_id][3] = time_baru
            return [new_expire_event, old_expire_event]
        else:
            return None, None
        
