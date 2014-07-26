#!/usr/bin/env pypy


import peer
import event
import cdn
import math
import cPickle as pickle
from datetime import datetime
from os import listdir
import random
import bisect


#time_cur = 10800 
cache_size = 1000.0 #100M
upload_bw = 1000.0 #100kbps
download_bw = 1000.0 #1Mbps = 1000Kbps
lamb = 1.2
event_list = event.Timeline()
numpeer = 10000
TIME_LENGTH=86400

with open('contents_baru.pickle', 'rb') as handle:
    contents = pickle.load(handle)
handle.close()
this_cdn = cdn.CDN(contents)

# generate peers
peer_list = []
for p in range(numpeer):
   np = peer.Peer(p, this_cdn, cache_size, upload_bw, download_bw)
   peer_list.append(np)
this_cdn.set_peer_list(peer_list)


file_list_asal = []
file_list_asal=listdir("requestreplica/")
panjang=len(file_list_asal)

nilai = []
#cari maximum
for i in range(panjang):
    val=int(file_list_asal[i].rpartition('-')[2].partition('.')[0])
    nilai.append(val)
nilai.sort()

file_list=['requestreplica/request_events-1t-'+str(i)+'.pickle' for i in nilai]

def extend_event_list(filename):
    with open(filename, 'rb') as f:
        print 'LOAD', filename
        et = pickle.load(f)
        et.unmarshall(peer_list)
        event_list.extend_timeline(et)
    f.close()
    print str(datetime.now())

def get_first_time_requested(content_id):
    """
    ambil jumlah pertama kali video diakses
    """
    if video_tracking_number_requested[content_id] < 2:
        return 1  #positif pertama kali akses
    else:
        return -1 #negatif, sudah ada yng akses

def get_video_last_time_requested(content_id):
    """
    ambil waktu terakhir video direquest
    """
    return video_tracking_time_requested[content_id]['last']

def get_first_access(content_id):
    """
    get first access time
    """
    return first[content_id]


def get_number_requested_video(content_id):
    """
    ambil jumlah video yng telah di request
    """
    #print video_tracking_number_requested
    return video_tracking_number_requested[content_id]


def estimasi_vr(t_cur, content_id, percentile_request):
    """
    estimasi t before/at/after berdasarkan minggu dan view rate
    membutuhkan data view_rate dan minggu
    """
    #dapatkan minggu video
    #selisih = t_cur - t_upload
    upload_time = contents[content_id][1]
    waktu_terakhir_akses=get_video_last_time_requested(content_id)
    #selisih = waktu_terakhir_akses - upload_time
    selisih = t_cur - upload_time

    selisih = selisih/1000.0  #mili second to second
    minggu,second = divmod(selisih, 7*24*60*60)
    minggu = int(minggu)

    #kalau minggu 0
    #view rate yng dipakai 
    #selisih view count dng pertama kali upload
    if minggu == 0:
        view_rate = get_number_requested_video(content_id) - 1

    else:
        #kalau minggu > 0
        #view rate yng dipakai view rate pada minggu tsb
        #view count pada saat ini - view count terakhir minggu lalu
        view_count_saat_ini = get_number_requested_video(content_id)-1
        if not mingguan[content_id].has_key(minggu-1):
            mingguan[content_id][minggu-1]=0
            view_count_terakhir_minggu_lalu = mingguan[content_id][minggu-1]
        else:
            view_count_terakhir_minggu_lalu = mingguan[content_id][minggu-1] 
                
        view_rate = view_count_saat_ini - view_count_terakhir_minggu_lalu

    hasil=hitung_kmeans(minggu,view_rate,percentile_request)
    #pool = Pool(processes=4)
    #hasil = pool.apply_async( hitung_kmeans, (minggu,view_rate) )
    #return hasil.get()
    return hasil


def catatan_weekly( t):
    """
    items = [(0.5, "1"),
        (0.3, "2"),
        (0.2, "3")]
    """
    weekly_prob=[ (0.18297472391,1), (0.264039475009,2), (0.12956933302,3), 
                  (0.0642140243698,4), (0.0789835856467,5), (0.0569299452855,6), 
                  (0.00386022624283,7), (0.00362525594978,8), (0.00493437615387,9),
                  (0.0114799771743,10), (0.0058071229566,11), (0.00228256856097,12), 
                  (0.0026518075929,13), (0.0105736631869,14), (0.00966734919942,15), 
                  (0.00238327011514,16), (0.00375952468866,17), (0.00704910879125,18),
                  (0.012352723977,19), (0.00359168876506,20), (0.00312174817898,21),
                  (0.00500151052331,22), (0.00560571984828,23), (0.00802255714813,24),
                  (0.00671343694404,25), (0.00866033365782,26), (0.0124198583465,27),
                  (0.0154409049713,28), (0.00788828840925,29), (0.00902957268974,30),
                  (0.0105065288174,31), (0.0250075526166,32), (0.0123862911618, 33),
                  (0.00288677788594,34), (0.00657916820516,35) ]

    n=36
    hasil=weighted_sample(weekly_prob,n)
    temp_hasil = []
    for i in hasil:
        temp_hasil.append(i)
    temp_hasil.sort()
    banyak = temp_hasil.count(t)
    proba = banyak/float(n)
    if proba < 0.5:
        return -1 #before
    elif proba >= 0.5 and proba <=0.75:
        return 0 #at
    else:
        return 1 #positif peak


def weighted_sample(items, n):
    total = float(sum(w for w, v in items))
    i = 0

    w, v = items[0]
    while n:
        x = total * (1 - random.random() ** (1.0 / n))
        total -= x
        while x > w:
            x -= w
            i += 1
            w, v = items[i]
        w -= x
        yield v #ini generator!!!!
        n -= 1

def popularity(nir, tir, ai, time_cur):
    """
    hitung popularity
    """
    #berapa kali direquest
    n_ir = nir
    #terakhir kali diacccess
    t_ir = tir
    #pertama kali diakses
    a_i = ai
    if abs(time_cur - t_ir)/1000.0 == 0:
        kanan = 0.0
    else:
        kanan = 1000*(1.0)/abs(time_cur - t_ir)

    pembagi = abs(t_ir - a_i)/1000.0
    if pembagi == 0:
        pembagi = abs(t_ir - 0)/1000.0
    kiri = (n_ir)/pembagi
    P_i = min(kiri,kanan)
    return P_i



def percentile(N, P):
    n = int(round(P * len(N) + 0.5))
    if n > 1:
        return N[n-2]
    else:
        return 0

def hitung_kmeans(x_cari, y_cari, percentile_request):
    """
    hitung k nn neighbors
    """
    percentile_data = 0
    temp_1 = []

    if x_cari < 33:
    	list_of_y_key = sorted(x_dict[x_cari].keys())
    	maks_y = max(list_of_y_key)
    	#hitung percentile
    	target = maks_y
    	b=1
    	akhir = 10
    	while b<10:
        	persen = b*0.1
        	hasil_percentile=percentile(list_of_y_key,P=persen)
        	if target <= hasil_percentile:
            		percentile_data = persen
            		break
        	else: 
            		percentile_data =1.0
        	b+=1
    	pengali = float(percentile_data/percentile_request)
    	y_cari = y_cari * pengali


    if x_cari == 0:
        hasil = []
        # temp_1 = []
        # ini berarti x_dict[0]

        #utk x_cari = 0
        # y tdk sama deng y_cari
        if not x_dict[x_cari].has_key(y_cari):
            #cari y terdekat:
            list_of_y_key = sorted(x_dict[x_cari].keys())
            nilai_max = max(list_of_y_key)
            nilai_min = min(list_of_y_key)
            #cari pakai bisec
            indeks = bisect.bisect_left(list_of_y_key, y_cari)
            if y_cari > nilai_max:
                key_1 = list_of_y_key[indeks-1]
                temp_1 = x_dict[x_cari][key_1]
                #hasil.append(sum(temp_1)/float(len(temp_1)))
            elif y_cari < nilai_min:
                key_2 = list_of_y_key[indeks]
                temp_1 = x_dict[x_cari][key_2]
                #hasil.append(sum(temp_1)/float(len(temp_1)))
            else:
                try: 
                    key_2 = list_of_y_key[indeks]
                    key_1 = list_of_y_key[indeks-1]
                    temp_1 = x_dict[x_cari][key_1] + x_dict[x_cari][key_2]
                except:
                    print indeks, len(list_of_y_key), list_of_y_key
            hasil.append(sum(temp_1)/float(len(temp_1)))
        else:
            #sisanya y_cari = y
            temp_1 = x_dict[x_cari][y_cari]
            hasil.append(sum(temp_1)/float(len(temp_1)))

        #utk x  = x_cari + 1
        # y tdk sama dengan y_cari
        if not x_dict[x_cari+1].has_key(y_cari):
            #cari y terdekat:
            list_of_y_key = sorted(x_dict[x_cari+1].keys())
            nilai_max = max(list_of_y_key)
            nilai_min = min(list_of_y_key)
            #cari pakai bisec
            indeks = bisect.bisect_left(list_of_y_key, y_cari)
            if y_cari > nilai_max:
                key_1 = list_of_y_key[indeks-1]
                temp_1 = x_dict[x_cari+1][key_1]
            elif y_cari < nilai_min:
                key_2 = list_of_y_key[indeks]
                temp_1 = x_dict[x_cari+1][key_2]
            else:
                key_1 = list_of_y_key[indeks-1]
                key_2 = list_of_y_key[indeks]
                temp_1 = x_dict[x_cari+1][key_1] + x_dict[x_cari+1][key_2]
            hasil.append(sum(temp_1)/float(len(temp_1)))
        else:
            #sisanya y_cari = y
            temp_1 = x_dict[x_cari+1][y_cari]
            hasil.append(sum(temp_1)/float(len(temp_1)))
        rata=sum(hasil)/float(len(hasil))
        if rata <= -0.5: #toleransi 
            return -1 #before
        elif rata > -0.5 and rata <= 0.5: 
            return 0 #at
        else:
            return 1 #after
    
    elif x_cari < 33 and x_cari > 0:
        hasil=[]
        #utk x = x_cari
        # y tdk sama dengan y_cari
        if not x_dict[x_cari].has_key(y_cari):
            #cari y terdekat:
            list_of_y_key = sorted(x_dict[x_cari].keys())
            nilai_max = max(list_of_y_key)
            nilai_min = min(list_of_y_key)
            #cari pakai bisec
            indeks = bisect.bisect_left(list_of_y_key,y_cari)
            if y_cari > nilai_max:
                key_1 = list_of_y_key[indeks-1]
                temp_1 = x_dict[x_cari][key_1]
                #hasil.append(sum(temp_1)/float(len(temp_1)))
            elif y_cari < nilai_min:
                key_2 = list_of_y_key[indeks]
                temp_1 = x_dict[x_cari][key_2]
                #hasil.append(sum(temp_1)/float(len(temp_1)))
            else:
                key_1 = list_of_y_key[indeks-1]
                key_2 = list_of_y_key[indeks]
                temp_1 = x_dict[x_cari][key_1] + x_dict[x_cari][key_2]
            hasil.append(sum(temp_1)/float(len(temp_1)))
        else:
            #sisanya y_cari = y
            temp_1 = x_dict[x_cari][y_cari]
            hasil.append(sum(temp_1)/float(len(temp_1)))

        #utk x = x_cari + 1
        # y tdk sama deng y_cari
        if not x_dict[x_cari+1].has_key(y_cari):
            #cari y terdekat:
            list_of_y_key = sorted(x_dict[x_cari+1].keys())
            nilai_max = max(list_of_y_key)
            nilai_min = min(list_of_y_key)
            #cari pakai bisec
            indeks = bisect.bisect_left(list_of_y_key,y_cari)
            if y_cari > nilai_max:
                key_1 = list_of_y_key[indeks-1]
                temp_1 = x_dict[x_cari+1][key_1]
            elif y_cari < nilai_min:
                key_2 = list_of_y_key[indeks]
                temp_1 = x_dict[x_cari+1][key_2]
            else:
                key_1 = list_of_y_key[indeks-1]
                key_2 = list_of_y_key[indeks]
                temp_1 = x_dict[x_cari+1][key_1] + x_dict[x_cari+1][key_2]
            hasil.append(sum(temp_1)/float(len(temp_1)))
        else:
            #sisanya y_cari = y
            temp_1 = x_dict[x_cari+1][y_cari]
            hasil.append(sum(temp_1)/float(len(temp_1)))

        #utk x = x_cari -1 
        # y tdk sama deng y_cari
        if not x_dict[x_cari-1].has_key(y_cari):
            #cari y terdekat:
            list_of_y_key = sorted(x_dict[x_cari-1].keys())
            nilai_max = max(list_of_y_key)
            nilai_min = min(list_of_y_key)
            #cari pakai bisec
            indeks = bisect.bisect_left(list_of_y_key,y_cari)
            if y_cari > nilai_max:
                key_1 = list_of_y_key[indeks-1]
                temp_1 = x_dict[x_cari-1][key_1]
            elif y_cari < nilai_min:
                key_2 = list_of_y_key[indeks]
                temp_1 = x_dict[x_cari-1][key_2]
            else:
                key_2 = list_of_y_key[indeks]
                key_1 = list_of_y_key[indeks-1]
                temp_1 = x_dict[x_cari-1][key_1] + x_dict[x_cari-1][key_2]
            hasil.append(sum(temp_1)/float(len(temp_1)))
        else:
            #sisanya y_cari = y
            temp_1 = x_dict[x_cari-1][y_cari]
            hasil.append(sum(temp_1)/float(len(temp_1)))

        rata=sum(hasil)/float(len(hasil))
        if rata <= -0.5: #toleransi 
            return -1 #before
        elif rata > -0.5 and rata < 0.5: 
            return 0 #at
        else:
            return 1 #after
    else:
        return 1



#pertama kali baca
print 'masuk pertama kali'
extend_event_list(file_list.pop(0))

x_dict={}
fd=open('datatest2d.dat', "r")
for line in fd:
    x_key = int(line.split()[0])
    y_key = int(line.split()[1])
    z_val = int(line.split()[2])

    if not x_dict.has_key( x_key ):
        x_dict[x_key]={}
        x_dict[x_key][y_key] = []
    if not x_dict[x_key].has_key(y_key):
        x_dict[x_key][y_key] = []
    x_dict[x_key][y_key].append(z_val)
fd.close()



data = {}
counter = 0
timelist=[]
first={}
video_tracking_time_requested={}
video_tracking_number_requested = {}
popularity_tracking = {}
first_time = {}
mingguan = {}
hasil = []
kamus = {}
while event_list:
    next_ev = event_list.get_next_event()
    if file_list and not event_list:
        extend_event_list(file_list.pop(0))

    time = float( next_ev.action_params[1] )
    content_id =int( next_ev.action_params[0] )

    if not kamus.has_key(content_id):
        kamus[content_id]=[]

    if not video_tracking_time_requested.has_key(content_id):
        video_tracking_time_requested[content_id]={}
        video_tracking_time_requested[content_id]['new']=time
        video_tracking_time_requested[content_id]['last']=0

    video_tracking_time_requested[content_id]['last']=video_tracking_time_requested[content_id]['new']
    video_tracking_time_requested[content_id]['new']=time

    if not first.has_key(content_id):
        first[content_id]=time

    video_tracking_number_requested[content_id] = video_tracking_number_requested.get(content_id,0)+1

    if not popularity_tracking.has_key(content_id):
        popularity_tracking[content_id] = 0

    if not first_time.has_key(content_id):
        first_time[content_id] = time



    upload_time = contents[content_id][1]
    selisih = time-upload_time

    jumlah_request = video_tracking_number_requested[content_id]

    if not mingguan.has_key(content_id):
        mingguan[content_id]={}

    selisih = selisih/1000.0 #mili second to second
    minggu,second = divmod(selisih, 7*24*60*60)
    minggu = int(minggu)
    mingguan[content_id][minggu]=jumlah_request

    #check pertama kali datang atau tidak
    cek_pertama_kali = get_first_time_requested(content_id)

    nir = video_tracking_number_requested[content_id]
    tir = video_tracking_time_requested[content_id]['last']
    ai = first_time[content_id]
    popu = popularity(nir, tir, ai, time)
    popularity_tracking[content_id]=popu
    tempo = []
    for k,v in popularity_tracking.iteritems():
        tempo.append(v)
    tempo = sorted(tempo)
    #hitung percentile
    target = popu
    b=1
    akhir = 10
    while b<10:
        persen = b*0.1
        hasil_percentile=percentile(tempo,P=persen)
        if target <= hasil_percentile:
            percentile_request = persen
            break
        b+=1

    
    if cek_pertama_kali == 1:
        tebakan_minggu = catatan_weekly(minggu)
        if tebakan_minggu == -1:
            #before
            kamus[content_id].append( (time,-1) ) 
        elif tebakan_minggu == 0:
            #at
            kamus[content_id].append( (time,0) )
        else:
            #after
            kamus[content_id].append( (time,1) ) 

    else:
        #tdk pertama kali diakses
        hasil = estimasi_vr(time,content_id, percentile_request)
        if hasil == -1:
            #before
            kamus[content_id].append( (time,-1) )
        elif hasil == 0:
            #at
            kamus[content_id].append( (time,0) )
        else:
            #after
            kamus[content_id].append( (time,1) )


#terakhir
filenames = 'timelistcontentidreplica' + '.pickle'
with open(filenames,'wb') as fs:
    pickle.dump(kamus,fs)
fs.close()
