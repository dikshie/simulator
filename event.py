#!/usr/bin/env python
#import bisect
#import collections
#import cProfile

class Timeline(object):
    def __init__(self):
        self.timeline = []

    def __str__(self):
         return str(self.timeline)
    
    def search_event_pos2(self, event_object):
        lo,hi = 0, self.time_length

        time_search = event_object.time
        #print self.time_length,(time_search - self.time_first),(self.time_last - self.time_first)
        #if self.time_length > 70000:
        try:            
            guess_hi = int(1.5*self.time_length*(time_search - self.time_first)/(self.time_last - self.time_first))
            if guess_hi < self.time_length >> 6: #significantly small
                if self.timeline[guess_hi].time > time_search:
                    hi = guess_hi
                    #print 'SIGNIFICANTLY SMALL',hi, guess_hi, self.time_length
                    #else:
                    #print 'NOT SMALL',hi, guess_hi, self.time_length
        except:
            #print 'masuk except hi'
            hi=self.time_length

        #counters=0
        while lo < hi:
            mid = (lo+hi)//2
            if self.timeline[mid].time < time_search:
                lo = mid + 1
            else:
                hi = mid
            #counters+=1
        #print 'BISECT COUNTERS 2 :', counters, len(self.timeline), lo
        return lo

    def dual_pivot(self, event_object, lo=0, hi=0):
        if lo==hi:
            return lo

        p = lo + (hi-lo)//3
        q = lo + 2 * (hi-lo)//3
       

        if event_object.time < self.timeline[p].time:
            return self.dual_pivot(event_object,lo,p)

        elif (event_object.time > self.timeline[p].time and event_object.time < self.timeline[q].time):
            return self.dual_pivot(event_object,p+1,q)

        elif event_object.time > self.timeline[q].time:
            return self.dual_pivot(event_object,q+1,hi)
    
        if event_object.time == self.timeline[p].time:
            #print 'DUAL PIVOT', dual_count, p
            return p
        else:
            #print 'DUAL PIVOT', dual_count, q
            return q

    def add_event(self, event_object):
        try:
            if not self.timeline:
                self.timeline.append(event_object)
                
                return 0
            #print event_object.time
            #idx = self.search_event_pos2(event_object)
            #idx = self.search_event_pos_numpy(event_object)
            self.time_first=self.timeline[0].time
            self.time_last=self.timeline[-1].time
            self.time_length = len(self.timeline)
            
            idx=self.search_event_pos2(event_object)
            #print 'IDXXX', idx

            #idx = self.dual_pivot(event_object,0,len(self.timeline))
            if idx==self.time_length:
                self.timeline.append(event_object)
            else:
                self.timeline.insert(idx, event_object)
            return idx
        except:
            pass
        #for i in range(len(self.timeline)):    
        #    print self.timeline[i].time


    def cancel_event(self, event_object):
        """
        untuk menghapus suatu event, perlu:
        1. event.actor
        2. event.action
        3. event.action_param
        """
        if not self.timeline:
            self.timeline.append(event_object)
            return 0
        
        #for i in range(len(self.timeline)):
        #        templist.append(self.timeline[i].time)
        #indekx awal
        idx=self.search_event_pos2(event_object)
        #idx=self.dual_pivot(event_object,0,len(self.timeline))
        #idx=idx-1
        #print idx
        awal=self.timeline[idx].time
        #print idx, awal, self.timeline[idx].time, awal==self.timeline[idx].time
        #i=0
        while awal: 
            #print idx, awal, self.timeline[idx].time, awal==self.timeline[idx].time
            if awal != self.timeline[idx].time:
                print 'cancel index not found, FATAL'
                return idx
            if self.timeline[idx].actor == event_object.actor and self.timeline[idx].action == event_object.action:
                self.timeline.pop(idx)
                #awal = False
                return idx
            else:
                idx+=1

        #idx = 0
        #for idx, ev in enumerate(self.timeline):
        #    if ev.actor == partial_e_o.actor and ev.action == partial_e_o.action:
        #        param_equal = True
        #        for p_idx, p_obj in enumerate(partial_e_o.action_params):
        #            if p_obj != ev.action_params[p_idx]:
        #                param_equal = False
        #                break
        #        if param_equal:
        #            break
        #else:
        #    return -1
        #self.timeline.pop(idx)
        #return idx
        
    def get_next_event(self):
        if self.timeline:
            return self.timeline.pop(0)
        else:
            return None
            
    def append_event(self, object_event):
        self.timeline.append(object_event)

    def __nonzero__(self):
        if self.timeline:
            return True
        else:
            return False

    def marshall(self):
        self.marshalled_timeline = [ ev.marshall() for ev in self.timeline ]
        del self.timeline

    def unmarshall(self, peer_list):
        self.timeline = []
        for m in self.marshalled_timeline:
            ev = Event(0,0)
            ev.unmarshall(m, peer_list)
            self.timeline.append(ev)

    def extend_timeline(self, other):
        if other.timeline:
            self.timeline += other.timeline

class Event(object):
    def __init__(self, type, time, actor=None, action=None, action_params=None):
        self.type = type #types 1 = request, 2 = download, 3 = save ....
        self.time = time
        self.actor = actor
        self.action = action
        self.action_params = action_params #list of parameters
        
    def execute(self):
        return self.action(*self.action_params)
        #self.action is a callable that returns a tuple of new event list and cancelled event list
        
    def __repr__(self):
        return '%s %s  %s.%s %s'%(self.time, self.type, self.actor,  self.action.__name__, self.action_params[0])
        #return [self.type, self.time, self.actor]
    
    def __str__(self):
        return '%s %s  %s.%s %s'%(self.time, self.type, self.actor,  self.action.__name__, self.action_params[0])

    def set_actor_list(self, actor_list):
        self.actor_list = actor_list

    def marshall(self):
        return {'type':self.type,'time':self.time,'actor':self.actor.id,'action':self.action.__name__,'action_params':self.action_params}

    def unmarshall(self,m, peer_list):
        self.type = m['type']        
        self.time = m['time']
        #self.actor = self.actor_list[m['actor']]
        self.actor = peer_list[m['actor']]
        self.action = getattr(self.actor, m['action'])
        self.action_params = m['action_params']
      
REQUEST = 1
DOWNLOAD = 2
UPLOAD_START = 3
UPLOAD_DONE = 4
CACHE_CONTENT = 5
REMOVE_CONTENT = 6  
UPDATE_STATE = 7
REMOVE_CONTENT_DUMMY = 8
CACHE_CONTENT_CDN = 9
LOG = 10
