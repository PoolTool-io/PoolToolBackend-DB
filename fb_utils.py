
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import messaging
from config import *
import math
from pt_utils import *
import asyncio
import concurrent.futures


class fb_utils:
    def __init__(self):

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=40)
        cred = credentials.Certificate('firebase-account.json')
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://pegasus-pool.firebaseio.com'
        })
        self.poolCache={}
        self.writebatch=[]

    def push_notification(self,title,message,fcm_token):
       

        # apns
        alert = messaging.ApsAlert(title = title, body = message)
        aps = messaging.Aps(alert = alert, sound = "default")
        payload = messaging.APNSPayload(aps)
        # message
        msg = messaging.Message(
            notification = messaging.Notification(
                title = title,
                body = message
            ),
            # data = ntf_data,
            token = fcm_token,
            
            apns = messaging.APNSConfig(payload = payload)
        )

        # send
        res = messaging.send(msg)

    def updateFb(self,path,update):
        #print("updating: ",path,update)
        db.reference(path).update(update)
    
    def pushFb(self,path,update):
        #print("pushing: ",path,update)
        db.reference(path).push(update)
    
    def deleteFb(self,path):
        #print("deleting: ",path)
        db.reference(path).delete()

    def setFb(self,path,update):
        #print("setting: ",path)
        db.reference(path).set(update)

    def writeFb(self,path,value):
        self.writebatch.append({"path":path,"value":value})

    async def get_batch_tasks(self,batch,event_loop):
        coroutines=[]
        for item in batch:
            coroutines.append(self.write_fb_queue(self.event_loop,item['path'],item['value'])) 
        completed, pending = await asyncio.wait(coroutines)

    async def write_fb_queue(self,event_loop,path,value):
        await event_loop.run_in_executor(self.executor, self.updateFb, path, value)
        return True

    def initializePoolCache(self):
        if len(self.poolCache)==0:
            self.poolCache = db.reference(baseNetwork+"/stake_pools").get()
    
    def getReference(self,path):
        return db.reference(path)
        
    def getKey(self,path):
        return db.reference(path).get()
    
    def getDelegatorList(self,pool_id):
        return db.reference(baseNetwork+"/pool_stats/"+str(pool_id)+"/delegators").get()
    
    def getPoolData(self,pool_id):
        return db.reference(baseNetwork+"/stake_pools/"+str(pool_id)).get()
    
    def updatePoolCache(self,pool_id,key,value):
        if pool_id not in self.poolCache:
            self.poolCache[pool_id]={}
        self.poolCache[pool_id][key]=value
    
    def writeBatch(self):
        print("async writing ",len(self.writebatch)," updates")
        if len(self.writebatch):
            self.event_loop = asyncio.new_event_loop()
            try:
                self.event_loop.run_until_complete(self.get_batch_tasks(self.writebatch, self.event_loop))
            finally:
                self.event_loop.close()
            self.writebatch=[]




    

    def poolUpdateLedgerWrite(self,hex_pool_id,pool_name,ticker,changes,block_time):
        if len(changes):
            for change in changes:
                changepacket={}
                if change['type']=="margin_cost":
                    changepacket['type']="margin" # 0%, 3.00% in from/to written
                    if change['old'] == 0:
                        changepacket['from']='0%'
                    else:
                        changepacket['from']="{:.2f}%".format(change['old']*100)
                    if change['new'] == 0:
                        changepacket['to']='0%'
                    else:
                        changepacket['to']="{:.2f}%".format(change['new']*100)
                elif change['type']=="pledge":
                    changepacket['type']="pledge"
                    changepacket['from']=human_format(change['old']/1e6)
                    changepacket['to']=human_format(change['new']/1e6)
                elif change['type']=="retire":
                    changepacket['type']="retire"
                    changepacket['retiring_epoch']=change['retiring_epoch']
                elif change['type']=="fixed_cost":
                    changepacket['type']="fixed"
                    changepacket['from']=human_format(change['old']/1e6)
                    changepacket['to']=human_format(change['new']/1e6)
                elif change['type']=="ticker":
                    continue
                    # changepacket['type']="ticker" 
                    # changepacket['from']=change['old']
                    # changepacket['to']=change['new']
                elif change['type']=="registration":
                    changepacket['type']="registration" 
                else:
                    print("unknown change type")
                    print(change)
                changepacket['time']=block_time*1000
                changepacket['name']=pool_name
                changepacket['poolId']=hex_pool_id
                changepacket['ticker']=ticker
                db.reference(baseNetwork+"/all_pool_updates_blockfrost").push(changepacket)
                db.reference(baseNetwork+"/pool_updates_blockfrost/"+str(hex_pool_id)).push(changepacket)
        else:
            print("no changes")