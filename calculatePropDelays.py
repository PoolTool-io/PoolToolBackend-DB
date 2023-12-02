from config import *
from pg_utils import *
from fb_utils import *
from aws_utils import *
import json
from json import JSONEncoder
import numpy as np

pg=pg_utils("fixPoolBlocks")
fb=fb_utils()
aws=aws_utils()


pg.cur1_execute("select epoch from blocks order by block desc limit 1")
row=pg.cur1_fetchone()

target_epoch = int(row['epoch'])-1

class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

by_producer={}
by_producer_pool_ids={}
by_producer_datapoints={}

by_receiver={}
by_receiver_pool_ids={}
by_receiver_datapoints={}
by_receiver_by_pool={}

pg.cur1_execute("select hash, block, pool_id from blocks where epoch=%s order by block asc",[target_epoch])
row=pg.cur1_fetchone()
while row:
    blockhash = row['hash']
    blockheight = row['block']
    poolid = row['pool_id']
    try:
        tipdata = aws.load_s3("blockdata/"+str(math.floor(int(blockheight)/1000))+"/"+str(blockhash)+".json")
    except:
       tipdata = []
    if len(tipdata):
        for tip in tipdata['rawtips']:
            
            
            if True: #tipdata['rawtips'][tip] < 10000 and tipdata['rawtips'][tip] > -60000: # throw out extraneous items (disabled)
                if tipdata['rawtips'][tip]>5000:
                    tipdata['rawtips'][tip]=5000
                if tipdata['rawtips'][tip] < -5000:
                    tipdata['rawtips'][tip]=-5000
                if poolid not in by_producer:
                    by_producer[poolid]=[]
                    by_producer_pool_ids[poolid]=[]
                    by_producer_datapoints[poolid]=0
                by_producer[poolid].append(tipdata['rawtips'][tip])
                if tip not in by_producer_pool_ids[poolid]:
                    by_producer_pool_ids[poolid].append(tip) # if unique?
                by_producer_datapoints[poolid]+=1
                if tip not in by_receiver:
                    by_receiver[tip]=[]
                    by_receiver_pool_ids[tip]=[]
                    by_receiver_datapoints[tip]=0
                    by_receiver_by_pool[tip]={}
                if poolid not in by_receiver_by_pool[tip]:
                    by_receiver_by_pool[tip][poolid]=[]
                if poolid not in by_receiver_pool_ids[tip]:
                    by_receiver_pool_ids[tip].append(poolid) # if unique?
                by_receiver_datapoints[tip]+=1
                by_receiver[tip].append(tipdata['rawtips'][tip])


    row=pg.cur1_fetchone()

print("outputing producer histograms")
for poolid in by_producer:
    
    avg =  sum(by_producer[poolid])/len(by_producer[poolid]) if len(by_producer[poolid])>0  else 0
    hist = json.loads(json.dumps(np.histogram(by_producer[poolid], range=(-5000,5000), bins=100),cls=NumpyArrayEncoder))
    
    print("stats/pools/"+str(poolid)+"/by_producer.json")
    aws.dump_s3({"epoch":target_epoch,"hist":hist,"avg":avg, "unique_pools":len(by_producer_pool_ids[poolid]),"datapoints":by_producer_datapoints[poolid]},"stats/pools/"+str(poolid)+"/by_producer.json")

print("outputing receiver histograms")
for poolid in by_receiver:
    
    avg = sum(by_receiver[poolid])/len(by_receiver[poolid]) if len(by_receiver[poolid])>0 else 0
    hist = json.loads(json.dumps(np.histogram(by_receiver[poolid], range=(-5000,5000), bins=100),cls=NumpyArrayEncoder))
    print("stats/pools/"+str(poolid)+"/by_receiver.json")
    aws.dump_s3({"epoch":target_epoch,"hist":hist,"avg":avg,"unique_pools":len(by_receiver_by_pool[poolid]),"datapoints":by_receiver_datapoints[poolid]},"stats/pools/"+str(poolid)+"/by_receiver.json")

print("done")