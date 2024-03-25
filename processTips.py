#!/usr/bin/python3

from config import *
from fb_utils import *
from pt_utils import *
from aws_utils import *
from pg_utils import *
from log_utils import *

import json
import binascii
import time
import hashlib
import io
from datetime import datetime, timedelta
from copy import deepcopy
import psycopg2.extras
from psycopg2.extras import Json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import statistics
import math
import numpy as np
from json import JSONEncoder
from ctypes import *
cardanocli = "/home/ubuntu/.cabal/bin/cardano-cli"
environ["CNODE_HOME"] = "/opt/cardano/cnode"
environ["CARDANO_NODE_SOCKET_PATH"] = environ["CNODE_HOME"] + "/sockets/node0.socket"

# Bindings are not avaliable so using ctypes to just force it in for now.
libsodium = cdll.LoadLibrary("/usr/local/lib/libsodium.so")

libsodium.sodium_init()

fb=fb_utils()
aws=aws_utils()
pg=pg_utils("ProcessTips")
pg.cur2_close() # not needed
pg.cur3_close() # not needed


heightbattles={}
vrfmins={}
bvrfmins={}
response = {}
vkeys={}
tickers={}

class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

def saveHistogram(height,hash,rawtips):
    tiptiming=[]
    histogram={}
    for poolid in rawtips:
        tiptiming.append(rawtips[poolid])
    histogram=json.dumps(np.histogram(tiptiming, bins=50, range=(0,10000)),cls=NumpyArrayEncoder)
    median=np.median(tiptiming)
    plt.clf()
    plt.hist(tiptiming, bins=50)
    plt.title('Propagation Delays Block '+str(height))
    plt.ylabel('Pools Reporting')
    plt.xlabel('mS from slot time')
    img_data = io.BytesIO()
    plt.savefig(img_data, format='png')
    img_data.seek(0)
    #print("saving "+ str(tiptiming[tiphash]["height"]))
    aws.s3_put_object("blockdata/"+str(math.floor(int(height)/1000))+"/F_"+str(hash)+".png",img_data)
    
    return [histogram,median]

maxsyndlength = 360
skipallbefore=0
response = {}
tiptiming={}
syncstatus={}
histogram=None
histogramheight=0
histogramhash=''
starttime = time.time()
fifteencounter=0
maxcountheight = 0
logger.info('process tips')
while True:
    countversion = {}
    countplatform = {}
    hash2slot = {}
    maxcount = 0

    reportingstatus = {
        'lasthash': 0,
        'lasthashparent': 0,
        'lasthashparentepoch': 0,
        'lasthashparentepochslot': 0,
        'version': 0,
        'platform': 0
        }
    count = {}
    heights = {}
    lasthour = []
    priottolasthour = []
    allheights = {}
    syncd = 0
    fifteencounter=fifteencounter+1
    #print("tick" + str(time.time()))
    fakeoverride=0
    tip=fb.getKey(baseNetwork+"/mary_db_sync_status")
    
    if tip is not None and 'block_no' in tip and 'epoch_no' in tip:
        maxblock=int(tip['block_no'])
        
        response['LastEvaluatedKey'] = True
        while 'LastEvaluatedKey' in response:
            if response['LastEvaluatedKey'] == True:
                response = aws.tipstable.scan()
            else:
                response = aws.tipstable.scan(ExclusiveStartKey=response['LastEvaluatedKey'])

            for tipsdata in response['Items']:
                
                if 'lasthash' not in tipsdata or tipsdata['lasthash'] is None or tipsdata['lasthash']=='null':
                    continue
                if 'protocol_minor' not in tipsdata:
                    tipsdata['protocol_minor']=0
                else:
                    tipsdata['protocol_minor']=int(tipsdata['protocol_minor'])
                if 'protocol_major' not in tipsdata:
                    tipsdata['protocol_major']=0
                else:
                    tipsdata['protocol_major']=int(tipsdata['protocol_major'])
                tipsdata['lastupdate']=int(tipsdata['lastupdate'])
                tipsdata['mytip']=int(tipsdata['mytip']) if tipsdata['mytip'] is not None else 0
                if 'at' in tipsdata and 'theoretical' in tipsdata:
                    #print(tipsdata['mytip'], tipsdata['lastslot'], tipsdata['at'],tipsdata['theoretical'])
                    if tipsdata['lasthash'] not in tiptiming:
                        tiptiming[tipsdata['lasthash']]={"protocol_minor":tipsdata['protocol_minor'],"protocol_major":tipsdata['protocol_major'],"height":tipsdata['mytip'], "slot":int(tipsdata['lastslot']), "theoretical":int(tipsdata['theoretical']),"tiptiming":[],"histogram":{},"rawtips":{}}

                        if 'nodevkey' in tipsdata and tipsdata['nodevkey'] is not None and tipsdata['nodevkey'].strip()!='':
                            if tipsdata['nodevkey'] not in vkeys:
                                #print(tipsdata['nodevkey'])
                                te = runcli(f"{cardanocli} stake-pool id --output-format hex --stake-pool-verification-key " + tipsdata['nodevkey'] )
                                if te is not None:
                                    #print(te)
                                    vkeys[tipsdata['nodevkey']]=te
                            
                            if tipsdata['nodevkey'] in vkeys:
                                
                                if vkeys[tipsdata['nodevkey']] not in tickers or tickers[vkeys[tipsdata['nodevkey']]]['lastcheck']<(time.time()-432000):
                                    pg.cur1_create()
                                    pg.cur1_execute("select ticker,pool_name from pools where pool_id=%s",[vkeys[tipsdata['nodevkey']]])
                                    row=pg.cur1_fetchone()
                                    pg.conn_commit()
                                    pg.cur1_close()
                                    if row:
                                        tickers[vkeys[tipsdata['nodevkey']]]={"lastcheck":time.time(),"ticker":row['ticker'].strip() if row['ticker'] is not None else '',"name":row['pool_name']}
                                
                        if "name" not in tiptiming[tipsdata['lasthash']] and 'nodevkey' in tipsdata and tipsdata['nodevkey'] is not None and tipsdata['nodevkey'].strip()!='' and tipsdata['nodevkey'] in vkeys and vkeys[tipsdata['nodevkey']] in tickers:

                            tiptiming[tipsdata['lasthash']]["name"]=tickers[vkeys[tipsdata['nodevkey']]]['name']
                            tiptiming[tipsdata['lasthash']]["ticker"]=tickers[vkeys[tipsdata['nodevkey']]]['ticker']

                        

                        
                    if tipsdata['id'] not in tiptiming[tipsdata['lasthash']]["rawtips"]:
                        tiptiming[tipsdata['lasthash']]["rawtips"][tipsdata['id']] = int(tipsdata['at'])-int(tipsdata['theoretical'])
                        if tipsdata['protocol_major'] is not None and tipsdata['protocol_major']>0 and tiptiming[tipsdata['lasthash']]['protocol_major']==0:
                             tiptiming[tipsdata['lasthash']]['protocol_major']=tipsdata['protocol_major']
                             tiptiming[tipsdata['lasthash']]['protocol_minor']=tipsdata['protocol_minor']

                if fifteencounter==15:
                    if 'lastupdate' in tipsdata:

                        allheights[tipsdata['id']]=tipsdata['mytip']
                        if int(tipsdata['lastupdate']) > (int(datetime.now().timestamp())-3600):
                            lasthour.append(tipsdata)
                            heights[tipsdata['id']]=tipsdata['mytip']
                            if tipsdata['mytip'] not in count:
                                count[tipsdata['mytip']]=0
                            count[tipsdata['mytip']] += 1
                            if 'version' in tipsdata and tipsdata['version']!= ' ':
                                
                                # clean any extraneous spaces and whitespace characters like tabs or returns
                                tipsdata['version'] = tipsdata['version'].replace('\r', '').replace('\n', '').strip()
                                reportingstatus['version'] += 1
                                if tipsdata['version'] not in countversion:
                                    countversion[tipsdata['version']]=0
                                countversion[tipsdata['version']] += 1

                            if 'platform' in tipsdata and tipsdata['platform']!= ' ':
                                tipsdata['platform'] = tipsdata['platform'].replace('\r', '').replace('\n', '').strip()
                                reportingstatus['platform'] += 1
                                if tipsdata['platform'] not in countplatform:
                                    countplatform[tipsdata['platform']]=0
                                countplatform[tipsdata['platform']] += 1

                            if 'lasthash' in tipsdata and tipsdata['lasthash']!= ' ':
                                reportingstatus['lasthash'] += 1
                               

                            if 'lastslot' not in tipsdata or tipsdata['lastslot']== ' ':
                                tipsdata['lastslot'] = None
                            else:
                                hash2slot[tipsdata['lasthash']]=tipsdata['lastslot']

                            

                            if (count[tipsdata['mytip']] > maxcount) or len(count)==1:
                                maxcount = count[tipsdata['mytip']]
                                maxcountheight = tipsdata['mytip']
                        else:
                            priottolasthour.append(tipsdata)



                if 'lastslot' not in tipsdata or tipsdata['lastslot'] is None:
                    continue
                if 'leadervrf' not in tipsdata or tipsdata['leadervrf'] is None:
                    continue
                # we either need a leadervrfproof (if we are prior to babbage) or blockproof/blockvrf (if we are in babbage)
                if ('leadervrfproof' not in tipsdata or tipsdata['leadervrfproof'] is None) and ('blockproof' not in tipsdata or tipsdata['blockproof'] is None) and ('blockvrf' not in tipsdata or tipsdata['blockvrf'] is None):
                # if ('blockproof' not in tipsdata or tipsdata['blockproof'] is None):
                    continue
                if 'mytip' not in tipsdata or tipsdata['mytip'] is None:
                    continue    
                if int(tipsdata['mytip'])<(maxblock-10):
                    continue        
                if 'nodevkey' not in tipsdata or tipsdata['nodevkey'] is None or tipsdata['nodevkey'].strip()=='':
                    continue
                if 'mytip' not in tipsdata or tipsdata['mytip'] is None:
                    continue
                if 'lastparent' not in tipsdata or  tipsdata['lastparent'] is None:
                    continue
                # get pool id from nodevkey
                
                if tipsdata['nodevkey'] not in vkeys:

                    #print(tipsdata['nodevkey'])
                    te = runcli(f"{cardanocli} stake-pool id --output-format hex --stake-pool-verification-key " + tipsdata['nodevkey'] )
                    if te is not None:
                        #print(te)
                        vkeys[tipsdata['nodevkey']]=te
                
                if tipsdata['nodevkey'] not in vkeys:
                    continue
                if vkeys[tipsdata['nodevkey']] not in tickers or tickers[vkeys[tipsdata['nodevkey']]]['lastcheck']<(time.time()-432000):
                    pg.cur1_create()
                    pg.cur1_execute("select ticker,pool_name from pools where pool_id=%s",[vkeys[tipsdata['nodevkey']]])
                    row=pg.cur1_fetchone()
                    pg.conn_commit()
                    pg.cur1_close()
                    if row:
                        tickers[vkeys[tipsdata['nodevkey']]]={"lastcheck":time.time(),"ticker":row['ticker'].strip() if row['ticker'] is not None else '',"name":row['pool_name']}
                    

                
                if int(tipsdata['mytip']) not in heightbattles:
                    heightbattles[int(tipsdata['mytip'])]={}
                if vkeys[tipsdata['nodevkey']] not in heightbattles[int(tipsdata['mytip'])]:
                    heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]]={}
                if tipsdata['lasthash'] not in heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]]:
                    # this is a new block we are adding.  verify the block is valid before adding.

                    # // Quick verification that the block's leaderVrf is not malformed so we can fail-fast for people trying to
                    # // game the system.
                    proofHash = create_string_buffer(libsodium.crypto_vrf_outputbytes())
                    if 'leadervrfproof' in tipsdata and tipsdata['leadervrfproof'] is not None and tipsdata['leadervrfproof'].strip() != '':
                        try:
                            leaderVrfHash = libsodium.crypto_vrf_proof_to_hash(proofHash,binascii.unhexlify(tipsdata['leadervrfproof']))
                        except:
                            leaderVrfHash=None
                        if leaderVrfHash is None or binascii.hexlify(proofHash.raw).decode("utf-8") !=tipsdata['leadervrf']:
                            print("binascii.hexlify(proofHash.raw).decode(utf-8) !=tipsdata['leadervrf']")
                            continue
                        
                    elif 'blockproof' in tipsdata and tipsdata['blockproof'] is not None and tipsdata['blockproof'].strip() != '':
                        
                        #verify block proof
                        proofHash = create_string_buffer(libsodium.crypto_vrf_outputbytes())
                        try:
                            leaderVrfHash = libsodium.crypto_vrf_proof_to_hash(proofHash,binascii.unhexlify(tipsdata['blockproof']))
                        except:
                            leaderVrfHash=None
                        if leaderVrfHash is None or binascii.hexlify(proofHash.raw).decode("utf-8") !=tipsdata['blockvrf']:
                            print("binascii.hexlify(proofHash.raw).decode(utf-8) !=tipsdata['blockvrf']")
                            continue
                        
                        
                    else:
                        continue
                    # block is valid, continue...

                    tipsdata['intleadervrf']  = int.from_bytes(binascii.unhexlify(tipsdata['leadervrf']), byteorder="big", signed=False)
                    tipsdata['intblockvrf']  = int.from_bytes(binascii.unhexlify(tipsdata['blockvrf']), byteorder="big", signed=False)
                    if int(tipsdata['mytip']) not in bvrfmins:
                        bvrfmins[int(tipsdata['mytip'])]=tipsdata['intblockvrf']
                    else:
                        bvrfmins[int(tipsdata['mytip'])]=min(tipsdata['intblockvrf'],bvrfmins[int(tipsdata['mytip'])])

                    if int(tipsdata['mytip']) not in vrfmins:
                        vrfmins[int(tipsdata['mytip'])]=tipsdata['intleadervrf']
                    else:
                        vrfmins[int(tipsdata['mytip'])]=min(tipsdata['intleadervrf'],vrfmins[int(tipsdata['mytip'])])
                    

                    epoch = firstSlotEpoch + math.floor((tipsdata['lastslot'] -  firstSlot) / 432000)
                    
                    heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]={"bvrfwinner":True if bvrfmins[int(tipsdata['mytip'])]==tipsdata['intblockvrf'] else False,"vrfwinner":True if vrfmins[int(tipsdata['mytip'])]==tipsdata['intleadervrf'] else False,"lastparent":tipsdata['lastparent'],"leaderPoolName":tickers[vkeys[tipsdata['nodevkey']]]['name'],"leaderPoolTicker":tickers[vkeys[tipsdata['nodevkey']]]['ticker'],"epoch":epoch,"intblockvrf":tipsdata['intblockvrf'],"intleadervrf":tipsdata['intleadervrf'],"newData":True,"reports":0, "slot":int(tipsdata['lastslot']),"rawtips":{},"histogram":{},"reporter_versions":{}}
                    if tipsdata['lasthash'] in tiptiming:
                        if "protocol_major" in tiptiming[tipsdata['lasthash']]:
                            heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["protocol_major"]=tiptiming[tipsdata['lasthash']]["protocol_major"]
                            heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["protocol_minor"]=tiptiming[tipsdata['lasthash']]["protocol_minor"]
                            
                if 'at' in tipsdata and 'theoretical' in tipsdata:
                    heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["time"]=int(tipsdata['theoretical'])
                    if tipsdata['id'] not in heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["rawtips"]:
                        heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]['newData']=True
                        heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["rawtips"][tipsdata['id']] = int(tipsdata['at'])-int(tipsdata['theoretical'])
                        # here we need to add some analysis detail on the reporters
                        if 'version' in tipsdata and tipsdata['version']!= ' ':
                            fbcompatibletipversion = tipsdata['version'].replace('\r', '').replace('\n', '').replace('/', '').replace('.', ',').replace('#', '_').replace('$', '').replace('[', '{').replace(']', '}').strip()
                            if fbcompatibletipversion not in heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["reporter_versions"]:
                                heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["reporter_versions"][fbcompatibletipversion]=0
                            heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["reporter_versions"][fbcompatibletipversion] += 1
                # now make sure the protocol version we are using for this block is non zero
                if heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["protocol_major"]==0 and "protocol_major" in tiptiming[tipsdata['lasthash']] and tiptiming[tipsdata['lasthash']]["protocol_major"]>0:
                    heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["protocol_major"]=tiptiming[tipsdata['lasthash']]["protocol_major"]
                    heightbattles[int(tipsdata['mytip'])][vkeys[tipsdata['nodevkey']]][tipsdata['lasthash']]["protocol_minor"]=tiptiming[tipsdata['lasthash']]["protocol_minor"]
        
        for tiphash in list(tiptiming):
            # print(str(tiptiming[tiphash]["height"])+ " vs "+ str(maxcountheight - 1))
            if tiptiming[tiphash]["height"] <= skipallbefore:
                del tiptiming[tiphash]
                continue
            if tiptiming[tiphash]["height"] < maxcountheight:

                # we have moved on, go ahead and save the data for the previous block and then delete it from the memory

                for poolid in tiptiming[tiphash]["rawtips"]:
                    tiptiming[tiphash]["tiptiming"].append(tiptiming[tiphash]["rawtips"][poolid])
                tiptiming[tiphash]['histogram']=json.dumps(np.histogram(tiptiming[tiphash]["tiptiming"], bins=50, range=(0,2000)),cls=NumpyArrayEncoder)
                plt.clf()
                plt.hist(tiptiming[tiphash]["tiptiming"], bins=50)
                plt.title('Propagation Delays Block '+str(tiptiming[tiphash]["height"]))
                plt.ylabel('Pools Reporting')
                plt.xlabel('mS from slot time')
                img_data = io.BytesIO()
                plt.savefig(img_data, format='png')
                img_data.seek(0)
                #print("saving "+ str(tiptiming[tiphash]["height"]))
                aws.s3_put_object("blockdata/"+str(math.floor(int(tiptiming[tiphash]["height"])/1000))+"/"+str(tiphash)+".png",img_data)
                #print(tiptiming[tiphash])
                aws.dump_s3(tiptiming[tiphash],"blockdata/"+str(math.floor(int(tiptiming[tiphash]["height"])/1000))+"/"+str(tiphash)+".json")
                histogram=tiptiming[tiphash]['histogram']
                histogramheight=tiptiming[tiphash]['height']
                histogramhash=tiphash
                skipallbefore=tiptiming[tiphash]["height"]
                del tiptiming[tiphash]


        tipsbypoolhash={}
        if fifteencounter==15:
            fifteencounter=0
            # now process all the data
            dist = {}
            for tipsdata in lasthour:
                if tipsdata['mytip'] > (maxcountheight - 3) and tipsdata['mytip'] <= (maxcountheight + 3):
                    syncd += 1
                    if tipsdata['id'] not in syncstatus or syncstatus[tipsdata['id']]==False:
                        
                        
                        syncstatus[tipsdata['id']]=True
                    tipsbypoolhash[tipsdata['id']]=1
                else:
                    tipsbypoolhash[tipsdata['id']]=tipsdata['mytip']
                    if tipsdata['id'] not in syncstatus or syncstatus[tipsdata['id']]==True:
                        
                        syncstatus[tipsdata['id']]=False
                        
                    #update the postgres db for this pool
            

            for tipsdata in priottolasthour:
                #these tips have not updated in over an hour.  set them to 0
                if tipsdata['id'] not in syncstatus or syncstatus[tipsdata['id']]==True:
                    
                    tipsbypoolhash[tipsdata['id']]=0
                    syncstatus[tipsdata['id']]=False

            for key in count:
                if abs(key - maxcountheight)<25:
                    dist[key-maxcountheight]=count[key]
            genesisdata=fb.getKey(baseNetwork+"/recent_block")
            fb.updateFb(baseNetwork+"/stake_pool_columns/heights",tipsbypoolhash)
            






            senddata = {
                "samples": len(lasthour),
                "syncd": syncd,
                "majoritymax": maxcountheight,
                "max": int(max(item['mytip'] for item in lasthour)),
                "histogram":histogram,
                "histogramheight":histogramheight,
                "histogramhash":histogramhash,
                "countversion":countversion,
                "countplatform":countplatform,
                "reportingstatus":reportingstatus,
                "min": int(min(item['mytip'] for item in lasthour)),
                "distribution": count,
            }

            #pprint.pprint(qlresponse)
            syncpacket = {
                "syncd":syncd,
                "countversion":countversion,
                "countplatform":countplatform,
                "reportingstatus":reportingstatus,
                "time": int(datetime.now().timestamp()),
                "majoritymax": maxcountheight,
                "samples": len(lasthour),
                "dist":dist
                }

            syncpacket['currentepoch'] = int(genesisdata['epoch'])
            syncpacket['currentslot'] = int(genesisdata['slot'])
            syncpacket['currentheight'] = int(genesisdata['block'])
            senddata['currentepoch'] = int(genesisdata['epoch'])
            senddata['currentslot'] = int(genesisdata['slot'])
            
            #print(str(genesisdata['block']),str(genesisdata['epoch'])+"."+str(genesisdata['slot']))

            try:
                oldsyncd = aws.load_s3('stats/syncd.json')
            except:
                oldsyncd = []

            oldsyncd.append(syncpacket)
            epochsyncd = []
            itemtomove = {}
            if len(oldsyncd) > maxsyndlength:

                while len(oldsyncd) > maxsyndlength:
                    itemtomove = oldsyncd[0]
                    oldsyncd = oldsyncd[1:]

                try:
                    epochsyncd = aws.load_s3('stats/byepoch/'+str(itemtomove['currentepoch'])+'/syncd.json')
                except:
                    epochsyncd = []

                epochsyncd.append(itemtomove)
                #print("epoch syncd.json")
                #pprint.pprint(epochsyncd)
                aws.dump_s3(epochsyncd,'stats/byepoch/'+str(itemtomove['currentepoch'])+'/syncd.json')
            #print("gen syncd.json")
            #print(oldsyncd)
            aws.dump_s3(oldsyncd,'stats/syncd.json')
            #print("stats.json")
            #pprint.pprint(senddata)
            aws.dump_s3(senddata,'stats/stats.json')
            #print("heights.json")
            #pprint.pprint({"tips": heights, "stats": {"majoritymax": maxcountheight, "syncd":syncd, "samples": len(lasthour)} })
            aws.dump_s3({"tips": heights, "stats": {"majoritymax": maxcountheight, "syncd":syncd, "samples": len(lasthour)} },'stats/heights.json')
            

            
            #convert platform and version into an indexed version thing.
            newcountversion = []
            for key, value in senddata['countversion'].items(): 
                newcountversion.append({"t":key,"v":value})
            senddata['countversion']=newcountversion
            newcountplatform = []
            for key,value in senddata['countplatform'].items():
                newcountplatform.append({"t":key,"v":value})
            senddata['countplatform']=newcountplatform
            
            del senddata["max"]
            del senddata["histogram"]
            del senddata["countversion"]
            del senddata["countplatform"]
            del senddata["reportingstatus"]
            del senddata["min"]
            del senddata["distribution"]
            del senddata["currentslot"]
            #print(senddata)
            fb.setFb(baseNetwork+"/syncdata",senddata)    

        for height in list(heightbattles.keys()):
            
            for poolid in heightbattles[height]:
                for hash in heightbattles[height][poolid]:
                    if len(heightbattles[height])>1:
                        print("height battle:",height)
                        print("pools involved: ")
                        bdlist=[]
                        for minipoolid in heightbattles[height]:
                            for minihash in heightbattles[height][minipoolid]:
                                bdlist.append("blockdata/"+str(math.floor(int(height)/1000))+"/C_"+str(minihash)+".json")
                        heightbattles[height][poolid][hash]['block_data']=bdlist        
                        #print(heightbattles[height].keys())
                        if 'competitive' in heightbattles[height][poolid][hash] and heightbattles[height][poolid][hash]['competitive']==False:
                            heightbattles[height][poolid][hash]['newData']=True
                        heightbattles[height][poolid][hash]['competitive']=True
                    else:
                        if 'competitive' in heightbattles[height][poolid][hash] and heightbattles[height][poolid][hash]['competitive']==True:
                            heightbattles[height][poolid][hash]['newData']=True
                        heightbattles[height][poolid][hash]['competitive']=False
                    if len(heightbattles[height][poolid])>1:
                        print("forker:",poolid)
                        #print(heightbattles[height][poolid])
                        bdlist=[]
                        for minipoolid in heightbattles[height]:
                            for minihash in heightbattles[height][minipoolid]:
                                bdlist.append("blockdata/"+str(math.floor(int(height)/1000))+"/C_"+str(minihash)+".json")
                        heightbattles[height][poolid][hash]['block_data']=bdlist 
                        if 'forker' in heightbattles[height][poolid][hash] and heightbattles[height][poolid][hash]['forker']==False:
                            heightbattles[height][poolid][hash]['newData']=True
                        heightbattles[height][poolid][hash]['forker']=True
                    else:
                        if 'forker' in heightbattles[height][poolid][hash] and heightbattles[height][poolid][hash]['forker']==True:
                            heightbattles[height][poolid][hash]['newData']=True
                        heightbattles[height][poolid][hash]['forker']=False
                    
                    if vrfmins[height] < heightbattles[height][poolid][hash]['intleadervrf']:
                        if heightbattles[height][poolid][hash]['vrfwinner'] == True:
                            heightbattles[height][poolid][hash]['newData']=True
                            heightbattles[height][poolid][hash]['vrfwinner']=False
                    else: #vrfmins[height] >= heightbattles[height][poolid][hash]['intleadervrf']:
                        if heightbattles[height][poolid][hash]['vrfwinner'] == False:
                            heightbattles[height][poolid][hash]['newData']=True
                            heightbattles[height][poolid][hash]['vrfwinner']=True
                    if bvrfmins[height] < heightbattles[height][poolid][hash]['intblockvrf']:
                        if heightbattles[height][poolid][hash]['bvrfwinner'] == True:
                            heightbattles[height][poolid][hash]['newData']=True
                            heightbattles[height][poolid][hash]['bvrfwinner']=False
                    else: #vrfmins[height] >= heightbattles[height][poolid][hash]['intblockvrf']:
                        if heightbattles[height][poolid][hash]['bvrfwinner'] == False:
                            heightbattles[height][poolid][hash]['newData']=True
                            heightbattles[height][poolid][hash]['bvrfwinner']=True

                        
                    
                    if heightbattles[height][poolid][hash]['newData']:
                        
                        [heightbattles[height][poolid][hash]['histogram'],heightbattles[height][poolid][hash]['median']]=saveHistogram(height,hash,heightbattles[height][poolid][hash]['rawtips'])
                        
                        heightbattles[height][poolid][hash]['newData']=False

                        putpack=deepcopy(heightbattles[height][poolid][hash])
                        aws.dump_s3(putpack,"blockdata/"+str(math.floor(int(height)/1000))+"/C_"+str(hash)+".json")
                        if 'rawtips' in putpack:
                            putpack['reports']=len(heightbattles[height][poolid][hash]['rawtips'])
                            del putpack['rawtips']
                        
                        del putpack['newData']
                        
                       
                        
                        fb.getReference(baseNetwork+"/competitive").child(str(height)).child(str(poolid)).child(str(hash)).set(putpack)
                        print("write",height)
            if height<(maxblock-10):
                del heightbattles[height]
                if height in vrfmins:
                    del vrfmins[height]
                if height in bvrfmins:
                    del bvrfmins[height]
                continue
            
            #print(maxblock,len(heightbattles))
            #print(max(heightbattles.keys()))   

    print("tock" + str(time.time()))
    time.sleep(1.0 - (time.time() % 1.0))

#print(slotbattles)