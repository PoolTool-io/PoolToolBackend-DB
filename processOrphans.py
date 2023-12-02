from config import *
from fb_utils import *
from log_utils import *
import time
fb=fb_utils()
import time
from datetime import datetime
from pg_utils import pg_utils
from aws_utils import aws_utils
aws=aws_utils()
pg=pg_utils("ProcessOrphans")

pool_protocol_versions={}

while True:
    updatestats=False
    lastkey=fb.getKey(baseNetwork+"/mary_db_sync_status")
    #########################################################################################################################
    print("process orphans")
    logger.info('process orphans')
    #########################################################################################################################
    if "deleted_block" not in lastkey:
        lastkey["deleted_block"]=0


    processblocks = fb.getReference(baseNetwork+"/competitive").order_by_key().start_at(str(int(lastkey['deleted_block'])+1)).end_at(str(lastkey['block_no']-1)).get()
    for height in processblocks:
        block=None
        forker=False
        competitive=False
        slotBattle=False
        if len(processblocks[height])>1:
            competitive=True
            print(competitive)
        slotlist=[]
        for poolid in processblocks[height]:
            if poolid=="classification":
                continue
            for hash in processblocks[height][poolid]:
                if processblocks[height][poolid][hash]['slot'] not in slotlist:
                    slotlist.append(processblocks[height][poolid][hash]['slot'])
            if len(slotlist)>1:
                slotBattle=False
            else:
                slotBattle=True
        for poolid in processblocks[height]:
            if poolid=="classification":
                continue
            if len(processblocks[height][poolid])>1:
                forker=True
                print(forker)
            # check for height vs slot battle
            
            for hash in processblocks[height][poolid]:
                if competitive or forker:
                    # test which block is actually adopted on chain.  mark the one on chain and archived the ones NOT on chain to each pool
                    if 'epoch' in processblocks[height][poolid][hash]:
                        print("found one to process",str(height))
                        if block is None:
                            block = fb.getReference(baseNetwork+"/blocks").child(str(processblocks[height][poolid][hash]['epoch'])).child(str(height)).get()
                        
                        if block['hash']==hash:
                            fb.getReference(baseNetwork+"/pool_blocks").child(str(poolid)).child(str(processblocks[height][poolid][hash]['epoch'])).child(str(height)).update({"classification":"forker" if forker else "competitive","slotBattle":slotBattle})
                            fb.getReference(baseNetwork+"/blocks").child(str(processblocks[height][poolid][hash]['epoch'])).child(str(height)).update({"classification":"forker" if forker else "competitive","slotBattle":slotBattle})
                            fb.getReference(baseNetwork+"/competitive").child(str(height)).child(str(poolid)).child(str(hash)).update({"chained":True,"slotBattle":slotBattle})
                            fb.getReference(baseNetwork+"/competitive").child(str(height)).update({"classification":"forker" if forker else "competitive"})
                            print("marked chained block as forker or competitive")
                            
                        else:
                            print("none block")
                            
                            
                            entry = {
                                "block":height,
                                "epoch":processblocks[height][poolid][hash]['epoch'],
                                "hash":hash,
                                "leaderPoolId":poolid,
                                "leaderPoolName":processblocks[height][poolid][hash]["leaderPoolName"] if "leaderPoolName" in processblocks[height][poolid][hash] else '',
                                "leaderPoolTicker":processblocks[height][poolid][hash]["leaderPoolTicker"] if "leaderPoolTicker" in processblocks[height][poolid][hash]  else '',
                                "output":0,
                                "size":0,
                                "slot":((processblocks[height][poolid][hash]['slot']-172800) % 432000) ,
                                "time":int(processblocks[height][poolid][hash]['time']),
                                "transactions": 0,
                                "classification":"forker" if forker else "competitive",
                                "slotBattle":slotBattle
                            }
                
                            fb.getReference(baseNetwork+"/deleted_blocks/"+str(processblocks[height][poolid][hash]['epoch'])+"/"+str(height)).set(entry)
                            fb.getReference(baseNetwork+"/pool_deleted_blocks/"+poolid+"/"+str(processblocks[height][poolid][hash]['epoch'])+"/"+str(height)).set(entry)
                            fb.getReference(baseNetwork+"/competitive").child(str(height)).child(str(poolid)).child(str(hash)).update({"chained":False,"slotBattle":slotBattle})
        lastdeletedblock=height
        
        if lastdeletedblock is not None:
            logger.info('lastdeletedblock: %s',lastdeletedblock)
            fb.getReference(baseNetwork+"/mary_db_sync_status").update({"deleted_block":lastdeletedblock})
            pg.cur1_execute("update sync_status set block=%s where key='deleted_block'",[lastdeletedblock])
            pg.conn_commit()
            lastkey["deleted_block"]=lastdeletedblock
    
    # here we move things to competitive blocks area
    pg.cur1_execute("select block from sync_status where key='competitive'")
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row:
        process_block=row['block']+1
    else:
        process_block=6621461

    pg.cur1_execute("select block from sync_status where key='deleted_block'")
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row:
        end_at=row['block']
    #competitive/height/pool_id/block_hash
    while process_block<=end_at:
        
        print(process_block,end_at)
        competitive = fb.getReference(f"{baseNetwork}/competitive/{process_block}").get()
        
        classification=''
        if competitive is not None:
            if 'classification' in competitive:
                classification=competitive['classification']
            for pool_id in competitive:
                if pool_id=='classification':
                    continue
                for block_hash in competitive[pool_id]:
                    
                    writedata=competitive[pool_id][block_hash]
                    if 'protocol_major' not in writedata:
                        writedata['protocol_major']=0
                    if 'protocol_minor' not in writedata:
                        writedata['protocol_minor']=0
                    pool_protocol=str(writedata['protocol_major'])+"."+str(writedata['protocol_minor'])
                    if pool_id not in pool_protocol_versions or pool_protocol_versions[pool_id]!=pool_protocol:
                        pool_protocol_versions[pool_id]=pool_protocol
                        print(pool_id,pool_protocol)
                        pg.cur1_execute("update pools set protocol_major=%s, protocol_minor=%s where pool_id=%s",[writedata['protocol_major'],writedata['protocol_minor'],pool_id])
                        pg.conn_commit()
                        updatestats=True

                    writedata['classification']=classification
                    if 'chained' not in writedata:
                        pg.cur1_execute("select block from blocks where hash=%s and block=%s",[block_hash,process_block])
                        row=pg.cur1_fetchone()
                        pg.conn_commit()
                        if row:
                            writedata['chained']=True
                        else:
                            writedata['chained']=False
                    writedata['pool_id']=pool_id
                    writedata['block_hash']=block_hash
                    writedata['block_no']=process_block
                    if 'competitive' not in writedata:
                        writedata['competitive']=False
                    if 'forker' not in writedata:
                        writedata['forker']=False
                    if 'epoch' not in writedata or 'time' not in writedata:
                        pg.cur1_execute("select epoch,timestamp from blocks where block=%s",[process_block])
                        row=pg.cur1_fetchone()
                        pg.conn_commit()
                        if row:
                            writedata['epoch']=int(row['epoch'])
                            writedata['time']=int(row['timestamp'])
                        else:
                            print("can't find block")
                            exit()
                    if 'reports' not in writedata:
                        writedata['reports']=None
                    if 'leaderPoolTicker' not in writedata or 'leaderPoolName' not in writedata:
                        pg.cur1_execute("select ticker, pool_name from pools where pool_id=%s",[pool_id])
                        row=pg.cur1_fetchone()
                        pg.conn_commit()
                        if row:
                            writedata['leaderPoolTicker']=row['ticker']
                            writedata['leaderPoolName']=row['pool_name']
                        else:
                            writedata['leaderPoolTicker']=''
                            writedata['leaderPoolName']=''
                    if 'lastparent' not in writedata:
                        pg.cur1_execute("select hash from blocks where block=%s",[process_block-1])
                        row=pg.cur1_fetchone()
                        pg.conn_commit()
                        if row:
                            writedata['lastparent']=row['hash']
                        else:
                            print("can't find block")
                            exit()
                    if 'median' not in writedata:
                        writedata['median']=None
                    if 'vrfwinner' not in writedata:
                        writedata['vrfwinner']=False
                    print("writing")
                    pg.cur1_execute("""insert into competitive_blocks(block_no, epoch, timestamp, pool_id, block_hash, competitive, forker,block_slot_no, reports, pool_ticker, pool_name, vrf_winner,lastparent,median,chained,classification,protocol_major,protocol_minor)
                    values(%(block_no)s,%(epoch)s,%(time)s,%(pool_id)s,%(block_hash)s,%(competitive)s,%(forker)s,%(slot)s,%(reports)s,%(leaderPoolTicker)s,%(leaderPoolName)s,%(vrfwinner)s,%(lastparent)s,%(median)s,%(chained)s,%(classification)s,%(protocol_major)s,%(protocol_minor)s)""",writedata)
                    pg.conn_commit()
                    
        pg.cur1_execute("update sync_status set block=%s where key='competitive'",[process_block])
        pg.conn_commit()
        process_block=process_block+1
    
    if updatestats:

        updatestats=False

    # summarize battle data
    block_slot_no = (int(datetime.now().timestamp()) - 1596059091) + 4492800 #%432000
    print(block_slot_no)

    pg.cur1_execute("""select count(block_no) as battles, 
                        count(block_no) filter (where forker=true) as forkerblocks,
                        count(block_no) filter (where slot_battle=true and forker=false) as slotbattleblocks,
                        count(block_no) filter (where slot_battle=false and forker=false) as heightbattleblocks from 
                        (select block_no, bool_or(competitive) as competitive, bool_or(forker) as forker, 
                        case when  min(block_slot_no)=max(block_slot_no) then true else false end as slot_battle from competitive_blocks 
                        where (competitive=true or forker=true) and block_slot_no>%s group by block_no ) as a """,[block_slot_no-(3600*6)])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    battledata={"recentBattles":int(row['battles']),"recentForkers":int(row['forkerblocks']),"recentSlotBattles":int(row['slotbattleblocks']),"recentHeightBattles":int(row['heightbattleblocks'])}
    fb.updateFb(baseNetwork + "/ecosystem",{"battleData":battledata} )
    battledata["time"]= int(datetime.now().timestamp())
    genesisdata=fb.getKey(baseNetwork+"/recent_block")
    battledata['currentepoch'] = int(genesisdata['epoch'])
    battledata['currentslot'] = int(genesisdata['slot'])
    battledata['currentheight'] = int(genesisdata['block'])
    try:
        oldbattledata = aws.load_s3('stats/battledata.json')
    except:
        oldbattledata = []
    maxbattledatalength=3600*2/20



    oldbattledata.append(battledata)
    epochbattledata = []
    itemtomove = {}
    if len(oldbattledata) > maxbattledatalength:

        while len(oldbattledata) > maxbattledatalength:
            itemtomove = oldbattledata[0]
            oldbattledata = oldbattledata[1:]

        try:
            epochbattledata = aws.load_s3('stats/byepoch/'+str(itemtomove['currentepoch'])+'/battledata.json')
        except:
            epochbattledata = []

        epochbattledata.append(itemtomove)
        aws.dump_s3(epochbattledata,'stats/byepoch/'+str(itemtomove['currentepoch'])+'/battledata.json')
    aws.dump_s3(oldbattledata,'stats/battledata.json')  


    print("updating stats")
    pg.cur1_execute("select protocol_major, protocol_minor, sum(live_stake) as stake, count(*) as qty from pools where protocol_major is not null and protocol_minor is not null group by protocol_major, protocol_minor")
    row=pg.cur1_fetchone()
    package={}
    while row:
        protocolver=str(int(row['protocol_major']))+"_"+str(int(row['protocol_minor']))
        if protocolver=="7_2":
            protocolver="7_0" 
        if protocolver in package:
            package[protocolver]["qty"]+=int(row['qty'])
            package[protocolver]["stake"]+=int(row['stake'])
        else:
            package[protocolver]={"ver":protocolver,"qty":int(row['qty']),"stake":int(row['stake'])}
        row=pg.cur1_fetchone()
    pg.conn_commit()
    
    pg.cur1_execute("select protocol_major, protocol_minor, count(*) as blocks from competitive_blocks where chained=true and epoch=%s group by protocol_major, protocol_minor ",[int(genesisdata['epoch'])])  
    row=pg.cur1_fetchone()
    while row:
        if row['protocol_major']!= None and row['protocol_minor']!=None:
            protocolver=str(int(row['protocol_major'] if row['protocol_major'] is not None else 0))+"_"+str(int(row['protocol_minor'] if row['protocol_minor'] is not None else 0))
            if protocolver=="7_2":
                protocolver="7_0" 
            
            if protocolver not in package:
                package[protocolver]={"ver":protocolver,"blocks":int(row['blocks'])}
            else:
                if 'blocks' in package[protocolver]:
                    package[protocolver]['blocks']+=int(row['blocks'])
                else:
                    package[protocolver]['blocks']=int(row['blocks'])
        row=pg.cur1_fetchone()
    pg.conn_commit()

    pg.cur1_execute("""select protocol_major, protocol_minor, 
    count(block_no) filter (where timestamp/1000>extract(epoch from now())-3600) as blocks1hr,
    count(block_no) filter (where timestamp/1000>extract(epoch from now())-3600*6) as blocks6hr,
    count(block_no) filter (where timestamp/1000>extract(epoch from now())-3600*12) as blocks12hr,
    count(block_no) filter (where timestamp/1000>extract(epoch from now())-3600*24) as blocks24hr 
    from competitive_blocks where chained=true group by protocol_major, protocol_minor""")
    row=pg.cur1_fetchone()
    while row:
        if row['protocol_major']!= None and row['protocol_minor']!=None:
            protocolver=str(int(row['protocol_major'] if row['protocol_major'] is not None else 0))+"_"+str(int(row['protocol_minor'] if row['protocol_minor'] is not None else 0))
            print(protocolver)
            if protocolver=="7_2":
                protocolver="7_0" 
            if protocolver not in package:
                package[protocolver]={"blocks1hr":int(row['blocks1hr']),"blocks6hr":int(row['blocks6hr']),"blocks12hr":int(row['blocks12hr']),"blocks24hr":int(row['blocks24hr'])}
                # package[protocolver]={"blocks1hr":int(row['blocks1hr']),"blocks6hr":int(row['blocks6hr']),"blocks12hr":int(row['blocks12hr']),"blocks24hr":int(row['blocks24hr'])}
            else:
                if 'blocks1hr' in package[protocolver]:
                    package[protocolver]['blocks1hr']+=int(row['blocks1hr'])
                    package[protocolver]['blocks6hr']+=int(row['blocks6hr'])
                    package[protocolver]['blocks12hr']+=int(row['blocks12hr'])
                    package[protocolver]['blocks24hr']+=int(row['blocks24hr'])
                else:
                    package[protocolver]['blocks1hr']=int(row['blocks1hr'])
                    package[protocolver]['blocks6hr']=int(row['blocks6hr'])
                    package[protocolver]['blocks12hr']=int(row['blocks12hr'])
                    package[protocolver]['blocks24hr']=int(row['blocks24hr'])
        row=pg.cur1_fetchone() 
    pg.conn_commit()
    print(package)
    fb.updateFb(baseNetwork+"/ecosystem/",{"protocolVersions":package})

    
    print("updating forkers")
    # summarize forkers
    pg.cur1_execute("""select epoch,block_no,  
    array_agg(DISTINCT pool_id) as pool_ids,array_agg(DISTINCT pool_ticker) as pool_tickers from competitive_blocks 
    where (forker=true) and block_slot_no>%s group by epoch,block_no""",[block_slot_no-(3600*24*10)])

    row=pg.cur1_fetchone()
    forkers={}
    while row:
        forkers[int(row['block_no'])]={"epoch":int(row['epoch']),"block_no":int(row['block_no']),"pool_id":row['pool_ids'][0],"pool_ticker":row['pool_tickers'][0]}
        row=pg.cur1_fetchone()
    pg.conn_commit()
    fb.updateFb(baseNetwork + "/ecosystem",{"recentForkers":forkers} )
    
    logger.info('pausing for 20 seconds')
    print("pausing for 20 seconds")
    time.sleep(20)
    pause=True
    while pause:
        pg.cur1_execute("select bool from sync_status where key='pause_orphans'")
        row=pg.cur1_fetchone()
        pg.conn_commit()
        if row:
            if row['bool']:
                pause=True
                logger.info('pausing orphans processing')
                print("pausing orphans processing")
                time.sleep(30)
            else:
                pause=False
        else:
            pause=True
    logger.info('continuing orphans processing')
    print("continuing orphans processing")