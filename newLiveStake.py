from config import *
from pt_utils import *
import time
from fb_utils import *
from aws_utils import *
from pg_utils import *
import pickle
import zlib

allowChanges=True
sendTelegramBotMessages=True
pg=pg_utils("txInOutProcess")
fb=fb_utils()
aws=aws_utils()
fb.initializePoolCache()
liveStakeFbMirror={}
for pool_id in fb.poolCache:
    if pool_id not in liveStakeFbMirror:
        liveStakeFbMirror[pool_id]={}
    liveStakeFbMirror[pool_id]['live_stake']=fb.poolCache[pool_id]['ls'] if 'ls' in fb.poolCache[pool_id] else 0
    liveStakeFbMirror[pool_id]['poolpledgevalue']=fb.poolCache[pool_id]['ap'] if 'ap' in fb.poolCache[pool_id] else 0

while True: 
    stake_change_updates = 0
    if True:
        filelisting=aws.get_directory_listing_s3('livedata')
        print(filelisting)
        if len(filelisting)==0:
            print("no files in livedata, sleeping 30 seconds")
            time.sleep(30)
            continue

        # sort the filelisting by filename so we strip off the leading path (livedata/ and also strip off the file extension before sorting)
        filelisting.sort(key=lambda x: x.split('/')[-1].split('.')[0])
        print(filelisting)

        

        # load in the most recent file (it should be the lst one in the list)
        liveStakeData=aws.load_s3_object(filelisting[-1])

        # UnCompress (zlib) the  data
        liveStakeData=zlib.decompress(liveStakeData)

        #unpickle the liveStakeData
        liveStakeData=pickle.loads(liveStakeData)

        # save the picle file to disk
        with open('/tmp/liveStakeData.pkl','wb') as f:
            pickle.dump(liveStakeData,f)
        
        # delete all the files on the server because the old ones we will never process as well.
        for file in filelisting:
            aws.delete_s3_object(file)
    # open the pickle file and read it back in
    with open('/tmp/liveStakeData.pkl','rb') as f:
        liveStakeData=pickle.load(f)
        #{"pool_data":pool_data,"treasury":extracted_data['treasury'],"reserves":extracted_data['reserves']}

    items = enumerate(liveStakeData['pool_data'])

    treasury = liveStakeData['treasury']
    reserves = liveStakeData['reserves']

    print("treasury",treasury)
    print("reserves",reserves)

    pg.cur1_execute("select block from sync_status where key='height'")
    row=pg.cur1_fetchone()
    syncdheight=row['block']

    pg.cur1_execute("select block,slot,epoch from blocks where block=%s",[row['block']])
    row=pg.cur1_fetchone()
    current_epoch=row['epoch']

    pg.cur1_execute("select optimal_pool_count, treasury, reward_pot, reserves,decentralisation from epoch_params where epoch=%s",[current_epoch-1])
    row=pg.cur1_fetchone()
    if not row:
        # early in the epoch this may not be valid, or some items may be null.
        print("cannot get epoch params for epoch %s, sleeping 30 seconds" % (current_epoch-1))
        time.sleep(30)
        continue
    else:
        if row['optimal_pool_count'] is None or row['treasury'] is None or row['reserves'] is None or row['decentralisation'] is None:
            print("cannot get epoch params for epoch %s, sleeping 30 seconds" % (current_epoch-1))
            time.sleep(30)
            continue

    optimal_pool_count=int(row['optimal_pool_count'])
    treasury=int(row['treasury'])
    reserves=row['reserves']
    decentralisation=row['decentralisation']

    pg.cur1_execute("select optimal_pool_count, treasury, reward_pot, reserves,decentralisation from epoch_params where epoch=%s",[current_epoch-2])
    row=pg.cur1_fetchone()
    # if we got through the above step this step should be fine..  still we may as well test.
    rewardpot=int(row['reward_pot'])
    if rewardpot is None:
        print("cannot get reward pot for epoch %s, sleeping 30 seconds" % (current_epoch-2))
        time.sleep(30)
        continue
    # 45 billion minus reserves is the total_utxo
    total_utxo=45000000000000000-reserves
    saturationLevel=float((total_utxo+treasury)/optimal_pool_count)

    print("saturationLevel",saturationLevel)


    owners=[]
    pooldetailsbyhashid={}
    poolstakebyhashid={}
    max_live_stake=0
    totalPledge=0
    totalFixedFees=0
    totalMargin=0
    activePools=0
    tickers={}
    poolnames={}
    ts=0
    delegatorsbypool={}
    delegators=0
    totalSaturated=0
    pg.cur1_execute("select pool_id, pool_name, pledge,margin,cost, pool_owners, ticker, live_stake, poolpledgevalue, retired from pools where genesis=false")
    row=pg.cur1_fetchone()
    while row:
        if row['pool_id'] not in tickers:
            tickers[row['pool_id']]=row['ticker'].strip() if row['ticker'] is not None else ''
        if row['pool_id'] not in poolnames:
            poolnames[row['pool_id']]=row['pool_name'].strip() if row['pool_name'] is not None else ''
        pool_id_bin = bytes.fromhex(row['pool_id'])
        if pool_id_bin in liveStakeData['pool_data']:
            if 'ls' in liveStakeData['pool_data'][pool_id_bin]:
                #print("old live stake: ",row['live_stake'])
                #print("new live stake: ",liveStakeData['pool_data'][pool_id_bin]['ls'])
                if liveStakeData['pool_data'][pool_id_bin]['ls']>saturationLevel:
                    totalSaturated=totalSaturated+1
                max_live_stake=max(max_live_stake,liveStakeData['pool_data'][pool_id_bin]['ls'])
                if liveStakeData['pool_data'][pool_id_bin]['ls'] >=1000000 and not row['retired']:
                    activePools=activePools+1
                    totalPledge=totalPledge+row['pledge']
                    totalFixedFees=totalFixedFees+row['cost']
                    totalMargin=totalMargin+row['margin']

        

                if 'dl' in liveStakeData['pool_data'][pool_id_bin]:
                    if row['pool_id'] not in pooldetailsbyhashid:
                        pooldetailsbyhashid[row['pool_id']]={
                            "live_stake":liveStakeData['pool_data'][pool_id_bin]['ls'],
                            "pledge":liveStakeData['pool_data'][pool_id_bin]['cp'],
                            "delegatorCount":len(liveStakeData['pool_data'][pool_id_bin]['dl']),
                            "poolpledgevalue":liveStakeData['pool_data'][pool_id_bin]['ap']
                        }
                  
                    for delegator in liveStakeData['pool_data'][pool_id_bin]['dl']:
                        delegators=delegators+1
                        delegatorhex=delegator.hex()
                        if row['pool_id'] not in delegatorsbypool:
                            delegatorsbypool[row['pool_id']]={"pledges":{},"delegations":{}}
                        delegatorsbypool[row['pool_id']]['delegations'][delegatorhex]=liveStakeData['pool_data'][pool_id_bin]['dl'][delegator]
                        ts=ts+liveStakeData['pool_data'][pool_id_bin]['dl'][delegator]
                        if delegatorhex in row['pool_owners']:
                            delegatorsbypool[row['pool_id']]['pledges'][delegatorhex]=liveStakeData['pool_data'][pool_id_bin]['dl'][delegator]
                        

                poolstakebyhashid[row['pool_id']]=liveStakeData['pool_data'][pool_id_bin]['ls']
                
            del liveStakeData['pool_data'][pool_id_bin]
        else:
            #print("pool_id not found in liveStakeData",row['pool_id'])
            if not row['retired']:
                print("pool not retired something is wrong")
                print(row)
                exit()
            
            

            
        row=pg.cur1_fetchone()
    print("ts: ",ts)
    total_staked=ts
    if len(liveStakeData['pool_data'])>0:
        print("we have unprocessed pool data")
        print(liveStakeData['pool_data'])
        exit()
    
    poolcounter=0
    for pool_id in pooldetailsbyhashid:
        poolcounter+=1
        # if pool_id != '95c4956f7a137f7fe9c72f2e831e6038744b6307d00143b2447e6443':
        #     continue
        if pool_id is not None and (pool_id not in liveStakeFbMirror or liveStakeFbMirror[pool_id]['live_stake']!=pooldetailsbyhashid[pool_id]['live_stake']):
            
            if pool_id not in liveStakeFbMirror or liveStakeFbMirror[pool_id]['live_stake']!=pooldetailsbyhashid[pool_id]['live_stake']:
                print({"type":"stake_change","data":  {"pool":pool_id, "ticker":tickers[pool_id] if pool_id in tickers else '',"old_stake":liveStakeFbMirror[pool_id]['live_stake'] if pool_id in liveStakeFbMirror else 0, "livestake":pooldetailsbyhashid[pool_id]['live_stake']}})
                stake_change_updates+=1
                print("stake change updates: ",stake_change_updates)
                print("poolcounter: ",poolcounter)
                if allowChanges and sendTelegramBotMessages:
                    aws.awsbroadcast({"type":"stake_change","data":  {"pool":pool_id, "ticker":tickers[pool_id] if pool_id in tickers else '',"old_stake":liveStakeFbMirror[pool_id]['live_stake'] if pool_id in liveStakeFbMirror else 0, "livestake":pooldetailsbyhashid[pool_id]['live_stake']}})
                    
            if pool_id in delegatorsbypool:
                remapped_delegatorsbypool={'delegatorHash':None,'delegators':[]}
                for delegator in delegatorsbypool[pool_id]["delegations"]:
                    
                    if delegatorsbypool[pool_id]["delegations"][delegator]>=1e6:
                        ph=""
                        ah=[]
                        pg.cur3_execute("select package from delegator_loyalty where stake_key=%s",[delegator])
                        row3=pg.cur3_fetchone()
                        if row3:
                            dl=row3['package']
                        else:
                            dl=None
                        remapped_delegatorsbypool['delegators'].append({"dl":dl,"ph":ph,"ah":ah,"k":delegator,"v":delegatorsbypool[pool_id]["delegations"][delegator]})
                delegatorhash = myHash(json.dumps(remapped_delegatorsbypool['delegators']))
                remapped_delegatorsbypool['delegatorHash']=delegatorhash
                poolstatsupdate={
                    'delegatorHash':delegatorhash,
                    'delegatorCount':len(delegatorsbypool[pool_id]["delegations"])}
               
                
                if allowChanges:
                    aws.dump_s3(remapped_delegatorsbypool,f"live_delegators_by_pool/{pool_id}.json")
                print(f"update pools set delegator_count=%s where pool_id=%s",[poolstatsupdate['delegatorCount'],pool_id])
                
                if allowChanges:
                    pg.cur3_execute("update pools set delegator_count=%s where pool_id=%s",[poolstatsupdate['delegatorCount'],pool_id]) 
                    pg.conn_commit() 
            else:
                poolstatsupdate={'delegatorHash':34351230,'delegatorCount':0}
                print("aws dump: ")
                print({'delegatorhash':34351230,'delegators':[]},f"live_delegators_by_pool/{pool_id}.json")
                if allowChanges:
                    aws.dump_s3({'delegatorhash':34351230,'delegators':[]},f"live_delegators_by_pool/{pool_id}.json")
            print(poolstatsupdate)
            print({"ls":pooldetailsbyhashid[pool_id]['live_stake']})
           
            if allowChanges:
               fb.writeFb(baseNetwork+"/pool_stats/"+str(pool_id),poolstatsupdate)
               fb.writeFb(baseNetwork+"/stake_pools/"+str(pool_id),{"ls":pooldetailsbyhashid[pool_id]['live_stake']})
            
            if pool_id not in liveStakeFbMirror:
                liveStakeFbMirror[pool_id]={}
            liveStakeFbMirror[pool_id]['live_stake']=pooldetailsbyhashid[pool_id]['live_stake']
    print("done gathering pool details")
    if allowChanges:
        #print(poolstakebyhashid)
        fb.updateFb(baseNetwork+"/stake_pool_columns/livestake",poolstakebyhashid)
    else:
        print("writing stake pool columns livestake")

    
    if allowChanges and baseNetwork=="Mainnet":
        print("writing stake pool columns to s3")
        aws.dump_s3(delegatorsbypool,"stats/livestake.json")  


    totalHonoredPledge=0
    for pool_id in delegatorsbypool:
        # if pool_id != '95c4956f7a137f7fe9c72f2e831e6038744b6307d00143b2447e6443':
        #     continue
        poolpledgevalue=0
        
        
        for pledge_key in delegatorsbypool[pool_id]['pledges']:
            poolpledgevalue=poolpledgevalue+delegatorsbypool[pool_id]['pledges'][pledge_key]
        if pool_id not in liveStakeFbMirror:
            liveStakeFbMirror[pool_id]={'poolpledgevalue':0}
        if 'poolpledgevalue' not in liveStakeFbMirror[pool_id]:
            liveStakeFbMirror[pool_id]['poolpledgevalue']=0
        if 'poolpledgevalue' not in liveStakeFbMirror[pool_id] or liveStakeFbMirror[pool_id]['poolpledgevalue']!=poolpledgevalue:
            print(baseNetwork+"/stake_pools/"+str(pool_id),{"ap":poolpledgevalue})
            
            if allowChanges:
                print("writing poolpledgevalue")
                fb.writeFb(baseNetwork+"/stake_pools/"+str(pool_id),{"ap":poolpledgevalue})
            else:
                print("poolpledgevalue: ",pool_id,liveStakeFbMirror[pool_id]['poolpledgevalue'],"->",poolpledgevalue)
            
            liveStakeFbMirror[pool_id]['poolpledgevalue']=poolpledgevalue
            print("update pools set poolpledgevalue=%s where pool_id=%s returning pledge",[poolpledgevalue,pool_id])
            if allowChanges:
                pg.cur1_execute("update pools set poolpledgevalue=%s where pool_id=%s returning pledge",[poolpledgevalue,pool_id])
                row=pg.cur1_fetchone()
                if row:
                    if 'pledge' in row:
                        pooldetailsbyhashid[pool_id]['pledge']=row['pledge']
                        print("updating pledge")
                pg.conn_commit()
        if pool_id in pooldetailsbyhashid:
            if poolpledgevalue>=pooldetailsbyhashid[pool_id]['pledge']:
                print(pool_id," Meets Pledge with ",poolpledgevalue, " vs ",pooldetailsbyhashid[pool_id]['pledge'])
                totalHonoredPledge=totalHonoredPledge+pooldetailsbyhashid[pool_id]['pledge']
            else:
                print(pool_id," FAILS ",poolpledgevalue, " vs ",pooldetailsbyhashid[pool_id]['pledge'])
        else:
            print("we don't have this pool id in live delegators: ",pool_id)
    fb.writeBatch()
    print("totalHonoredPledge",totalHonoredPledge)
    print("max_live_stake",max_live_stake)
    print("delegators",delegators)
    #pooldetailsbyhashid[row['pool_id']]['delegatorCount']
    #delegatorsbypool[row['pool_id']]['delegations']
    print("totalPledge",totalPledge)
    print("totalFixedFees",totalFixedFees)
    print("totalMargin",totalMargin)
    print("total_utxo",total_utxo)
    print("totalSaturated",totalSaturated)

    print("optimal_pool_count",optimal_pool_count)
    print("treasury",treasury)
    print("rewardpot",rewardpot)
    print("reserves",reserves)
    print("decentralisation",decentralisation)

    avg_variable_fee=round((totalMargin/activePools),2)
    avg_fixed_fee=int((totalFixedFees/activePools))
    avg_pledge=int((totalPledge/activePools))
    ecowrite={"current_epoch":current_epoch,"total_utxo":total_utxo, "saturation":(saturationLevel),"saturated":totalSaturated,"desiredPoolNumber":optimal_pool_count, "decentralizationLevel":((1-decentralisation)*100),"treasury":treasury, "rewardpot":rewardpot, "reserves":reserves,"totalStaked":total_staked,"maxLiveStake":max_live_stake,"totalHonoredPledge":totalHonoredPledge,"activePools":activePools,"delegators":delegators,"totalPledge":totalPledge,"averageVariableFee":avg_variable_fee,"averageFixedFee":avg_fixed_fee,"averagePledge":avg_pledge}
    statswrite={"total_utxo":total_utxo, "saturation":(saturationLevel),"saturated":totalSaturated,"desiredPoolNumber":optimal_pool_count, "decentralizationLevel":((1-decentralisation)*100),"treasury":treasury, "rewardpot":rewardpot, "reserves":reserves,"totalStaked":total_staked,"maxLiveStake":max_live_stake,"totalHonoredPledge":totalHonoredPledge,"activePools":activePools,"delegators":delegators,"totalPledge":totalPledge,"averageVariableFee":avg_variable_fee,"averageFixedFee":avg_fixed_fee,"averagePledge":avg_pledge}
    writetime=(int(float(time.time()))) #stats_history_write_point
    print(ecowrite)
    print(statswrite)
    if allowChanges:
        fb.writeFb(baseNetwork+"/circulating_supply",{(current_epoch+2):total_utxo})
        fb.writeFb(baseNetwork+"/total_active_stake",{(current_epoch+2):total_staked})
    else:
        print("total_utxo",total_utxo)
        print("total_staked",total_staked)
    

    todayprices=ptGetPrices()
    if todayprices is None:
        print("##################################################################################")
        print("could not get prices for some reason - check api")
        print("##################################################################################")
        ecowrite["prices"]=None
    else:
        ecowrite["prices"]=todayprices['cardano']
    print(ecowrite["prices"])
    if allowChanges:
        pg.cur1_execute("""insert into stats_history (timestamp,epoch,total_utxo,saturation,saturated,optimal_pools,treasury,reserves,rewardpot,total_staked,active_pools,delegators,total_pledge, avg_variable_fee, avg_fixed_fee, avg_pledge,prices) 
        values( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT stats_history_pkey DO UPDATE set total_utxo=%s,saturation=%s,saturated=%s,optimal_pools=%s,treasury=%s,reserves=%s,rewardpot=%s,total_staked=%s,active_pools=%s,delegators=%s,total_pledge=%s, avg_variable_fee=%s, avg_fixed_fee=%s, avg_pledge=%s,prices=%s""",[
            writetime,current_epoch,total_utxo,saturationLevel,totalSaturated,optimal_pool_count,treasury,reserves,rewardpot,total_staked,activePools,delegators,totalPledge,avg_variable_fee,avg_fixed_fee,avg_pledge,Json(ecowrite["prices"]),total_utxo,saturationLevel,totalSaturated,optimal_pool_count,treasury,reserves,rewardpot,total_staked,activePools,delegators,totalPledge,avg_variable_fee,avg_fixed_fee,avg_pledge,Json(ecowrite["prices"])
        ])
        pg.conn_commit()
    
    writetime=str(int(float(time.time())))
    
    if allowChanges:
        print("writing ecowrite")
        fb.updateFb(baseNetwork+"/ecosystem",ecowrite)
    else:
        print("fake ecowrite")
    
    fb.writeBatch()
   
    
    print("waiting 30 seconds until we check for a new ledger state")
    time.sleep(30)