from pt_utils import * 
from aws_utils import *
from config import *
from fb_utils import *
from pg_utils import *
pg=pg_utils("Loyalty")
fb=fb_utils()
aws=aws_utils()

def processEpochLoyaltySummary(epoch):
    #to process after wait stake is done
    #pg.cur1_execute("select stake_key from (select stake_key, count(distinct delegated_to_pool) as poolchanges from stake_history where  (epoch=%s or epoch=%s) group by stake_key) a where a.poolchanges>1",[epoch,epoch-1])
    thevars={"epoch":epoch,"epochm1":epoch-1}
    pg.cur1_execute("select stake_key,stake_amount,poolswitch,max_epoch from (select stake_key, max(amount) as stake_amount,max(epoch) as max_epoch, count(distinct delegated_to_pool) as poolchanges,array_agg(delegated_to_pool || delegated_to_ticker  ORDER BY epoch desc) as poolswitch from stake_history where (epoch=%(epoch)s or epoch=%(epochm1)s) group by stake_key) a where a.poolchanges>1 or array_length(poolswitch,1)=1 ",thevars)
    row=pg.cur1_fetchone()
    package={}
    while row:
        package[row['stake_key']]={"stake_amount":int(row['stake_amount'])}
        if len(row['poolswitch'])==2:
            
            package[row['stake_key']]["new_pool"]=row['poolswitch'][0] if row['poolswitch'][0] is not None and row['poolswitch'][0][0:56].strip()!='None' else None
            package[row['stake_key']]["old_pool"]=row['poolswitch'][1] if row['poolswitch'][1] is not None and row['poolswitch'][1][0:56].strip()!='None' else None
        else:
            if row['max_epoch']==epoch:
                package[row['stake_key']]["new_pool"]=row['poolswitch'][0] if row['poolswitch'][0] is not None and row['poolswitch'][0][0:56].strip()!='None' else None
                package[row['stake_key']]["old_pool"]=None
            else:
                package[row['stake_key']]["new_pool"]=None
                package[row['stake_key']]["old_pool"]=row['poolswitch'][0] if row['poolswitch'][0] is not None and row['poolswitch'][0][0:56].strip()!='None' else None
        row=pg.cur1_fetchone()

    pg.conn_commit()
    aws.dump_s3(package,"stats/loyalty/"+str(epoch)+"/summary.json")
    return package

def process3EpochPoolMigrations(epoch):
    # load all 3 summaries, and combine together
    total_stake_moved=0
    stake_gained_by_pool={}
    stake_gained_from_by_pool={}

    stake_lost_by_pool={}
    stake_lost_to_by_pool={}
    for targetepoch in range(epoch-3,epoch):
        package=aws.load_s3("stats/loyalty/"+str(targetepoch)+"/summary.json")
        for stake_key in package:
            if "new_pool" in package[stake_key] and package[stake_key]["new_pool"] is not None and "old_pool" in package[stake_key] and package[stake_key]["old_pool"] is not None:
                total_stake_moved+=int(package[stake_key]['stake_amount'])
                new_pool_id=package[stake_key]["new_pool"][0:56]
                new_pool_ticker=package[stake_key]["new_pool"][56:]
                old_pool_id=package[stake_key]["old_pool"][0:56]
                old_pool_ticker=package[stake_key]["old_pool"][56:]
                
                # this is indivudual pool stats
                if new_pool_id not in stake_gained_from_by_pool:
                    stake_gained_from_by_pool[new_pool_id]={}
                if old_pool_id not in stake_gained_from_by_pool[new_pool_id]:
                    stake_gained_from_by_pool[new_pool_id][old_pool_id]={}
                stake_gained_from_by_pool[new_pool_id][old_pool_id][stake_key]={"pool_id":old_pool_id,"epoch":targetepoch,"pool_ticker":old_pool_ticker,"stake_key":stake_key,"stake":int(package[stake_key]['stake_amount'])}

                
                
                # this is indivudual pool stats
                if old_pool_id not in stake_lost_to_by_pool:
                    stake_lost_to_by_pool[old_pool_id]={}
                if new_pool_id not in stake_lost_to_by_pool[old_pool_id]:
                    stake_lost_to_by_pool[old_pool_id][new_pool_id]={}
                stake_lost_to_by_pool[old_pool_id][new_pool_id][stake_key]={"pool_id":new_pool_id,"epoch":targetepoch,"pool_ticker":new_pool_ticker,"stake_key":stake_key,"stake":int(package[stake_key]['stake_amount'])}
                

                # this is for overall gain/loss across all pools
                if new_pool_id not in stake_gained_by_pool:
                    stake_gained_by_pool[new_pool_id]={"pool_id":new_pool_id,"pool_ticker":new_pool_ticker,"delegators":1,"stake":int(package[stake_key]['stake_amount'])}
                else:
                    stake_gained_by_pool[new_pool_id]["delegators"]+=1
                    stake_gained_by_pool[new_pool_id]["stake"]+=int(package[stake_key]['stake_amount'])

                # this is for overall gain/loss across all pools
                if old_pool_id not in stake_lost_by_pool:
                    stake_lost_by_pool[old_pool_id]={"pool_id":old_pool_id,"pool_ticker":old_pool_ticker,"delegators":1,"stake":int(package[stake_key]['stake_amount'])}
                else:
                    stake_lost_by_pool[old_pool_id]["delegators"]+=1
                    stake_lost_by_pool[old_pool_id]["stake"]+=int(package[stake_key]['stake_amount'])

            elif "new_pool" in package[stake_key] and package[stake_key]["new_pool"] is None and "old_pool" in package[stake_key] and package[stake_key]["old_pool"] is not None:
                new_pool_id="None"
                new_pool_ticker=""
                old_pool_id=package[stake_key]["old_pool"][0:56]
                old_pool_ticker=package[stake_key]["old_pool"][56:]
                
                # this is indivudual pool stats
                if old_pool_id not in stake_lost_to_by_pool:
                    stake_lost_to_by_pool[old_pool_id]={}
                if new_pool_id not in stake_lost_to_by_pool[old_pool_id]:
                    stake_lost_to_by_pool[old_pool_id][new_pool_id]={}
                stake_lost_to_by_pool[old_pool_id][new_pool_id][stake_key]={"pool_id":new_pool_id,"epoch":targetepoch,"pool_ticker":new_pool_ticker,"stake_key":stake_key,"stake":int(package[stake_key]['stake_amount'])}
                
                
                # this is for overall gain/loss across all pools
                if old_pool_id not in stake_lost_by_pool:
                    stake_lost_by_pool[old_pool_id]={"pool_id":old_pool_id,"pool_ticker":old_pool_ticker,"delegators":1,"stake":int(package[stake_key]['stake_amount'])}
                else:
                    stake_lost_by_pool[old_pool_id]["delegators"]+=1
                    stake_lost_by_pool[old_pool_id]["stake"]+=int(package[stake_key]['stake_amount'])
            elif "new_pool" in package[stake_key] and package[stake_key]["new_pool"] is not None and "old_pool" in package[stake_key] and package[stake_key]["old_pool"] is None:
                new_pool_id=package[stake_key]["new_pool"][0:56]
                new_pool_ticker=package[stake_key]["new_pool"][56:]
                old_pool_id="None"
                old_pool_ticker=""
                
                # this is indivudual pool stats
                if new_pool_id not in stake_gained_from_by_pool:
                    stake_gained_from_by_pool[new_pool_id]={}
                if old_pool_id not in stake_gained_from_by_pool[new_pool_id]:
                    stake_gained_from_by_pool[new_pool_id][old_pool_id]={}
                stake_gained_from_by_pool[new_pool_id][old_pool_id][stake_key]={"pool_id":old_pool_id,"epoch":targetepoch,"stake_key":stake_key,"pool_ticker":old_pool_ticker,"stake":int(package[stake_key]['stake_amount'])}
                
                
                # this is for overall gain/loss across all pools
                if new_pool_id not in stake_gained_by_pool:
                    stake_gained_by_pool[new_pool_id]={"pool_id":new_pool_id,"pool_ticker":new_pool_ticker,"delegators":1,"stake":int(package[stake_key]['stake_amount'])}
                else:
                    stake_gained_by_pool[new_pool_id]["delegators"]+=1
                    stake_gained_by_pool[new_pool_id]["stake"]+=int(package[stake_key]['stake_amount'])
            else:
                print("ignorning none none")

    top_stake_gained_by_pool = sorted(stake_gained_by_pool.values(), key=lambda x: x["stake"], reverse=True)
    top_stake_lost_by_pool = sorted(stake_lost_by_pool.values(), key=lambda x: x["stake"], reverse=True)
    print("*************************************************1")
    #print(top_stake_gained_by_pool)
    aws.dump_s3(top_stake_gained_by_pool,"stats/loyalty/"+str(epoch)+"/threeepoch_top_stake_gained_by_pool.json")

    #print(top_stake_lost_by_pool)
    aws.dump_s3(top_stake_lost_by_pool,"stats/loyalty/"+str(epoch)+"/threeepoch_top_stake_lost_by_pool.json")

    # now do each of the pool summaries as well
    
    print("*************************************************2")
    for pool_id in stake_gained_from_by_pool:
        stake_gained_from=[]
    
        for from_pool_id in stake_gained_from_by_pool[pool_id]:
            stake_gained_from.extend(list(stake_gained_from_by_pool[pool_id][from_pool_id].values()))
        #print(stake_gained_from)    
        stake_gained_from.sort(key=lambda x: x["stake"], reverse=True)
        aws.dump_s3(stake_gained_from,"stats/loyalty/"+str(epoch)+"/threeepoch_stake_gained_from/"+str(pool_id)+".json")
    print("*************************************************3")
    for pool_id in stake_lost_to_by_pool:
        stake_lost_to=[]
        for to_pool_id in stake_lost_to_by_pool[pool_id]:
            stake_lost_to.extend(list(stake_lost_to_by_pool[pool_id][to_pool_id].values()))
        stake_lost_to.sort(key=lambda x: x["stake"], reverse=True)
        aws.dump_s3(stake_lost_to,"stats/loyalty/"+str(epoch)+"/threeepoch_stake_lost_to/"+str(pool_id)+".json")
    print("*************************************************4")

def processLoyalty(stake_key):
    
    # if we already have a record, then we process one epoch after the max record already defined:
    pg.cur1_execute("select package from delegator_loyalty where stake_key=%s",[stake_key])
    row=pg.cur1_fetchone()
    if row:
        package=row['package']
    else:
        package={"pool_ids_staked":{},"max_epoch":0,"current_pool":"","epochs_staked":0,"total_staked":0,"recent10":{}}
    
    #print("processing records after epoch: ",package['max_epoch'])
    pg.cur1_execute("select epoch,amount,delegated_to_pool, delegated_to_ticker, reward_address_details, stake_rewards from stake_history where stake_key=%s and epoch>=%s and forecast=false order by epoch asc",[stake_key,int(package['max_epoch'])])
    rows=pg.cur1_fetchall()
    
    recent10={}
    for row in rows:
        package['recent10'][int(row['epoch'])]=row['delegated_to_pool']
        for epoch in list(package['recent10'].keys()):
            if int(epoch)<int(row['epoch'])-9:
                del package['recent10'][epoch]
        
        package['max_epoch']=max(package['max_epoch'],int(row['epoch']))
        if package['max_epoch']==int(row['epoch']):
            package['current_pool']=row['delegated_to_pool']
        package['total_staked']+=int(row['amount'])
        package['epochs_staked']+=1
        # to get rewards we will always pull it out of stake details since we need the exact rewards ONLY for this stake.
        if str(stake_key+row['delegated_to_pool']) in row['reward_address_details']:
            rewards=int(row['reward_address_details'][str(stake_key+row['delegated_to_pool'])]['stakeRewards']) if row['reward_address_details'][str(stake_key+row['delegated_to_pool'])]['stakeRewards'] is not None else 0
        else:
            rewards=int(row['stake_rewards'])
            
        
        if row['delegated_to_pool'] not in package['pool_ids_staked']:
            package['pool_ids_staked'][(row['delegated_to_pool'])]={"ticker":row['delegated_to_ticker'].strip() if row['delegated_to_ticker'] is not None else "","amount":int(row['amount']),"epochs":1,"rewards":rewards}
        else:
            package['pool_ids_staked'][(row['delegated_to_pool'])]["amount"]+=int(row['amount'])
            package['pool_ids_staked'][(row['delegated_to_pool'])]["rewards"]+=rewards
            package['pool_ids_staked'][(row['delegated_to_pool'])]["epochs"]+=1
    recent5 = {k: v for k, v in package['recent10'].items() if int(k) > int(package['max_epoch'])-5}

    for pool_id in package['pool_ids_staked']:
        package['pool_ids_staked'][pool_id]['lifetime_loyalty']=package['pool_ids_staked'][pool_id]['epochs']/package['epochs_staked'] if package['epochs_staked']>0 else 0
        package['pool_ids_staked'][pool_id]['recent10_loyalty']=sum(1 for p in package['recent10'].values() if p==pool_id)/len(package['recent10']) if len(package['recent10'])>0 else 0
        package['pool_ids_staked'][pool_id]['recent5_loyalty']=sum(1 for p in recent5.values() if p==pool_id)/len(recent5) if len(recent5)>0 else 0
    #calculate loyalty scores

    pg.cur1_execute("insert into delegator_loyalty (stake_key,package) values(%s,%s) ON CONFLICT ON CONSTRAINT delegator_loyalty_pkey DO UPDATE set package=%s",[stake_key,Json(package),Json(package)])
    pg.conn_commit()
    aws.dump_s3(package,"stats/loyalty/bykey/"+str(stake_key)[0:4]+"/"+str(stake_key)+".json")


epoch_processing=fb.getKey(baseNetwork+"/epoch_processing")
epoch=epoch_processing['targetEpoch']

block_production_epoch=epoch+1
print("processing block_production_epoch: "+str(block_production_epoch))
# process epoch loyalty and get list of all stake_keys affected
package=processEpochLoyaltySummary(block_production_epoch)
for stake_key in package:
    processLoyalty(stake_key)
process3EpochPoolMigrations(block_production_epoch)
