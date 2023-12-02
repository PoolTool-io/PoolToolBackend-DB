from pt_utils import *
from fb_utils import *
from pg_utils import *
from aws_utils import *
from config import *
import json
import math
from cbor2 import dumps, loads, load
import psycopg2.extras
from psycopg2.extras import Json
import sys
import time
fb=fb_utils()
aws=aws_utils()
allowChanges=True
updatePrices=True
pg=pg_utils("processRewards")

forecast = False

if len(sys.argv)>1:
    print(sys.argv[1])
    if sys.argv[1]=="forecast":
        forecast=True
print("forecast",forecast)



# get current epoch.  subtract 2 to get the target epoch as an example epoch 346 now, rewards are distributed in 346 for blocks made in 344
# get block count by pool id for target epoch
# get delegation by stake key and pool and value in target epoch (from db)
# get pool params for target epoch

# get reserves in ledger state for target epoch + 1
# get reward_pot in ledger state for target epoch + 2 (this is what needs to be estimated for the forecasted rewards)
epoch_processing=fb.getKey(baseNetwork+"/epoch_processing")
epoch=epoch_processing['targetEpoch']

if forecast:
    block_production_epoch=epoch-1
else:
    block_production_epoch=epoch-2
print("processing "+("forecasted " if forecast else "")+"rewards epoch: "+str(block_production_epoch))


blocksbypool={}
total_epoch_blocks=0
pg.cur1_execute("select block_count, pool_id from pool_epoch_blocks where epoch=%s",[block_production_epoch])
row=pg.cur1_fetchone()
while row:
    blocksbypool[row['pool_id']]=row['block_count']
    total_epoch_blocks=total_epoch_blocks+row['block_count']
    row=pg.cur1_fetchone()


pg.cur1_execute("select * from epoch_params where epoch=%s",[block_production_epoch])
row=pg.cur1_fetchone()

analysis_epoch_decentralization_param=row['decentralisation']
analysis_epoch_k=row['optimal_pool_count']
a0=row['influence']
reserves=row['reserves']
treasury=row['treasury']
reward_pot=row['reward_pot']
epoch_all_blocks=row['epoch_blocks']
print("analysis_epoch_decentralization_param",analysis_epoch_decentralization_param)
print("analysis_epoch_k",analysis_epoch_k)
print("a0",a0)
print("reserves",reserves)
print("treasury",treasury)
print("epoch_all_blocks",epoch_all_blocks)
print("reward_pot",reward_pot)



rewardaccounts={}
totalpoolstake={}
pooldelegators={}
stake={}
totalstake=0
pg.cur1_execute("select * from stake_history where epoch=%s",[block_production_epoch])
row=pg.cur1_fetchone()
while row:
    if row['delegated_to_pool'] not in pooldelegators:
        pooldelegators[row['delegated_to_pool']]={}
    if row['stake_key'] not in pooldelegators[row['delegated_to_pool']]:
        pooldelegators[row['delegated_to_pool']][row['stake_key']]={"u":0,"r":0}
    stake[row['stake_key']]=row['amount']
    if row['delegated_to_pool'] is not None:
        if row['delegated_to_pool'] not in totalpoolstake:
            totalpoolstake[row['delegated_to_pool']]=0
        totalpoolstake[row['delegated_to_pool']]=totalpoolstake[row['delegated_to_pool']]+row['amount']
    totalstake=totalstake+row['amount']
    rewardaccounts[row['stake_key']]={
        "epoch":row['epoch'],
        "stake_key":row['stake_key'],
        "amount":row['amount'],
        "delegated_to_pool":row['delegated_to_pool'],
        "delegated_to_ticker":row['delegated_to_ticker'],
        "forecast":row['forecast'],
        "operator_rewards":0,
        "stake_rewards":0,
        "rewards_sent_to":row['rewards_sent_to'],
        "reward_address_details":{}}
    row=pg.cur1_fetchone()



ledgerpoolparams={}
pg.cur1_execute("select * from pool_epoch_params where epoch=%s",[block_production_epoch])
row=pg.cur1_fetchone()
while row:
    ledgerpoolparams[row['pool_id']]=row
    row=pg.cur1_fetchone()


# calculating rewards
termz0 = 1/analysis_epoch_k

term1 = reward_pot/(1 + a0) # we need this value from last epoch...
print("term1: "+ str(term1))
total_supply = (45e15 - reserves)

focusOnPool = None #['']
focusOnAddress = None #['']
count=0
totalcount=len(ledgerpoolparams)
poolstake={}
poolupdate={}
total_rewards=0
poolstakebyhashid={}
poolrosbyhashid={}
rewardaddressbypool={}
for processpool in ledgerpoolparams:
    
    count=count+1

    totalpool=0
    if ledgerpoolparams[processpool]['reward_address'] not in rewardaddressbypool:
        rewardaddressbypool[ledgerpoolparams[processpool]['reward_address']]=[]
    if processpool not in rewardaddressbypool[ledgerpoolparams[processpool]['reward_address']]:
        rewardaddressbypool[ledgerpoolparams[processpool]['reward_address']].append(processpool)

    if processpool not in blocksbypool:
        blocksbypool[processpool]=0
    if processpool not in totalpoolstake:
        totalpoolstake[processpool]=0
    # check if pledge met.  if not, nothing is distributed...
    sumpledge=0
    pledgemultiplier=1
    if focusOnPool is not None and processpool not in focusOnPool:
        continue
    print("pool::::::::::::: " + processpool+" ("+str(count)+"/"+str(totalcount)+")")
    #print("pool delegator count: " + str(len(pooldelegators[processpool])))
    if ledgerpoolparams[processpool]['owners'] is not None:
        for owneraddr in ledgerpoolparams[processpool]['owners']:
            if owneraddr in stake:
                sumpledge=sumpledge+stake[owneraddr]
        if int(sumpledge) < int(ledgerpoolparams[processpool]['pledge']):
            pledgemultiplier=0
    else:
        pledgemultiplier=0
    #print("pledgemultiplier",pledgemultiplier)


    #print("reward_pot: "+ str(reward_pot))
    #term1 = reward_pot/(1 + a0) # we need this value from last epoch...
    #print("analysis_epoch_decentralization_param: "+ str(analysis_epoch_decentralization_param))
    #print("a0: " + str(a0))
    #print("termz0: "+ str(termz0))
    sigmaprime= min(totalpoolstake[processpool]/total_supply,termz0) #J23
    #print("sigmaprime: " + str(sigmaprime))
    sprime = min(ledgerpoolparams[processpool]['pledge']/total_supply,termz0) #J24
    #print("sprime: " + str(sprime))

    optimalrewards = pledgemultiplier*((((sigmaprime-((1-sigmaprime/termz0)*sprime))/termz0*sprime*a0)+sigmaprime) * term1)
    #print("optimal rewards")
    #print(optimalrewards)
    
    if analysis_epoch_decentralization_param>0:
        expected_blocks=(totalpoolstake[processpool]/totalstake) * epoch_all_blocks
    else:
        expected_blocks=((totalpoolstake[processpool]/totalstake)  *  total_epoch_blocks)
    #print("expected blocks: "+ str(expected_blocks))
    
    if analysis_epoch_decentralization_param>=0.8:
        if processpool in blocksbypool and blocksbypool[processpool]>0:
            performance=1
        else:
            performance=0
    else:
        if processpool in blocksbypool and expected_blocks>0:
            performance=(blocksbypool[processpool]/expected_blocks)
        else:
            performance=0
    #print("performance: "+str(performance))
    performance_adjusted_rewards=int(max(0,performance*optimalrewards))


    #print("performance_adjusted_rewards: "+ str(performance_adjusted_rewards))
    if performance_adjusted_rewards>ledgerpoolparams[processpool]['cost']:
        pool_fees=int(ledgerpoolparams[processpool]['cost']+((performance_adjusted_rewards-ledgerpoolparams[processpool]['cost'])*ledgerpoolparams[processpool]['margin']))
    else:
        pool_fees=int(performance_adjusted_rewards)
    #print("pool_fees: "+str(pool_fees))

    #print("updating pool: " + processpool)
    if totalpoolstake[processpool]>0:
        ros = math.pow(((performance_adjusted_rewards-pool_fees) / totalpoolstake[processpool]) + 1, (365 / 5)) - 1
    else:
        ros = 0


    poolupdate[processpool]={"epochRos":ros,"epochTax":pool_fees,"epochRewards":(performance_adjusted_rewards-pool_fees)}
    poolstakebyhashid[processpool]={"f":False,"epochRos":ros,"epochTax":pool_fees,"epochRewards":(performance_adjusted_rewards-pool_fees)}
    if allowChanges:
        fb.writeFb(baseNetwork+"/pool_stats/"+processpool+"/pool_fees",{str(block_production_epoch):pool_fees})
        fb.writeFb(baseNetwork+"/pool_stats/"+processpool+"/delegators_rewards",{str(block_production_epoch):(performance_adjusted_rewards-pool_fees)})
        fb.writeFb(baseNetwork+"/pool_stats/"+processpool+"/ros",{str(block_production_epoch):(ros*100)})
    else:
        print(baseNetwork+"/pool_stats/"+processpool+"/pool_fees",{str(block_production_epoch):pool_fees})
        print(baseNetwork+"/pool_stats/"+processpool+"/delegators_rewards",{str(block_production_epoch):(performance_adjusted_rewards-pool_fees)})
        print(baseNetwork+"/pool_stats/"+processpool+"/ros",{str(block_production_epoch):(ros*100)})
    if not forecast:
        pg.cur1_execute("insert into pool_fees (pool_id,epoch,value) values(%s,%s,%s) ON CONFLICT ON CONSTRAINT pool_fees_pkey DO UPDATE set value=%s",[processpool,block_production_epoch,pool_fees,pool_fees])
        pg.cur1_execute("insert into pool_delegators_rewards (pool_id,epoch,value) values(%s,%s,%s) ON CONFLICT ON CONSTRAINT pool_delegators_rewards_pkey DO UPDATE set value=%s",[processpool,block_production_epoch,(performance_adjusted_rewards-pool_fees),(performance_adjusted_rewards-pool_fees)])
        pg.cur1_execute("insert into pool_ros (pool_id,epoch,ros) values(%s,%s,%s) ON CONFLICT ON CONSTRAINT pool_ros_pkey DO UPDATE set ros=%s",[processpool,block_production_epoch,(ros*100),(ros*100)])
        pg.conn_commit()

        #calculate lifetime statistics
        pg.cur1_execute("select sum(value) as sumvalue, sum(value) filter (where epoch>%s-12) as sumvalue12,sum(value) filter (where epoch>%s-6) as sumvalue6 from pool_delegators_rewards where pool_id=%s and epoch<=%s",[block_production_epoch,block_production_epoch,processpool,block_production_epoch])
        row=pg.cur1_fetchone()
        if row:
            poolupdate[processpool]['lifetimeRewards']=int(row['sumvalue']) if row['sumvalue'] is not None else 0
            poolupdate[processpool]['12EpochRewards']=int(row['sumvalue12']) if row['sumvalue12'] is not None else 0
            poolupdate[processpool]['6EpochRewards']=int(row['sumvalue6']) if row['sumvalue6'] is not None else 0
            poolstakebyhashid[processpool]['lifetimeRewards']=int(row['sumvalue']) if row['sumvalue'] is not None else 0
        else:
            poolupdate[processpool]['lifetimeRewards']=0
            poolupdate[processpool]['12EpochRewards']=0
            poolupdate[processpool]['6EpochRewards']=0
            poolstakebyhashid[processpool]['lifetimeRewards']=0
        pg.cur1_execute("select sum(value) as sumvalue,sum(value) filter (where epoch>%s-12) as sumvalue12,sum(value) filter (where epoch>%s-6) as sumvalue6 from pool_fees where pool_id=%s and epoch<=%s",[block_production_epoch,block_production_epoch,processpool,block_production_epoch])
        row=pg.cur1_fetchone()
        if row:
            poolupdate[processpool]['lifetimeTax']=int(row['sumvalue']) if row['sumvalue'] is not None else 0
            poolupdate[processpool]['12EpochTax']=int(row['sumvalue12']) if row['sumvalue12'] is not None else 0
            poolupdate[processpool]['6EpochTax']=int(row['sumvalue6']) if row['sumvalue6'] is not None else 0
            poolstakebyhashid[processpool]['lifetimeTax']=int(row['sumvalue']) if row['sumvalue'] is not None else 0
        else:
            poolupdate[processpool]['lifetimeTax']=0
            poolupdate[processpool]['12EpochTax']=0
            poolupdate[processpool]['6EpochTax']=0
            poolstakebyhashid[processpool]['lifetimeTax']=0
        pg.cur1_execute("select sum(value) as sumvalue,sum(value) filter (where epoch>%s-12) as sumvalue12,sum(value) filter (where epoch>%s-6) as sumvalue6 from pool_stake where pool_id=%s and epoch<=%s",[block_production_epoch,block_production_epoch,processpool,block_production_epoch])
        row=pg.cur1_fetchone()
        if row:
            poolupdate[processpool]['lifetimeStake']=int(row['sumvalue']) if row['sumvalue'] is not None else 0
            poolupdate[processpool]['12EpochStake']=int(row['sumvalue12']) if row['sumvalue12'] is not None else 0
            poolupdate[processpool]['6EpochStake']=int(row['sumvalue6']) if row['sumvalue6'] is not None else 0
            poolstakebyhashid[processpool]['lifetimeStake']=int(row['sumvalue']) if row['sumvalue'] is not None else 0
        else:
            poolupdate[processpool]['lifetimeStake']=0
            poolupdate[processpool]['12EpochStake']=0
            poolupdate[processpool]['6EpochStake']=0
            poolstakebyhashid[processpool]['lifetimeStake']=0
        pg.cur1_execute("select first_epoch from pools where pool_id=%s",[processpool])
        row=pg.cur1_fetchone()
        if row:
            package={}
            if row['first_epoch']>0 and row['first_epoch']<(block_production_epoch-12) and poolupdate[processpool]['12EpochRewards']>0 and poolupdate[processpool]['12EpochStake']>0:
                compoundingperiods=12
                roioverspan = poolupdate[processpool]['12EpochRewards'] / ((poolupdate[processpool]['12EpochStake']) / compoundingperiods)
                poolupdate[processpool]['12EpochRos']=math.pow(roioverspan + 1, 1 / (compoundingperiods / (365 / 5))) - 1
                package['tr']=poolupdate[processpool]['12EpochRos']
            else:
                package['tr']=0

            if row['first_epoch']>0 and row['first_epoch']<(block_production_epoch-6) and poolupdate[processpool]['6EpochRewards']>0 and poolupdate[processpool]['6EpochStake']>0:
                compoundingperiods=6
                roioverspan = poolupdate[processpool]['6EpochRewards'] / ((poolupdate[processpool]['6EpochStake']) / compoundingperiods)
                poolupdate[processpool]['6EpochRos']=math.pow(roioverspan + 1, 1 / (compoundingperiods / (365 / 5))) - 1
                package['sr']=poolupdate[processpool]['6EpochRos']
            else:
                package['sr']=0

            if row['first_epoch']>0 and poolupdate[processpool]['lifetimeRewards']>0 and poolupdate[processpool]['lifetimeStake']>0:
                compoundingperiods = int(block_production_epoch) - int(row['first_epoch']) - 1  
                if compoundingperiods>0:
                    roioverspan = poolupdate[processpool]['lifetimeRewards'] / ((poolupdate[processpool]['lifetimeStake']) / compoundingperiods)
                    poolupdate[processpool]['lifetimeRos']=math.pow(roioverspan + 1, 1 / (compoundingperiods / (365 / 5))) - 1
                    poolstakebyhashid[processpool]['lifetimeRos']=math.pow(roioverspan + 1, 1 / (compoundingperiods / (365 / 5))) - 1
                    package['lros']=poolstakebyhashid[processpool]['lifetimeRos']
                else:
                    poolupdate[processpool]['lifetimeRos']=0
                    poolstakebyhashid[processpool]['lifetimeRos']=0
                    package['lros']=0
            else:
                poolupdate[processpool]['lifetimeRos']=0
                poolstakebyhashid[processpool]['lifetimeRos']=0
                package['lros']=0
            poolrosbyhashid[processpool]=poolstakebyhashid[processpool]['lifetimeRos']
            if allowChanges:
                fb.updateFb(baseNetwork+"/stake_pools/"+processpool,package)
                print(baseNetwork+"/stake_pools/"+processpool,package)
            else:
                print(baseNetwork+"/stake_pools/"+processpool,package)
if not forecast:
    if allowChanges:
        fb.writeFb(baseNetwork+"/stake_pool_columns/lifetimeros",poolrosbyhashid)
        pg.cur1_execute("""select width_bucket(ros, 0, 10, 19) as bucket,
        numrange(min(ros)::numeric, max(ros)::numeric, '[]') as range,
                count(pool_id) as freq
            from pool_ros where epoch=%s and ros>0
        group by bucket order by bucket""",[block_production_epoch])

        rewardsHistogram={"epoch":block_production_epoch,"histogramdata":[]}

        row=pg.cur1_fetchone()
        while row:
            rewardsHistogram['histogramdata'].append({"min":format(float(row['range'].lower),".2f"),"max":format(float(row['range'].upper),".2f"),"freq":int(row['freq'])})
            row=pg.cur1_fetchone()

        print(rewardsHistogram)
        fb.updateFb(baseNetwork + "/staticCharts",{"poolRewardsHistogram":rewardsHistogram} )
    else:
        print(poolstakebyhashid)
        
if allowChanges:
    aws.dump_s3(poolstakebyhashid,baseNetwork+'/stake_pool_columns/'+str(block_production_epoch)+'/rewards.json')
else:
    print(poolrosbyhashid)
# update all pool details  here
fb.writeBatch()
if forecast:
    fb.getReference(baseNetwork+"/mary_db_sync_status").update({"pool_forecast_calculated_epoch":(block_production_epoch)})
else:
    fb.getReference(baseNetwork+"/mary_db_sync_status").update({"pool_actuals_calculated_epoch":(block_production_epoch)})

allpoolids={}
multiple_pools_reward_addresses={}
for reward_address in rewardaddressbypool:
    if len(rewardaddressbypool[reward_address])>1:
        multiple_pools_reward_addresses[reward_address]=rewardaddressbypool[reward_address]
# now process all the delegator values
for poolid in pooldelegators:
    
    if focusOnPool is not None and poolid not in focusOnPool:
        continue
    for delegator in pooldelegators[poolid]:
        if poolid not in totalpoolstake:
            totalpoolstake[poolid]=0 
        if poolid not in poolupdate:
            poolupdate[poolid]={"epochRos":0,"epochTax":0,"epochRewards":0}
        if totalpoolstake[poolid]>0:
            # print(poolid)
            # print(delegator)
            # print(pooldelegators[poolid][delegator])
            # print(poolupdate[poolid])
            # print(poolstake[poolid])
            r = int(stake[delegator]*(poolupdate[poolid]["epochRewards"])/totalpoolstake[poolid])
        else:
            r = 0
        # check to see if delegation rewards are sent somewhere else before adding them to this account
        if delegator!=rewardaccounts[delegator]['rewards_sent_to']:

            rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['forecast']=forecast
            if r>0:
                rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['stake_rewards']=rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['stake_rewards']+r
                if (delegator+poolid) not in rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['reward_address_details']:
                    rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['reward_address_details'][delegator+poolid]={"amount":rewardaccounts[delegator]['amount'],"pool":poolid,"stakeRewards":0,"forecast":forecast,"operatorRewards":0}
                rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['reward_address_details'][delegator+poolid]['forecast']=forecast
                rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['reward_address_details'][delegator+poolid]['stakeRewards']=rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['reward_address_details'][delegator+poolid]['stakeRewards']+r
                rewardaccounts[delegator]['stake_rewards']=r

        else:
            if not (poolid.strip()!='None' and delegator==ledgerpoolparams[poolid]['reward_address'] and delegator not in ledgerpoolparams[poolid]['owners']):
                rewardaccounts[delegator]['stake_rewards']=rewardaccounts[delegator]['stake_rewards']+r
                if (delegator+poolid) not in rewardaccounts[delegator]['reward_address_details']:
                    rewardaccounts[delegator]['reward_address_details'][delegator+poolid]={"amount":rewardaccounts[delegator]['amount'],"pool":poolid,"stakeRewards":r,"forecast":forecast,"operatorRewards":0}
                rewardaccounts[delegator]['reward_address_details'][delegator+poolid]['stakeRewards']=r
            rewardaccounts[delegator]['forecast']=forecast
        if focusOnAddress is not None and delegator in focusOnAddress:
            print("raw stake rewards as delegator for: ",delegator)
            print(r)
            print(rewardaccounts[delegator])

for poolid in ledgerpoolparams:

    # and finally for each pool make sure the operator rewards are going to the right place
    if poolid.strip()!='None':
        delegator = ledgerpoolparams[poolid]['reward_address']
        if poolid not in poolupdate:
            poolupdate[poolid]={"epochRos":0,"epochTax":0,"epochRewards":0}
        if delegator in rewardaccounts:
            if delegator!=rewardaccounts[delegator]['rewards_sent_to']:

                rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['forecast']=forecast
                if poolupdate[poolid]["epochTax"]>1:
                    rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['operator_rewards']=rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['operator_rewards']+poolupdate[poolid]["epochTax"]
                    if (delegator+poolid) not in rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['reward_address_details']:
                        rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['reward_address_details'][delegator+poolid]={"amount":rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['amount'],"pool":poolid,"stakeRewards":0,"forecast":forecast,"operatorRewards":0}
                    rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['reward_address_details'][delegator+poolid]['forecast']=forecast
                    rewardaccounts[rewardaccounts[delegator]['rewards_sent_to']]['reward_address_details'][delegator+poolid]['operatorRewards']=poolupdate[poolid]["epochTax"]
            else:

                rewardaccounts[delegator]['forecast']=forecast
                if poolupdate[poolid]["epochTax"]>1:
                    rewardaccounts[delegator]['operator_rewards']=rewardaccounts[delegator]['operator_rewards']+poolupdate[poolid]["epochTax"]
                    if (delegator+poolid) not in rewardaccounts[delegator]['reward_address_details']:
                        rewardaccounts[delegator]['reward_address_details'][(delegator+poolid)]={"amount":rewardaccounts[delegator]['amount'],"pool":poolid,"stakeRewards":0,"forecast":forecast,"operatorRewards":0}

                    rewardaccounts[delegator]['reward_address_details'][(delegator+poolid)]['forecast']=forecast
                    rewardaccounts[delegator]['reward_address_details'][(delegator+poolid)]['operatorRewards']=poolupdate[poolid]["epochTax"]
            
        if focusOnAddress is not None and delegator in focusOnAddress:
            print("###############################################")
            print("post operator rewards for : ",delegator)
            print("delegated to pool : ",poolid)
            print("pool epoch tax: ",poolupdate[poolid]["epochTax"])
            print(rewardaccounts[delegator])

print("deal with shelley multi pool issues")

for reward_address in multiple_pools_reward_addresses:
    if rewardaccounts[reward_address]['operator_rewards']>0 or rewardaccounts[reward_address]['stake_rewards']>0:
        print("looking at reward address: ",reward_address)
        
        line_items={}
        for item in rewardaccounts[reward_address]['reward_address_details']:
            line_item=rewardaccounts[reward_address]['reward_address_details'][item]
            
            if line_item['pool'] not in line_items:
                line_items[line_item['pool']]={'p':line_item['pool'],"s":0,"o":0,"t":0}
            line_items[line_item['pool']]['s']=line_items[line_item['pool']]['s']+line_item['stakeRewards']
            line_items[line_item['pool']]['t']=line_items[line_item['pool']]['t']+line_item['stakeRewards']
            line_items[line_item['pool']]['o']=line_items[line_item['pool']]['o']+line_item['operatorRewards']
            line_items[line_item['pool']]['t']=line_items[line_item['pool']]['t']+line_item['operatorRewards']
        mintotal="g"
        choosen_pool=None
        for item in line_items:
            print("p:",line_items[item]['p'],line_items[item]['s']/1e6,line_items[item]['o']/1e6,line_items[item]['t']/1e6)
            if int(line_items[item]['t'])>0:
                if (line_items[item]['p'])<mintotal:
                    mintotal=(line_items[item]['p'])
                    choosen_pool=line_items[mintotal]
        if reward_address=='6877dcb866858b2a4941351c98cd67202262bfd82a016cbbaa073927' and (block_production_epoch==222 or (block_production_epoch>=224 and block_production_epoch<227)):
            choosen_pool=line_items['2bf5a031b46b34c07937a769ff6f82b6a78a25c71022eaaf1f20eec9']
        if choosen_pool is not None:
            print("choosen_pool",choosen_pool['p'])
            if block_production_epoch<236:
                rewardaccounts[reward_address]['operator_rewards']=choosen_pool['o']
                rewardaccounts[reward_address]['stake_rewards']=choosen_pool['s']
                for item in rewardaccounts[reward_address]['reward_address_details']:
                    if rewardaccounts[reward_address]['reward_address_details'][item]['pool']!=choosen_pool['p']:
                        rewardaccounts[reward_address]['reward_address_details'][item]['shelley_rewards_bug']=True
        print("#####################")

print("writing reward account data")
count=0
for staddr in rewardaccounts:
    count=count+1
    pg.cur1_execute("""insert into stake_history (epoch, stake_key, amount, delegated_to_pool, delegated_to_ticker, forecast, operator_rewards, stake_rewards, rewards_sent_to, 
        reward_address_details) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT sh_key DO UPDATE set amount=%s, delegated_to_pool=%s, delegated_to_ticker=%s, 
        forecast=%s, operator_rewards=%s, stake_rewards=%s, rewards_sent_to=%s, reward_address_details=%s""",(
        block_production_epoch, staddr, rewardaccounts[staddr]['amount'],rewardaccounts[staddr]['delegated_to_pool'],rewardaccounts[staddr]['delegated_to_ticker'],forecast,rewardaccounts[staddr]['operator_rewards'],rewardaccounts[staddr]['stake_rewards'],
        rewardaccounts[staddr]['rewards_sent_to'],Json(rewardaccounts[staddr]['reward_address_details']),rewardaccounts[staddr]['amount'],rewardaccounts[staddr]['delegated_to_pool'],rewardaccounts[staddr]['delegated_to_ticker'],forecast,rewardaccounts[staddr]['operator_rewards'],rewardaccounts[staddr]['stake_rewards'],
        rewardaccounts[staddr]['rewards_sent_to'],Json(rewardaccounts[staddr]['reward_address_details'])
    ))
    if count % 100000 == 0:
        print(f"{count} records written")
        pg.conn_commit()
pg.conn_commit()
if not forecast:
    pg.cur1_execute("update stake_history set forecast=%s where epoch=%s",[forecast,block_production_epoch])
    pg.conn_commit()
if not forecast:
    
    pg.cur1_execute("""
    select width_bucket(((power (((stake_rewards::float) / amount::float) + 1 , (365 / 5)::float )-1)*100)::numeric, 0, 10, 19) as bucket,
    numrange(min(((power (((stake_rewards::float) / amount::float) + 1 , (365 / 5)::float )-1)*100)::numeric), max(((power (((stake_rewards::float) / amount::float) + 1 , (365 / 5)::float )-1)*100)::numeric), '[]') as range,
            count(stake_key) as freq
        from stake_history where epoch=%s and forecast=false and amount>0 and (select count(*) from jsonb_each(reward_address_details) s) <2  
    group by bucket order by bucket
    """,[block_production_epoch])

    rewardsHistogram={"epoch":block_production_epoch,"histogramdata":[]}

    row=pg.cur1_fetchone()
    while row:
        rewardsHistogram['histogramdata'].append({"min":format(float(row['range'].lower),".2f"),"max":format(float(row['range'].upper),".2f"),"freq":int(row['freq'])})
        row=pg.cur1_fetchone()
    fb.updateFb(baseNetwork + "/staticCharts",{"rewardsHistogram":rewardsHistogram} )

if not forecast:
    fb.getReference(baseNetwork+"/mary_db_sync_status").update({"new_rewards_complete_epoch":(block_production_epoch)})

    
    
print(("forecast " if forecast else "")+"rewards process complete for " + str(block_production_epoch))
