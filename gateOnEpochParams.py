from fb_utils import *
from config import *
from pg_utils import *
from aws_utils import *
fb=fb_utils()
pg=pg_utils('waitstakewriting')
aws=aws_utils()
allowChanges=True
while True:
    epoch_processing=fb.getKey(baseNetwork+"/epoch_processing")
    print(epoch_processing)
    if 'targetEpoch' in epoch_processing and 'epochPricesDone' in epoch_processing and 'epochParamsEpochDone' in epoch_processing and 'waitStakeEpochDone' in epoch_processing:
        if epoch_processing['targetEpoch'] != 0:
            if epoch_processing['targetEpoch'] == epoch_processing['epochPricesDone'] and epoch_processing['targetEpoch'] == epoch_processing['epochParamsEpochDone'] and epoch_processing['targetEpoch'] == epoch_processing['waitStakeEpochDone']:
                break
            else:
                print("epoch processing is in progress, waiting 30 seconds")
        else:
            print("epoch processing has not started yet, waiting 30 seconds")
    else:
        print("no epoch processing...")
    time.sleep(30)
print("all gate requirements met, continue processing")
epoch=epoch_processing['targetEpoch']
block_production_epoch=epoch+1
if True:
    snapshot_values=aws.load_s3("ls/markSnapshot"+str(epoch)+".json")
    ledgerpoolparams={}
    for pool_id in snapshot_values['pool_params']:
        ledgerpoolparams[pool_id]={}
        ledgerpoolparams[pool_id]['rewardaddresses']=snapshot_values['pool_params'][pool_id]['ra'][2:]
        ledgerpoolparams[pool_id]['pledge']=snapshot_values['pool_params'][pool_id]['pledge']
        if len(snapshot_values['pool_params'][pool_id]['own']):
            ledgerpoolparams[pool_id]['owners'] = sorted(list(snapshot_values['pool_params'][pool_id]['own']))
        else:
            ledgerpoolparams[pool_id]['owners'] = None 
            
        ledgerpoolparams[pool_id]['margin']=round(float(snapshot_values['pool_params'][pool_id]['margin_num']/snapshot_values['pool_params'][pool_id]['margin_den']),6)
        ledgerpoolparams[pool_id]['cost']=snapshot_values['pool_params'][pool_id]['cost']

    print("count pools",len(ledgerpoolparams))

    pg.cur1_execute("delete from pool_epoch_params where epoch=%s",[block_production_epoch])
    pg.conn_commit()

    for pool in ledgerpoolparams:
        if  ledgerpoolparams[pool]['pledge'] > 45000000000000000: # pledge
            ledgerpoolparams[pool]['pledge'] = 45000000000000000
        if  ledgerpoolparams[pool]['pledge'] < 0:
            ledgerpoolparams[pool]['pledge'] = 0
        if  ledgerpoolparams[pool]['cost'] > 45000000000000000: # cost
            ledgerpoolparams[pool]['cost'] = 45000000000000000
        if  ledgerpoolparams[pool]['cost'] < 0:
            ledgerpoolparams[pool]['cost'] = 0
        pg.cur1_execute("""insert into pool_epoch_params (epoch, pool_id, reward_address, owners, pledge, cost, margin) 
        values(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT pep_idx DO UPDATE set reward_address=%s, owners=%s, pledge=%s, cost=%s, margin=%s""",[
    block_production_epoch, pool, ledgerpoolparams[pool]['rewardaddresses'],(ledgerpoolparams[pool]['owners']),ledgerpoolparams[pool]['pledge'],ledgerpoolparams[pool]['cost'],ledgerpoolparams[pool]['margin'],
    ledgerpoolparams[pool]['rewardaddresses'],(ledgerpoolparams[pool]['owners']),ledgerpoolparams[pool]['pledge'],ledgerpoolparams[pool]['cost'],ledgerpoolparams[pool]['margin']
        ])
        pg.conn_commit()
        
    
    delegatorspool={}
    pooldelegators={}
    delegators={}
    stake_ls={}
    totalstake=0
    for keyhash in snapshot_values['values']:
        delegatorspool[keyhash]=snapshot_values['values'][keyhash]['dp']
        if snapshot_values['values'][keyhash]['dp'] not in pooldelegators:
            pooldelegators[snapshot_values['values'][keyhash]['dp']]={}
        if keyhash not in pooldelegators[snapshot_values['values'][keyhash]['dp']]:
            pooldelegators[snapshot_values['values'][keyhash]['dp']][keyhash]={"u":0,"r":0}
        stake_ls[keyhash]=snapshot_values['values'][keyhash]['v']
        totalstake+=snapshot_values['values'][keyhash]['v']
    if allowChanges:
        fb.updateFb(baseNetwork+"/total_active_stake",{(block_production_epoch):totalstake})
    print("total stake: ",totalstake)
    print("done with object.  release memory")
    del snapshot_values

    rewardaccounts={}
    count=0
    poolstake={}
    owners={}
    poolstakebyhashid={}
    rewardaddresses={}
    total_rewards=0
    for processpool in pooldelegators:
        poolupdate={}
        
        totalpool=0

        owner={}
        ownerstake={}
        totalpoolstake=0

        
        rewardaddresses[processpool]=ledgerpoolparams[processpool]['rewardaddresses']
        
            
        for item in pooldelegators[processpool]:
            totalpoolstake=totalpoolstake+ stake_ls[item]
        
        poolstake[processpool]=totalpoolstake

        poolstakebyhashid[processpool]=totalpoolstake
        
        poolupdate['waitStake']=totalpoolstake   
        
        fb.writeFb(baseNetwork+"/pool_stats/"+processpool+"/stake",{block_production_epoch:totalpoolstake})
        pg.cur1_execute("insert into pool_stake (epoch,pool_id,value) values(%s,%s,%s) ON CONFLICT ON CONSTRAINT pool_stake_pkey DO UPDATE set value=%s",[block_production_epoch,processpool,totalpoolstake,totalpoolstake])  
        pg.conn_commit()  
                


    if allowChanges:
        aws.dump_s3(poolstakebyhashid,baseNetwork+'/stake_pool_columns/'+str(block_production_epoch)+'/stake.json')

    # gather current ticker list
    allpoolids={}
    pg.cur1_execute("select pool_id, ticker from pools")
    row=pg.cur1_fetchone()
    while row:
        allpoolids[row['pool_id']]=row['ticker']
        row=pg.cur1_fetchone()



    rewardaccounts={}
    for acct in stake_ls:
        rewardaccounts[acct]={"amount":stake_ls[acct],"stakeRewards":0,"operatorRewards":0,"forecast": True,"rewardaddr_details":{}}
        rewardaccounts[acct]['delegatedTo']=delegatorspool[acct] if acct in delegatorspool else 'None'
        rewardaccounts[acct]['delegatedToTicker']=(allpoolids[delegatorspool[acct]] if delegatorspool[acct] in allpoolids else '') if rewardaccounts[acct]['delegatedTo']!='None' else '' 
        rewardaccounts[acct]['rewardsSentTo']=acct #assume all rewards sent to itself unless we find them in owner later
    for poolid in ledgerpoolparams:
        if ledgerpoolparams[poolid]['rewardaddresses'] not in rewardaccounts:
            rewardaccounts[ledgerpoolparams[poolid]['rewardaddresses']]={"amount":0,"stakeRewards":0,"operatorRewards":0,"forecast": True,"rewardaddr_details":{}}
            rewardaccounts[ledgerpoolparams[poolid]['rewardaddresses']]['delegatedTo']=delegatorspool[ledgerpoolparams[poolid]['rewardaddresses']] if ledgerpoolparams[poolid]['rewardaddresses'] in delegatorspool else 'None'
            rewardaccounts[ledgerpoolparams[poolid]['rewardaddresses']]['delegatedToTicker']=(allpoolids[delegatorspool[ledgerpoolparams[poolid]['rewardaddresses']]] if delegatorspool[ledgerpoolparams[poolid]['rewardaddresses']] in allpoolids else '') if rewardaccounts[ledgerpoolparams[poolid]['rewardaddresses']]['delegatedTo']!='None' else '' 
            rewardaccounts[ledgerpoolparams[poolid]['rewardaddresses']]['rewardsSentTo']=ledgerpoolparams[poolid]['rewardaddresses']
        
        for acct in ledgerpoolparams[poolid]['owners']:
        # if its a owner account but its NOT delegated to the pool its an owner of then the rewards do not get collected and forewarded to the reward address
            if acct not in rewardaccounts:
                rewardaccounts[acct]={"amount":0,"stakeRewards":0,"operatorRewards":0,"forecast": True,"rewardaddr_details":{}}
                rewardaccounts[acct]['delegatedTo']=delegatorspool[acct] if acct in delegatorspool else 'None'
                rewardaccounts[acct]['delegatedToTicker']=(allpoolids[delegatorspool[acct]] if delegatorspool[acct] in allpoolids else '') if rewardaccounts[acct]['delegatedTo']!='None' else '' 
                rewardaccounts[acct]['rewardsSentTo']=ledgerpoolparams[poolid]['rewardaddresses']
                #rewardaccounts[ledgerpoolparams[poolid]['rewardaddresses']]['rewardaddr_details'][acct + poolid]={"amount":0,"pool":poolid,"stakeRewards":0,"forecast":True,"operatorRewards":0}
            else:
                if poolid==rewardaccounts[acct]['delegatedTo']:
                    rewardaccounts[acct]['rewardsSentTo']=ledgerpoolparams[poolid]['rewardaddresses']
                    #rewardaccounts[ledgerpoolparams[poolid]['rewardaddresses']]['rewardaddr_details'][acct + poolid]={"amount":rewardaccounts[acct]['amount'],"pool":poolid,"stakeRewards":0,"forecast":True,"operatorRewards":0}
                

    print(f"deleting any existing records from {block_production_epoch}")
    pg.cur1_execute("delete from stake_history where epoch=%s",[block_production_epoch])
    pg.conn_commit()


    print(f"inserting new records for {block_production_epoch}")
    count = 0
    for staddr in rewardaccounts:
        count=count+1
        pg.cur1_execute("insert into stake_history (epoch, stake_key, amount, delegated_to_pool, delegated_to_ticker, forecast, operator_rewards, stake_rewards, rewards_sent_to, reward_address_details) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",(
            block_production_epoch, staddr, rewardaccounts[staddr]['amount'],rewardaccounts[staddr]['delegatedTo'],rewardaccounts[staddr]['delegatedToTicker'],True,rewardaccounts[staddr]['operatorRewards'],rewardaccounts[staddr]['stakeRewards'],
            rewardaccounts[staddr]['rewardsSentTo'],Json(rewardaccounts[staddr]['rewardaddr_details'])
        ))
        if count % 100000==0:
            print(f"{count} records processed")
            pg.conn_commit()
    pg.conn_commit()

    print("write batch")
    fb.writeBatch()
    print("writing waitstake done")