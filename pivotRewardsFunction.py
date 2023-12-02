from aws_utils import aws_utils
aws=aws_utils()
def pivotRewards(stake_key,pg,fb,baseNetwork):
    print("pivotRewards")
    pg.cur1_execute("select max(epoch) as epoch from stake_history where stake_key=%s and forecast=false",[stake_key])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if not row or row['epoch'] is None:
        # we don't have any history for this key
        return
    pivot_to=row['epoch']
    
    # uncomment below for testing a full rebuild of a stake key
    # cursor.execute("delete from key_pivot_epoch where stake_key=%s",[stake_key])
    # conn.commit()
    pg.cur1_execute("select pivoted_to_epoch, life_amount, life_operator_rewards, life_stake_rewards, epochs_staked from key_pivot_epoch where stake_key=%s",[stake_key])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row:
        pivot_from=row['pivoted_to_epoch']+1
        life_amount=row['life_amount']
        life_operator_rewards=row['life_operator_rewards']
        life_stake_rewards=row['life_stake_rewards']
        life_epochs_staked=row['epochs_staked']
    else:
        pivot_from=0
        life_amount=0
        life_operator_rewards=0
        life_stake_rewards=0
        life_epochs_staked=0
    if pivot_from==0:
        # if pivot from is zero, lets wipe whatever was in stake_hist to start so we are clean
        fb.deleteFb(f"{baseNetwork}/stake_hist/{stake_key}")
    updateStakeHistory={}
    pg.cur1_execute("select * from stake_history where stake_key=%s and epoch>=%s and epoch<=%s order by epoch asc",[stake_key,pivot_from,pivot_to])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    while row:
        
        life_amount=life_amount+int(row['amount'])
        life_operator_rewards=life_operator_rewards+int(row['operator_rewards'])
        life_stake_rewards=life_stake_rewards+int(row['stake_rewards'])
        life_epochs_staked=life_epochs_staked+1
        updateStakeHistory[row['epoch']]={"delegatedTo":row['delegated_to_pool'].strip() if row['delegated_to_pool'] is not None else 'None',
        "amount":row['amount'],
        "delegatedToTicker":row['delegated_to_ticker'].strip() if row['delegated_to_ticker'] is not None else '',
        "lifeAmount":life_amount,
        "forecast":False,
        "lifeOperatorRewards":life_operator_rewards,
        "lifeStakeRewards":life_stake_rewards,
        "operatorRewards":int(row['operator_rewards']),
        "stakeRewards":int(row['stake_rewards']),
        "rewardsSentTo":row['rewards_sent_to'],
        "rewardAddrDetails":row['reward_address_details'],
        "pivotedToEpoch":pivot_to}
        #print(row['epoch'],life_amount,life_stake_rewards,life_operator_rewards)
        row=pg.cur1_fetchone()
    # now add in forecasts and waitstake data
    pg.cur1_execute("select * from stake_history where stake_key=%s and epoch>%s order by epoch asc",[stake_key,pivot_to])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    while row:
        updateStakeHistory[row['epoch']]={"delegatedTo":row['delegated_to_pool'].strip() if row['delegated_to_pool'] is not None else 'None',
        "amount":row['amount'],
        "delegatedToTicker":row['delegated_to_ticker'].strip() if row['delegated_to_ticker'] is not None else '',
        "forecast":True,
        "operatorRewards":int(row['operator_rewards']),
        "stakeRewards":int(row['stake_rewards']),
        "rewardsSentTo":row['rewards_sent_to'],
        "rewardAddrDetails":row['reward_address_details'],
        "pivotedToEpoch":pivot_to}
        row=pg.cur1_fetchone()
    if len(updateStakeHistory)>0:
        fb.updateFb(f"{baseNetwork}/stake_hist/{stake_key}",updateStakeHistory)
    pg.cur1_execute("insert into key_pivot_epoch (pivoted_to_epoch,life_amount,life_operator_rewards,life_stake_rewards,epochs_staked,stake_key) values(%s,%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT key_pivot_epoch_pkey DO update set pivoted_to_epoch=%s, life_amount=%s, life_operator_rewards=%s, life_stake_rewards=%s, epochs_staked=%s",[
        pivot_to,life_amount,life_operator_rewards,life_stake_rewards,life_epochs_staked,stake_key,pivot_to,life_amount,life_operator_rewards,life_stake_rewards,life_epochs_staked
    ])
    pg.conn_commit()
    #print("success")

def pivotRewardsArchived(stake_key,pg,fb,baseNetwork):
    pg.cur1_execute("select max(epoch) as epoch from stake_history where stake_key=%s and forecast=false",[stake_key])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if not row or row['epoch'] is None:
        # we don't have any history for this key
        return
    pivot_to=row['epoch']
    
    # cursor.execute("delete from key_pivot_epoch where stake_key=%s",[stake_key])
    # conn.commit()
    pg.cur1_execute("select pivoted_to_epoch, life_amount, life_operator_rewards, life_stake_rewards, epochs_staked from key_pivot_epoch where stake_key=%s",[stake_key])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row:
        pivot_from=row['pivoted_to_epoch']+1
        life_amount=row['life_amount']
        life_operator_rewards=row['life_operator_rewards']
        life_stake_rewards=row['life_stake_rewards']
        life_epochs_staked=row['epochs_staked']
    else:
        pivot_from=0
        life_amount=0
        life_operator_rewards=0
        life_stake_rewards=0
        life_epochs_staked=0
    if pivot_from==1:
        # if pivot from is zero, lets wipe whatever was in stake_hist to start so we are clean
        fb.deleteFb(f"{baseNetwork}/stake_hist/{stake_key}")
    updateStakeHistory={}
    pg.cur1_execute("select block as epoch from sync_status where key='arch_stake_hist'")
    row=pg.cur1_fetchone()
    archived_complete_epoch=int(row['epoch'])
    print("gathering data from s3 for epochs "+str(pivot_from)+" to "+str(archived_complete_epoch))
    if pivot_from <= archived_complete_epoch:
        #we need to use the s3 archive data
        stake_shard=stake_key[0:6]
        try:
            stake_history = aws.load_s3("stake_history/"+str(stake_shard)+"/"+str(stake_key)+".json")
        except:
            stake_history = {}

        for epoch in range(max(211,pivot_from),int(archived_complete_epoch)+1):
            
            if str(epoch) in stake_history:
                print("archived: "+str(epoch))
                life_amount=life_amount+int(stake_history[str(epoch)]['amount'])
                life_operator_rewards=life_operator_rewards+int(stake_history[str(epoch)]['operator_rewards'])
                life_stake_rewards=life_stake_rewards+int(stake_history[str(epoch)]['stake_rewards'])
                life_epochs_staked=life_epochs_staked+1
                updateStakeHistory[int(epoch)]={"delegatedTo":stake_history[str(epoch)]['delegated_to_pool'].strip() if stake_history[str(epoch)]['delegated_to_pool'] is not None else 'None',
                "amount":int(stake_history[str(epoch)]['amount']),
                "delegatedToTicker":stake_history[str(epoch)]['delegated_to_ticker'].strip() if stake_history[str(epoch)]['delegated_to_ticker'] is not None else '',
                "lifeAmount":life_amount,
                "forecast":False,
                "lifeOperatorRewards":life_operator_rewards,
                "lifeStakeRewards":life_stake_rewards,
                "operatorRewards":int(stake_history[str(epoch)]['operator_rewards']),
                "stakeRewards":int(stake_history[str(epoch)]['stake_rewards']),
                "rewardsSentTo":stake_history[str(epoch)]['rewards_sent_to'],
                "rewardAddrDetails":stake_history[str(epoch)]['reward_address_details'],
                "pivotedToEpoch":pivot_to}
            else:
                pass
            pivot_from=epoch+1
    
    pg.cur1_execute("select * from stake_history where stake_key=%s and epoch>=%s and epoch<=%s order by epoch asc",[stake_key,pivot_from,pivot_to])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    print("gathering data from db for epochs "+str(pivot_from)+" to "+str(pivot_to))
    while row:
        print("db: "+str(row['epoch']))
        life_amount=life_amount+int(row['amount'])
        life_operator_rewards=life_operator_rewards+int(row['operator_rewards'])
        life_stake_rewards=life_stake_rewards+int(row['stake_rewards'])
        life_epochs_staked=life_epochs_staked+1
        updateStakeHistory[row['epoch']]={"delegatedTo":row['delegated_to_pool'].strip() if row['delegated_to_pool'] is not None else 'None',
        "amount":row['amount'],
        "delegatedToTicker":row['delegated_to_ticker'].strip() if row['delegated_to_ticker'] is not None else '',
        "lifeAmount":life_amount,
        "forecast":False,
        "lifeOperatorRewards":life_operator_rewards,
        "lifeStakeRewards":life_stake_rewards,
        "operatorRewards":int(row['operator_rewards']),
        "stakeRewards":int(row['stake_rewards']),
        "rewardsSentTo":row['rewards_sent_to'],
        "rewardAddrDetails":row['reward_address_details'],
        "pivotedToEpoch":pivot_to}
        #print(row['epoch'],life_amount,life_stake_rewards,life_operator_rewards)
        row=pg.cur1_fetchone()
    # now add in forecasts and waitstake data
    pg.cur1_execute("select * from stake_history where stake_key=%s and epoch>%s order by epoch asc",[stake_key,pivot_to])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    while row:
        updateStakeHistory[row['epoch']]={"delegatedTo":row['delegated_to_pool'].strip() if row['delegated_to_pool'] is not None else 'None',
        "amount":row['amount'],
        "delegatedToTicker":row['delegated_to_ticker'].strip() if row['delegated_to_ticker'] is not None else '',
        "forecast":True,
        "operatorRewards":int(row['operator_rewards']),
        "stakeRewards":int(row['stake_rewards']),
        "rewardsSentTo":row['rewards_sent_to'],
        "rewardAddrDetails":row['reward_address_details'],
        "pivotedToEpoch":pivot_to}
        row=pg.cur1_fetchone()
    if len(updateStakeHistory)>0:
        fb.updateFb(f"{baseNetwork}/stake_hist/{stake_key}",updateStakeHistory)
    pg.cur1_execute("insert into key_pivot_epoch (pivoted_to_epoch,life_amount,life_operator_rewards,life_stake_rewards,epochs_staked,stake_key) values(%s,%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT key_pivot_epoch_pkey DO update set pivoted_to_epoch=%s, life_amount=%s, life_operator_rewards=%s, life_stake_rewards=%s, epochs_staked=%s",[
        pivot_to,life_amount,life_operator_rewards,life_stake_rewards,life_epochs_staked,stake_key,pivot_to,life_amount,life_operator_rewards,life_stake_rewards,life_epochs_staked
    ])
    pg.conn_commit()
    print("success")