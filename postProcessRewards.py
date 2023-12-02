from config import *
from pg_utils import *
from fb_utils import *
from aws_utils import *
from pivotRewardsFunction import *
import os
import time
allowChanges=True
sendRewardUpdate=True
pg=pg_utils("postProcessRewards")
fb=fb_utils()
aws=aws_utils()

addressestoprocess=[]

pg.cur1_execute("select epoch from blocks order by block desc limit 1")
row=pg.cur1_fetchone()

target_epoch = int(row['epoch'])-1

if baseNetwork=="Mainnet":
    if True:
        legacy_users=fb.getKey(baseNetwork+"/legacy_users")
        for randcode in legacy_users:
            
            if 'accounts' in legacy_users[randcode]:
                if type(legacy_users[randcode]['accounts']) is dict:
                    for ident in legacy_users[randcode]['accounts']:
                        
                        if ident is not None and 'stakeKeyHash' in legacy_users[randcode]['accounts'][ident]:
                            addressestoprocess = addressestoprocess + [legacy_users[randcode]['accounts'][ident]['stakeKeyHash']]
                            
                else:
                    for ident in range(len(legacy_users[randcode]['accounts'])):
                    
                        if legacy_users[randcode]['accounts'][ident] is not None and 'stakeKeyHash' in legacy_users[randcode]['accounts'][ident]:
                            addressestoprocess = addressestoprocess + [legacy_users[randcode]['accounts'][ident]['stakeKeyHash']]
        
        users_privmeta=fb.getKey(baseNetwork+"/users/privMeta")

        for userid in users_privmeta:
            if 'mySubscriptions' in users_privmeta[userid] and 'myAddresses' in users_privmeta[userid]:
                addressestoprocess = addressestoprocess + (list(users_privmeta[userid]['myAddresses'].keys()))
            else:
                db.reference(baseNetwork+"/users/privMeta").child(userid).update({"mySubscriptions":{"feature_rewards_tracking":3.154e+10+(int(time.time())*1000)}})

        addressestoprocess = list( dict.fromkeys(addressestoprocess) )
        
    
        total=len(addressestoprocess)
        starttime = time.time()
        i=0
        for stake_key in addressestoprocess:
            i=i+1

            fctime=(time.time()-starttime)*total/i
            print(i,"/",total,"elapsed: ",time.time()-starttime,fctime)
            pivotRewardsArchived(stake_key,pg,fb,baseNetwork)
            
    
    pg.cur1_execute("select reserves from epoch_params where epoch=%s",[target_epoch])
    row=pg.cur1_fetchone()
    if row is not None and int(row['reserves'])>0:

        if sendRewardUpdate:
            aws.awsbroadcast({"type": "reward",'data': {'epoch': target_epoch-1}})

            aws.awsbroadcast({"type": "epoch_summary","data": {"d": 0,"epoch": target_epoch, "reserves":int(row['reserves'])}})

            print({"type": "epoch_summary","data": {"d": 0,"epoch": target_epoch, "reserves":int(row['reserves'])}})
            print("postProcessRewards Successfully completed")
    if allowChanges:
        fb.getReference(baseNetwork+"/mary_db_sync_status").update({"waitstake_complete_epoch":(target_epoch+2)})
        
