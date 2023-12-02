from config import *
from aws_utils import *
from pg_utils import *
from fb_utils import *
allowChanges=True
pg=pg_utils("fmQuery")
fb=fb_utils()
aws=aws_utils()
if baseNetwork=="Mainnet":
    bn=""
else:
    bn=baseNetwork+"/"  

poolcsvheader = "\"stake_address\",\"epoch\",\"poolid\",\"stake\",\"stake_rewards\",\"operator_rewards\"\r\n"


pg.cur1_execute("select epoch from blocks order by block desc limit 1")
row=pg.cur1_fetchone()
current_epoch=row['epoch']

for target_epoch in range((current_epoch-2),current_epoch):
    poolcsvdata = poolcsvheader
    print("processing epoch" + str(target_epoch))
    pg.cur1_execute("select stake_key,amount, delegated_to_pool, amount, operator_rewards, stake_rewards from stake_history where epoch=%s",[target_epoch])
    row=pg.cur1_fetchone()
    while row:
        
        dataline = "\""+row['stake_key']+"\",\""+str(target_epoch)+"\",\""+row['delegated_to_pool']+"\","+str(int(row['amount']))+","+str(int(row['stake_rewards']))+","+str(int(row['operator_rewards']))+"\r\n"
        poolcsvdata = poolcsvdata + dataline

        row=pg.cur1_fetchone()
    aws.s3_put_object(bn+"stats/allrewards"+("" if allowChanges else "_test_")+str(target_epoch)+".csv",poolcsvdata)
    print(bn+"stats/allrewards"+("" if allowChanges else "_test_")+str(target_epoch)+".csv")
if allowChanges:
    fb.updateFb(baseNetwork+"/mary_db_sync_status",{"actual_rewards_complete_epoch":(current_epoch-2),"forecast_rewards_complete_epoch":(current_epoch-1)})