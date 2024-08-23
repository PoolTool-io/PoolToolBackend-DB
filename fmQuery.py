from config import *
import os
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




pg.cur1_execute("select epoch from blocks order by block desc limit 1")
row=pg.cur1_fetchone()
current_epoch=row['epoch']

for target_epoch in range((current_epoch-2),current_epoch):
    poolcsvheader = "\"stake_address\",\"epoch\",\"poolid\",\"stake\",\"stake_rewards\",\"operator_rewards\"\r\n"
    output_file = "allrewards" + ("" if allowChanges else "_test_") + str(target_epoch) + ".csv"
    print("processing epoch " + str(target_epoch))

    # The SQL query for COPY command
    copy_sql = f"COPY (SELECT (stake_key) AS stake_key, ('{target_epoch}') as epoch, (delegated_to_pool) as pool_id, amount, operator_rewards, stake_rewards FROM stake_history WHERE epoch={target_epoch} ORDER BY stake_key ASC) TO STDOUT WITH CSV QUOTE '\"' FORCE QUOTE stake_key, epoch, pool_id"

    # Use the custom copy_expert method with headers
    pg.cur1_copy_expert_with_headers(copy_sql, output_file, poolcsvheader)

    # Assuming aws.s3_put_object is your function to upload to S3
    aws.s3_upload_file(bn + "stats/" + output_file, output_file)
    print(bn + "stats/" + output_file)
    # Remove the local file after uploading
    try:
        os.remove(output_file)
        print(f"Local file {output_file} has been removed.")
    except OSError as e:
        print(f"Error: {output_file} : {e.strerror}")

   
if allowChanges:
    fb.updateFb(baseNetwork+"/mary_db_sync_status",{"actual_rewards_complete_epoch":(current_epoch-2),"forecast_rewards_complete_epoch":(current_epoch-1)})