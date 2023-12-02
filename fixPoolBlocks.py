from config import *
from pg_utils import *
from fb_utils import *
allowChanges=True
pg=pg_utils("fixPoolBlocks")
fb=fb_utils()


pg.cur1_execute("select epoch from blocks order by block desc limit 1")
row=pg.cur1_fetchone()
current_epoch=row['epoch']

# scan all previous epoch pools and make sure there is an entry set in their pool blocks table for 0 blocks.
pg.cur1_execute("select pool_id from (select pools.pool_id, block_count from pools left join pool_epoch_blocks on pool_epoch_blocks.pool_id=pools.pool_id and pool_epoch_blocks.epoch=%s) as a where block_count is null",[current_epoch-1])
row=pg.cur1_fetchone()
while row:
    print(row['pool_id'],"fix pool blocks")
    if allowChanges:
        fb.writeFb(baseNetwork+"/pool_stats/"+row['pool_id']+"/blocks",{str(current_epoch-1):0})
    else:
        print(baseNetwork+"/pool_stats/"+row['pool_id']+"/blocks",{str(current_epoch-1):0})
    row=pg.cur1_fetchone()

fb.writeBatch()