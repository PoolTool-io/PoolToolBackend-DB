from config import *
from pg_utils import *
from fb_utils import *
import time


pg=pg_utils("assignedPerformanceAnalysis")
fb=fb_utils()


# wait until at least slot 3600 to process.  thats one hour after the epoch switch
pg.cur1_execute("select epoch_slot,epoch from blocks order by block desc limit 1")
row=pg.cur1_fetchone()
epoch_slot=row['epoch_slot']
while epoch_slot<3600:
    print("waiting for 1 hour into epoch")
    time.sleep(60)
    pg.cur1_execute("select epoch_slot,epoch from blocks order by block desc limit 1")
    row=pg.cur1_fetchone()
    epoch_slot=row['epoch_slot']
    
    


pg.cur1_execute("select distinct pool_id from pool_assigned_slots where processed=false")
row=pg.cur1_fetchone()
while row:
    print("pool: ",row['pool_id'])
    pg.cur2_execute("select pool_epoch_blocks.epoch, jsondata, slots, processed,verified, block_count from pool_assigned_slots join pool_epoch_blocks on pool_epoch_blocks.pool_id=pool_assigned_slots.pool_id and  pool_epoch_blocks.epoch=pool_assigned_slots.epoch where cheater=false and verified=true and pool_epoch_blocks.pool_id=%s order by pool_epoch_blocks.epoch asc",[row['pool_id']])
    row2=pg.cur2_fetchone()
    sumprocessed=0
    sumblocksprocessed=0
    sumnotprocessed=0
    sumblocksnotprocessed=0
    while row2:
        if row2:
            
            if row2['processed']:
                sumprocessed+=row2['slots']
                sumblocksprocessed+=row2['block_count']
                print("epoch:",row2['epoch'])
            else:
                # we have an unprocessed epoch.  slots are already confirmed verified (from select above) so we can add it to to the totals
                if int(row2['slots'])==int(row2['block_count']):
                    sumprocessed+=row2['slots']
                    sumblocksprocessed+=row2['block_count']
                    print("slots equal.  add to total and mark as processed")
                    fb.updateFb(baseNetwork+"/stake_pools/"+row['pool_id'],{"zs":sumprocessed,"zl":sumblocksprocessed})
                    fb.updateFb(baseNetwork+"/pool_stats/"+row['pool_id']+"/assigned_slots/"+str(row2['epoch']),{"processed":True})
                    pg.cur3_execute("update pools set lifetime_per_blocks=%s, lifetime_per_slots=%s where pool_id=%s",[sumblocksprocessed,sumprocessed,row['pool_id']])
                    pg.cur3_execute("update pool_assigned_slots set processed=True where pool_id=%s and epoch=%s",[row['pool_id'],row2['epoch']])
                    pg.conn_commit()
                    print("epoch:",row2['epoch'],"slots:",row2['block_count'],"/",row2['slots'],(row2['block_count']-row2['slots']),sumblocksprocessed,sumprocessed)
            
                elif int(row2['slots'])<int(row2['block_count']):
                    print("assigned slots was less than epoch blocks.  This will occur if the user cheated OR there was a problem with the calculations.  For now assume the former and do NOT update assigned performance.")
                    fb.updateFb(baseNetwork+"/pool_stats/"+row['pool_id']+"/assigned_slots/"+str(row2['epoch']),{"processed":True,"cheater":True})
                    pg.cur3_execute("update pool_assigned_slots set processed=True, cheater=True where pool_id=%s and epoch=%s",[row['pool_id'],row2['epoch']])
                    pg.conn_commit()
                    print("epoch:",row2['epoch'],"slots:",0,"/",0,0,sumblocksprocessed,sumprocessed)
            
                else:
                    #slots are greater than blocks.  so we need to analyze
                    if row2['jsondata'] is not None and len(row2['jsondata']):
                        pg.cur3_execute("select count(block) as blks,array_agg(slot) as slotsfound from blocks where slot = any(%s) and epoch=%s",[row2['jsondata'],row2['epoch']])
                        row3=pg.cur3_fetchone()
                        if row3['slotsfound'] is None:
                            row3['slotsfound']=[]
                        row3['slotsfound'].sort()
                        row2['jsondata'].sort()
                        slotsfound=row3['slotsfound']
                        slotsjson=row2['jsondata']
                        if row3 is None:
                            print("no results")
                            print(row2['jsondata'])
                            print(row3)
                            exit()
                        if int(row3['blks'])==int(row2['slots']):
                            #all battles were slot battles.  accept 100% performance accordingly
                            sumprocessed+=row2['slots']
                            sumblocksprocessed+=row2['block_count']
                            print("slots equal including slot battles.  add to total and mark as processed")
                            fb.updateFb(baseNetwork+"/stake_pools/"+row['pool_id'],{"zs":sumprocessed,"zl":sumblocksprocessed})
                            fb.updateFb(baseNetwork+"/pool_stats/"+row['pool_id']+"/assigned_slots/"+str(row2['epoch']),{"processed":True})
                            pg.cur3_execute("update pools set lifetime_per_blocks=%s, lifetime_per_slots=%s where pool_id=%s",[sumblocksprocessed,sumprocessed,row['pool_id']])
                            pg.cur3_execute("update pool_assigned_slots set processed=True where pool_id=%s and epoch=%s",[row['pool_id'],row2['epoch']])
                            pg.conn_commit()
                            print("epoch:",row2['epoch'],"slots:",row2['block_count'],"/",row2['slots'],(row2['block_count']-row2['slots']),sumblocksprocessed,sumprocessed)
            
                        else:
                            finalslotsfound=len(slotsfound)
                            print(row2['slots'],"vs", finalslotsfound)
                            
                            for slot in slotsjson:
                                if slot not in slotsfound:
                                    print("searching for what happened in slot ", slot)
                                    # search for orphan?
                                    pg.cur3_execute("select block_no from competitive_blocks where block_slot_no=%s and pool_id=%s",[slot,row['pool_id']])
                                    row3=pg.cur3_fetchone()
                                    if row3:
                                        print(row3)
                                        finalslotsfound+=1
                                        print("orphaned block found.  count as made")
                                        
                                    else:
                                        print("cannot find block in competitive slots.  we have to assume its not there")
                            sumprocessed+=finalslotsfound
                            sumblocksprocessed+=row2['block_count']
                            fb.updateFb(baseNetwork+"/stake_pools/"+row['pool_id'],{"zs":sumprocessed,"zl":sumblocksprocessed})
                            fb.updateFb(baseNetwork+"/pool_stats/"+row['pool_id']+"/assigned_slots/"+str(row2['epoch']),{"processed":True})
                            pg.cur3_execute("update pool_assigned_slots set processed=True where pool_id=%s and epoch=%s",[row['pool_id'],row2['epoch']])
                            pg.cur3_execute("update pools set lifetime_per_blocks=%s, lifetime_per_slots=%s where pool_id=%s",[sumblocksprocessed,sumprocessed,row['pool_id']])
                            pg.conn_commit()
                            print("epoch:",row2['epoch'],"slots:",sumprocessed,"/",sumblocksprocessed,(row2['block_count']-finalslotsfound),sumblocksprocessed,sumprocessed)
                    

                            




                    elif row2['slots']>0:
                        print("we have no json data but we do have slots.  we cannot do anything with this so just mark as processed")
                        # we have no json data but we do have slots.  we cannot do anything with this so just mark as processed
                        fb.updateFb(baseNetwork+"/pool_stats/"+row['pool_id']+"/assigned_slots/"+str(row2['epoch']),{"processed":True})
                        pg.cur3_execute("update pool_assigned_slots set processed=True where pool_id=%s and epoch=%s",[row['pool_id'],row2['epoch']])
                        pg.conn_commit()
                        print("epoch:",row2['epoch'],"slots:",0,"/",0,0,sumblocksprocessed,sumprocessed)
            

    


                
        row2=pg.cur2_fetchone()
    fb.updateFb(baseNetwork+"/stake_pools/"+row['pool_id'],{"zs":sumprocessed,"zl":sumblocksprocessed})
    pg.cur3_execute("update pools set lifetime_per_blocks=%s, lifetime_per_slots=%s where pool_id=%s",[sumblocksprocessed,sumprocessed,row['pool_id']])
    pg.conn_commit()
    row=pg.cur1_fetchone()
fb.updateFb(baseNetwork+"/epoch_processing",{"targetEpoch":0,"epochPricesDone":0,"epochParamsEpochDone":0,"waitStakeEpochDone":0,"rewardProcessingEpochDone":0})
