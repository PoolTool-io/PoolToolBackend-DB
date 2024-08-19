import hashlib
import binascii
import time
from config import *
from pt_utils import *
import subprocess, platform
from fb_utils import fb_utils
from aws_utils import aws_utils
from pg_utils import *
fb=fb_utils()

aws=aws_utils()
pg=pg_utils("consumerTools")
watchAddresses=['addr1qykeqj78jj98ulnkmjt0dxh5ya4pcw27qy7l4z5dkzyr5grszxlk5cj4w7utxxngjzjc2rc0ht9hmxr0zh493gyyks0qh0cexs','addr1q9hpgu76s9usmqg8kwjtehs32ux6pkq4e394hzvk7ffnd50czdv6hfcmfgr09y6lpvlpl5nfnemg8c768kvl0vada5tqdmvnmd']

def send_block_production_zapier(pool_id,poolTicker,poolName,epochBlocks,totalBlocks,blockNumber,blockSlot,blockEpoch):

    package={
        "height": blockNumber,
        "slot":blockSlot,
        "epoch":blockEpoch,
        "epochBlocks":epochBlocks,
        "totalBlocks":totalBlocks,
        "poolName":str(poolName).strip(),
        "poolTicker":str(poolTicker).strip(),
        "pool_id":pool_id
    }
    

    pg.cur1_execute("select zapier_url from zapier_triggers where trigger_type='new_block' and (data_key=%s or data_key='0')",[pool_id])
    row=pg.cur1_fetchone()
    while row:
        result=trigger_zapier_hook(row['zapier_url'],package)
        if 'failure' in result or ('status' in result and result['status']!='success'):
            # log to the retry queue for later
            pg.cur2_execute("insert into zapier_retry_triggers (zapier_url,package) values(%s,%s)",[row['zapier_url'],Json(package)])
            pg.conn_commit()
        row=pg.cur1_fetchone()

def send_block_production_notification(pool_id,poolTicker,poolName,epochBlocks,totalBlocks,blockNumber,blockSlot,blockEpoch):
    if poolTicker =='':
        name = "a pool"
    else:
        name="[" + str(poolTicker).strip() + "] " + str(poolName).strip()
    body = "\uD83D\uDEE0 Blocks this epoch: " + str(epochBlocks) + "\n" + "\uD83E\uDDF1 Total blocks: " + str(totalBlocks) + "\n\n" + "Block number: " + str(blockNumber) + "\n" + "Slot: " + str(blockSlot) + "\n" +  "Epoch: " + str(blockEpoch)
    title = "New block by " + name + "! \uD83D\uDD25"
    gsmtokens=fb.getKey(baseNetwork + "/legacy_alerts/" + pool_id+"/block_production")
    if gsmtokens is not None:
        for gsmtoken in gsmtokens.keys():
            try:
                fb.push_notification(title,body,gsmtoken)
            except Exception as e:
                fb.deleteFb(baseNetwork + "/legacy_alerts/" + pool_id+"/block_production/"+gsmtoken)
                print("token deleted")
                print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

def consumeTransaction(args,current_epoch):
    ex_units_mem=0
    ex_units_steps=0
    if 'plutus_redeemers' in args['transaction'] and args['transaction']['plutus_redeemers'] is not None:
        for redeemer in args['transaction']['plutus_redeemers']:
            if 'ex_units_mem' in redeemer and redeemer['ex_units_mem'] is not None:
                ex_units_mem+=redeemer['ex_units_mem']
            if 'ex_units_steps' in redeemer and redeemer['ex_units_steps'] is not None:
                ex_units_steps+=redeemer['ex_units_steps']
                
    pg.cur1_execute("update blocks set ex_units_mem=COALESCE(blocks.ex_units_mem,0)+%s, ex_units_steps=COALESCE(blocks.ex_units_steps,0)+%s, fees=blocks.fees+%s where block=%s",[ex_units_mem,ex_units_steps,args['transaction']['fee'],args['context']['block_number']])
    pg.cur1_execute("insert into epoch_params (epoch_feess,epoch) values(%s,%s) ON CONFLICT ON CONSTRAINT epparams_idx DO UPDATE set epoch_feess=epoch_params.epoch_feess+%s",[args['transaction']['fee'],current_epoch,args['transaction']['fee']])
    pg.conn_commit()

    if 'withdrawals' in args['transaction'] and args['transaction']['withdrawals'] is not None:
        pass
 

def consumeTxInput(args,current_epoch):
    pass


def consumeTxOutput(args,current_epoch):
 
    pg.cur1_execute("update blocks set output=blocks.output+%s where block=%s",[int(args['tx_output']['amount']),int(args['context']['block_number'])])
    pg.cur1_execute("insert into epoch_params (epoch_output,epoch) values(%s,%s) ON CONFLICT ON CONSTRAINT epparams_idx DO UPDATE set epoch_output=epoch_params.epoch_output+%s",[int(args['tx_output']['amount']),current_epoch,int(args['tx_output']['amount'])])
    pg.conn_commit()

def consumeBlockEnd(args,current_epoch,replay=False):
    #get all the final details about the block and write out everywhere.
    allowChanges=True
    if allowChanges:
        print("updating last details to fb")
        pg.cur1_execute("""select 
        blocks.ex_units_mem, 
        blocks.ex_units_steps,
        blocks.output, 
        blocks.fees, 
        blocks.body_size, 
        blocks.epoch_slot, 
        blocks.slot, 
        blocks.timestamp, 
        blocks.transactions, 
        blocks.cbor_size, 
        blocks.block,
        blocks.epoch,
        blocks.hash,
        blocks.pool_id, 
        pools.pool_name, 
        pools.ticker 
        from blocks left join pools on pools.pool_id=blocks.pool_id where blocks.block=%s""",[int(args['context']['block_number'])])
        row=pg.cur1_fetchone()
        pg.conn_commit()
        if row and row is not None:
            blockdata = {
                "block":int(row['block']),
                "epoch":int(row['epoch']) if 'epoch' in row and row['epoch'] is not None else 0,
                "fees":int(row['fees']) if 'fees' in row and row['fees'] is not None else 0,
                "hash":row['hash'] if 'hash' in row and row['hash'] is not None else '',
                "leaderPoolId":row['pool_id'] if 'pool_id' in row and row['pool_id'] is not None else '',
                "leaderPoolName":row['pool_name'].strip() if 'pool_name' in row and row['pool_name'] is not None else '',
                "leaderPoolTicker":row['ticker'].strip() if 'ticker' in row and row['ticker'] is not None else '',
                "output":int(row['output']) if 'output' in row and row['output'] is not None else 0,
                "size":int(row['body_size']) if 'body_size' in row and row['body_size'] is not None else 0,
                "cslot":int(row['slot']) if 'slot' in row and row['slot'] is not None else 0,
                "slot":int(row['epoch_slot']) if 'epoch_slot' in row and row['epoch_slot'] is not None else 0,
                "time":int(row['timestamp'])*1000 if 'timestamp' in row and row['timestamp'] is not None else 0,
                "transactions":int(row['transactions']) if 'transactions' in row and row['transactions'] is not None else 0,
                "cbor_size_bytes":int(row['cbor_size']) if 'cbor_size' in row and row['cbor_size'] is not None else 0,
                "ex_units_mem":int(row['ex_units_mem']) if 'ex_units_mem' in row and row['ex_units_mem'] is not None else 0,
                "ex_units_steps":int(row['ex_units_steps']) if 'ex_units_steps' in row and row['ex_units_steps'] is not None else 0
            }
            fb.updateFb(baseNetwork+"/recent_block",blockdata)
            fb.setFb(baseNetwork+"/blocks/"+str(row['epoch'])+"/"+str(row['block']),blockdata)
            print("blocks done")
            if 'pool_id' in row and row['pool_id'] != '':
                fb.setFb(baseNetwork+"/pool_blocks/"+str(row['pool_id'])+"/"+str(row['epoch'])+"/"+str(row['block']),blockdata)
                print("pool blocks done")
                fb.setFb(baseNetwork+"/pool_blocks_ne/"+str(row['pool_id'])+"/"+str(row['block']),blockdata) #TODO:  why is this necessary?
                print("pool blocks ne done")
            aws.dump_s3(blockdata,baseNetwork+'/blocks/'+str(row['epoch'])+"/"+str(row['block'])+".json")
            print("aws s3 done")
        try:
            pg.cur1_execute("select blocks.epoch, blocks.epoch_slot, blocks.block, blocks.slot, blocks.timestamp, blocks.timestamp-prevblock.timestamp as timedelta from blocks left join blocks prevblock on prevblock.block=blocks.block-1 where blocks.epoch>468 and blocks.lagprev is null order by block asc ")
            rows=pg.cur1_fetchall()
            pg.conn_commit()
            for row in rows:
                #print(row['timedelta'])
                pg.cur2_execute("update blocks set lagprev=%s where block=%s",[row['timedelta'],row['block']])
                pg.conn_commit()
            print("updated block lag")
        except:
            print("could not update block lag")

def consumeBlock(args,current_epoch,replay=False):
    global vkeylookup
    
    if args['block']['issuer_vkey'] not in vkeylookup:
        h = hashlib.blake2b(digest_size=28)
        h.update(binascii.unhexlify(args['block']['issuer_vkey']))
        vkeylookup[args['block']['issuer_vkey']]=h.hexdigest()
        
    print("inserting")
    args['cbor_size_bytes']=len(args['block']['cbor_hex'])/2
    pg.cur1_execute("insert into blocks (block,slot, timestamp, hash, transactions, epoch, era, epoch_slot, vkey, cbor_size, body_size,pool_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
    (args['context']['block_number'],args['context']['slot'],args['context']['timestamp'],args['context']['block_hash'],args['block']['tx_count'],args['block']['epoch'],
    args['block']['era'],args['block']['epoch_slot'],args['block']['issuer_vkey'],args['cbor_size_bytes'],args['block']['body_size'],vkeylookup[args['block']['issuer_vkey']]))
    if not replay:
        pg.cur1_execute("insert into sync_status (key,block,tx_fingerprints) values('height',%s,array[]::varchar[]) ON CONFLICT ON CONSTRAINT syncstatus_key DO UPDATE set block=%s, tx_fingerprints=array[]::varchar[]",(args['context']['block_number'],args['context']['block_number']))
    
    print("insert complete")
    
    pg.cur1_execute("select pool_id from pools where pool_id=%s",[vkeylookup[args['block']['issuer_vkey']]])
    row=pg.cur1_fetchone()
    if row is None:
        #create the genesis pool recoreds
        pg.cur1_execute("insert into pools (pool_id, genesis) values (%s,true)",[vkeylookup[args['block']['issuer_vkey']]])
        pg.conn_commit()
    if not replay or replay and int(current_epoch)==int(args['block']['epoch']):
        pg.cur1_execute("update pools set life_blocks=life_blocks+1,  epoch_blocks = (CASE WHEN epoch_blocks_epoch=%s THEN epoch_blocks+1 ELSE 1 END),  epoch_blocks_epoch=%s where pool_id=%s",(int(args['block']['epoch']),int(args['block']['epoch']),vkeylookup[args['block']['issuer_vkey']]))
    else:
        # if replay then we only update epoch blocks if we are current IN the epoch
        pg.cur1_execute("update pools set life_blocks=life_blocks+1, where pool_id=%s",(vkeylookup[args['block']['issuer_vkey']]))
    print("pool update complete")
    pg.cur1_execute("insert into pool_epoch_blocks (epoch, pool_id, block_count) values(%s,%s,1) ON CONFLICT ON CONSTRAINT peb_idx DO UPDATE set block_count=pool_epoch_blocks.block_count + 1",[int(args['block']['epoch']),vkeylookup[args['block']['issuer_vkey']]])
    pg.cur1_execute("insert into epoch_params (epoch_blocks,epoch,epoch_cbor_size,last_block_time,epoch_tx_count) values(1,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT epparams_idx DO UPDATE set epoch_blocks=epoch_params.epoch_blocks+1, epoch_cbor_size=epoch_params.epoch_cbor_size+%s,last_block_time=%s, epoch_tx_count=epoch_params.epoch_tx_count+%s",
    [current_epoch,args['cbor_size_bytes'],args['context']['timestamp'],args['block']['tx_count'],args['cbor_size_bytes'],args['context']['timestamp'],args['block']['tx_count']])
    print("final commit")
    pg.conn_commit()
    print("final commit done")
    #postgres update complete
    allowChanges=True
    sendTelegramBotMessages=True
    lastkey=fb.getKey(baseNetwork+"/mary_db_sync_status")
    #lastkey['block_no'] and lastkey['epoch_no'] are the last fully syncd blocks from thd old method.  we need to make sure we only process stuff below if the new block is greater than these values
    if int(args['context']['block_number'])>int(lastkey['block_no']):
        if allowChanges:
            pg.cur1_execute("select ticker, pool_name,life_blocks, epoch_blocks, epoch_blocks_epoch from pools where pool_id=%s",[vkeylookup[args['block']['issuer_vkey']]])
            row=pg.cur1_fetchone()
            pg.conn_commit()
            if row:
                poolticker=row['ticker']
                poolname=row['pool_name']
                pool_life_blocks=row['life_blocks']
                pool_epoch_blocks=row['epoch_blocks']
                pool_epoch_blocks_epoch=row['epoch_blocks_epoch']
            else:
                poolticker=''
                poolname=''
                pool_life_blocks=1
                pool_epoch_blocks=1
                pool_epoch_blocks_epoch=int(args['block']['epoch'])
            blockdata = {
                "block":int(args['context']['block_number']),
                "epoch":int(args['block']['epoch']),
                #"fees":int(row['fees']) if row['fees'] is not None else 0,
                "hash":args['context']['block_hash'],
                "leaderPoolId":vkeylookup[args['block']['issuer_vkey']],
                "leaderPoolName":poolname,
                "leaderPoolTicker":poolticker,
                #"output":int(row['output']) if row['output'] is not None else 0,
                "size":int(args['block']['body_size']),
                "slot":int(args['block']['epoch_slot']),
                "time":args['context']['timestamp']*1000,
                "transactions":int(args['block']['tx_count']),
                "cbor_size_bytes":args['cbor_size_bytes']
            }
            fb.updateFb(baseNetwork+"/stake_pools/"+str(vkeylookup[args['block']['issuer_vkey']]),{"b":pool_epoch_blocks,"eb":int(args['block']['epoch']),"l":pool_life_blocks})
            pg.cur1_execute("select block_count from pool_epoch_blocks where pool_id=%s and epoch=%s",[vkeylookup[args['block']['issuer_vkey']],args['block']['epoch']])
            row=pg.cur1_fetchone()
            pg.conn_commit()
            if row:
                fb.setFb(baseNetwork+"/pool_stats/"+str(vkeylookup[args['block']['issuer_vkey']])+"/blocks/"+str(args['block']['epoch']),row['block_count'])


            
            if sendTelegramBotMessages:
                sendstuff={"type":"block_minted","data":  {"pool":vkeylookup[args['block']['issuer_vkey']],"nbe":(pool_epoch_blocks), "nb":(pool_life_blocks)}}
                aws.awsbroadcast(sendstuff)
                try:
                    send_block_production_notification(vkeylookup[args['block']['issuer_vkey']],poolticker,poolname,pool_epoch_blocks,pool_life_blocks,args['context']['block_number'],args['block']['epoch_slot'],args['block']['epoch'])
                except Exception as e:
                    print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
                try:
                    send_block_production_zapier(vkeylookup[args['block']['issuer_vkey']],poolticker,poolname,pool_epoch_blocks,pool_life_blocks,args['context']['block_number'],args['block']['epoch_slot'],args['block']['epoch'])
                except Exception as e:
                    print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            award={}
            award['value'] = int(args['context']['timestamp'])
            blockno = int(args['context']['block_number'])
            award['hash'] = args['context']['block_hash']
            if pool_life_blocks==1:
                # give an award for first block
                award['award']='LIFETIME_BLOCKS_1'
                # award['value'] = int(args['context']['timestamp'])
                # blockno = int(args['context']['block_number'])
                # award['hash'] = args['context']['block_hash']
                award['text'] = f'# {blockno:,}<br/>First Mainnet Block'
                award['type'] = 'MAINBLOCKS'
                
                fb.pushFb(baseNetwork+"/awards/cardano/"+str(vkeylookup[args['block']['issuer_vkey']])+"/",award)
                if sendTelegramBotMessages:
                    aws.awsbroadcast({"type":"award","data":  {"pool":vkeylookup[args['block']['issuer_vkey']],"award":award}})
                #exit()
            elif pool_life_blocks==10:
                # give an award for 10th block
                award['award']='LIFETIME_BLOCKS_10'
                # award['value'] = int(args['context']['timestamp'])
                # blockno = int(args['context']['block_number'])
                # award['hash'] = args['context']['block_hash']
                award['text'] = f'# {blockno:,}<br/>10th Mainnet Block'
                award['type'] = 'MAINBLOCKS'
                fb.pushFb(baseNetwork+"/awards/cardano/"+str(vkeylookup[args['block']['issuer_vkey']])+"/",award)
                
                if sendTelegramBotMessages:
                    aws.awsbroadcast({"type":"award","data":  {"pool":vkeylookup[args['block']['issuer_vkey']],"award":award}})
            elif pool_life_blocks==100:
                # give an award for 100th block
                award['award']='LIFETIME_BLOCKS_100'
                # award['value'] = int(args['context']['timestamp'])
                # blockno = int(args['context']['block_number'])
                # award['hash'] = args['context']['block_hash']
                award['text'] = f'# {blockno:,}<br/>100th Mainnet Block'
                award['type'] = 'MAINBLOCKS'
                fb.pushFb(baseNetwork+"/awards/cardano/"+str(vkeylookup[args['block']['issuer_vkey']])+"/",award)
                if sendTelegramBotMessages:
                    aws.awsbroadcast({"type":"award","data":  {"pool":vkeylookup[args['block']['issuer_vkey']],"award":award}})
            elif pool_life_blocks==1000:
                # give an award for 1000th block
                # blockno = row['block_no']
                award['award']='LIFETIME_BLOCKS_1K'
                # award['value'] = int(row['time'].timestamp())
                # award['hash'] = row['block_hash']
                award['text'] = f'# {blockno:,}<br/>1000th Mainnet Block'
                award['type'] = 'MAINBLOCKS'
                fb.pushFb(baseNetwork+"/awards/cardano/"+str(vkeylookup[args['block']['issuer_vkey']])+"/",award)
                if sendTelegramBotMessages:
                    aws.awsbroadcast({"type":"award","data":  {"pool":vkeylookup[args['block']['issuer_vkey']],"award":award}})
            elif pool_life_blocks==10000:
                # give an award for 10000th block
                # blockno = row['block_no']
                award['award']='LIFETIME_BLOCKS_10K'
                # award['value'] = int(row['time'].timestamp())
                # award['hash'] = row['block_hash']
                award['text'] = f'# {blockno:,}<br/>10,000th Mainnet Block'
                award['type'] = 'MAINBLOCKS'
                fb.pushFb(baseNetwork+"/awards/cardano/"+str(vkeylookup[args['block']['issuer_vkey']])+"/",award)
                if sendTelegramBotMessages:
                    aws.awsbroadcast({"type":"award","data":  {"pool":vkeylookup[args['block']['issuer_vkey']],"award":award}})
            
            pg.cur1_execute("select epoch_output, epoch_tx_count, epoch_blocks,epoch_feess, epoch_cbor_size, last_block_time from epoch_params where epoch=%s",[args['block']['epoch']])
            row=pg.cur1_fetchone()
            pg.conn_commit()
            if row:
            
                epoch_record = {
                    "blocks" : int(row['epoch_blocks']),
                    "blocksGenesis":0,
                    "epoch" : args['block']['epoch'],
                    "fees" : int(row['epoch_feess']), # note this is fees through the end of the LAST block, not current one due to how we process tx's after block
                    "lastBlockTime" : int(args['context']['timestamp'])*1000,
                    "epochCborSize":int(row['epoch_cbor_size']),
                    "transactions":int(row['epoch_tx_count']),
                    "totalOutput" : int(row['epoch_output'])
                }
                fb.updateFb(baseNetwork+"/epochs/"+str(args['block']['epoch']),epoch_record)
                
  
            #finally update to maintain sync with old sync method
            fb.updateFb(baseNetwork+"/mary_db_sync_status",{"block_no":args['context']['block_number'],"epoch_no":args['block']['epoch']})
        else:
            print("changes not allowed, skipping")
    else:
        print("mary_db_sync_status says don't process this block")




def epochProcessing(args,current_epoch):
    #args are assumed to be the block args from the new epoch
    print("new epoch")
    
    try: shelllog
    except NameError:
        print("logging not defined yet")
    else: 
        shelllog.close()
        
    shelllog = open("logs/postprocess.log", 'wb') # Use this in Python < 3.3
    # Python >= 3.3 has subprocess.DEVNULL
    subprocess.Popen(['nohup','python3','-u', 'epoch_processing.py'], shell=False,stdout=shelllog,stderr=shelllog)
    print("spawned epoch_processing process")

    print("updating pools")
    
    pg.cur1_execute("select pool_id from pools where fretired is not null and fretired_epoch <= %s",[args['block']['epoch']])
    row=pg.cur1_fetchone()
    while row:
        fb.updateFb(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"d":True})
        row=pg.cur1_fetchone()
    pg.conn_commit()
    
    pg.cur1_execute("select pool_id,fpledge from pools where fpledge is not null and fpledge_epoch <= %s",[args['block']['epoch']])
    row=pg.cur1_fetchone()
    while row:
        fb.updateFb(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"p":int(row['fpledge'])})
        row=pg.cur1_fetchone()
    pg.conn_commit()

    pg.cur1_execute("select pool_id,fcost from pools where fcost is not null and fcost_epoch <= %s",[args['block']['epoch']])
    row=pg.cur1_fetchone()
    while row:
        fb.updateFb(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"f":int(row['fcost'])})
        row=pg.cur1_fetchone()
    pg.conn_commit()

    pg.cur1_execute("select pool_id,fmargin from pools where fmargin is not null and fmargin_epoch <= %s",[args['block']['epoch']])
    row=pg.cur1_fetchone()
    while row:
        fb.updateFb(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"m":round(row['fmargin']*100,2)})
        row=pg.cur1_fetchone()
    pg.conn_commit()

    pg.cur1_execute("update pools set retired=true,fretired=null, fretired_epoch=null where fretired=true and fretired_epoch is not null and fretired_epoch <= %s",[args['block']['epoch']])
    pg.cur1_execute("update pools set pledge=fpledge, fpledge=null, fpledge_epoch=null where fpledge_epoch is not null and fpledge_epoch >=%s",[args['block']['epoch']])
    pg.cur1_execute("update pools set cost=fcost, fcost=null, fcost_epoch=null where fcost_epoch is not null and fcost_epoch >=%s",[args['block']['epoch']])
    pg.cur1_execute("update pools set margin=fmargin, fmargin=null, fmargin_epoch=null where fmargin_epoch is not null and fmargin_epoch >=%s",[args['block']['epoch']])
    pg.conn_commit()


def consumeStakeDeRegistration(args,current_epoch,replay=False):
    pg.cur1_execute("select epoch from blocks where block=%s",[args['context']['block_number']])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row and "epoch" in row:
        epoch=row['epoch']
    if 'AddrKeyhash' in args['stake_deregistration']['credential']:
        AddrHash = args['stake_deregistration']['credential']['AddrKeyhash']
        AddrType = 'AddrKeyhash'
    elif 'Scripthash' in args['stake_deregistration']['credential']:
        AddrHash = args['stake_deregistration']['credential']['Scripthash']
        AddrType = 'Scripthash'
    else:
        print(args)
        print("unable to find Scripthash or AddrKeyhash")
        exit()
    pg.cur1_execute("select pool_id from live_delegators where stake_key=%s",[AddrHash])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row and "pool_id" in row:
        from_pool_id=row['pool_id']
    else:
        from_pool_id=None
    to_pool_id=None
    pg.cur1_execute("insert into delegation_updates (stake_key, from_pool_id, to_pool_id, block, slot, timestamp, epoch_effective,addr_type) values(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING ",[
        AddrHash,from_pool_id, to_pool_id,args['context']['block_number'],args['context']['slot'],
        args['context']['timestamp'],(epoch+2),AddrType
    ])
    pg.cur1_execute("insert into live_delegators (stake_key, pool_id,addr_type,dirty) values(%s,%s,%s,true) ON CONFLICT ON CONSTRAINT stake_key_idx DO UPDATE SET pool_id=%s",[
        AddrHash,None,AddrType,None
    ])
    if not replay:
        pg.cur1_execute("update sync_status set tx_fingerprints = array_append(tx_fingerprints,%s)  where key='height'",[args['fingerprint']])
    pg.conn_commit()

def consumeStakeDelegation(args,current_epoch,replay=False):
    pg.cur1_execute("select epoch from blocks where block=%s",[args['context']['block_number']])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row and "epoch" in row:
        epoch=row['epoch']
    
    if 'AddrKeyhash' in args['stake_delegation']['credential']:
        AddrHash = args['stake_delegation']['credential']['AddrKeyhash']
        AddrType = 'AddrKeyhash'
    elif 'Scripthash' in args['stake_delegation']['credential']:
        AddrHash = args['stake_delegation']['credential']['Scripthash']
        AddrType = 'Scripthash'
    else:
        print(args)
        print("unable to find Scripthash or AddrKeyhash")
        exit()
        
    pg.cur1_execute("select pool_id from live_delegators where stake_key=%s",[AddrHash])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row and "pool_id" in row:
        from_pool_id=row['pool_id']
    else:
        from_pool_id=None
    pg.cur1_execute("insert into delegation_updates (stake_key, from_pool_id, to_pool_id, block, slot, timestamp, epoch_effective,addr_type) values(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING ",[
        AddrHash,from_pool_id, args['stake_delegation']['pool_hash'],args['context']['block_number'],args['context']['slot'],
        args['context']['timestamp'],(epoch+2),AddrType
    ])
    
    pg.cur1_execute("insert into live_delegators (stake_key, pool_id,addr_type,dirty) values(%s,%s,%s,true) ON CONFLICT ON CONSTRAINT stake_key_idx DO UPDATE SET pool_id=%s",[
        AddrHash,args['stake_delegation']['pool_hash'],AddrType,args['stake_delegation']['pool_hash']
    ])
    if not replay:
        pg.cur1_execute("update sync_status set tx_fingerprints = array_append(tx_fingerprints,%s)  where key='height'",[args['fingerprint']])
    pg.conn_commit()
      

def consumePoolRegistration(args,current_epoch,replay=False):
    sendTelegramBotMessages=True
    allowChanges=True
    pg.cur1_execute("select epoch from blocks where block=%s",[args['context']['block_number']])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row and "epoch" in row:
        epoch=row['epoch']
    else:
        print("cannot find block..  halting.. " + str(args['context']['block_number']))
        exit()
    
    if  args['pool_registration']['pledge'] > 45000000000000000: # pledge
        args['pool_registration']['pledge'] = 45000000000000000
    if  args['pool_registration']['pledge'] < 0:
        args['pool_registration']['pledge'] = 0
    if  args['pool_registration']['cost'] > 45000000000000000: # cost
        args['pool_registration']['cost'] = 45000000000000000
    if  args['pool_registration']['cost'] < 0:
        args['pool_registration']['cost'] = 0
    if args['pool_registration']['reward_account'] is not None and len(args['pool_registration']['reward_account'])==58 and args['pool_registration']['reward_account'][:2]=="e1":
        args['pool_registration']['reward_account']=args['pool_registration']['reward_account'][2:]
    owners=list((args['pool_registration']['pool_owners']))
    ownersnew=[]
    for stakekey in owners:
        if stakekey is not None and len(stakekey)==58 and stakekey[:2]=="e1":
            ownersnew.append(stakekey[2:])
        else:
            ownersnew.append(stakekey)
    ownersnew.sort()
    args['pool_registration']['pool_owners']=ownersnew
    # diff the update
    changes=[]
    pool_update={
        "pool_name":'',
        "ticker":'',
        "pool_id":args['pool_registration']['operator'],
        "pledge":args['pool_registration']['pledge'],
        "cost":args['pool_registration']['cost'],
        "margin":args['pool_registration']['margin'],
        "reward_account":args['pool_registration']['reward_account'],
        "pool_owners":list((args['pool_registration']['pool_owners'])),
        "relays":list((args['pool_registration']['relays'])),
        "metadata":args['pool_registration']['pool_metadata'],
        "metadata_hash":args['pool_registration']['pool_metadata_hash'],
        "fpledge":None,
        "fmargin":None,
        "fcost":None,
        "fpledge_epoch":None,
        "fmargin_epoch":None,
        "fcost_epoch":None}
    pg.cur1_execute("select * from pools where pool_id=%s",[args['pool_registration']['operator']])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    updatestring=''
    if not row:
        changes.append({'type':'registration','old':None,'new':None})
        # this is a new pool registration, insert some necessary records into fb
        stake_pool_data={
            "id":args['pool_registration']['operator'],
            "d":False,
            "r":1000,
            "m": (round(args['pool_registration']['margin']*100,2)),
            "f": int(args['pool_registration']['cost']),
            "p": int(args['pool_registration']['pledge']),
            "fm": (round(args['pool_registration']['margin']*100,2)),
            "ff": int(args['pool_registration']['cost']),
            "fp": int(args['pool_registration']['pledge']),
            "x":False
        }

        fb.updateFb(baseNetwork+"/stake_pools/"+str(args['pool_registration']['operator']),stake_pool_data)

        pool_stats_data={
            "firstEpoch":(epoch+3),
            "owners":list((args['pool_registration']['pool_owners'])),
            "reward_address":args['pool_registration']['reward_account']
        }
        fb.updateFb(baseNetwork+"/pool_stats/"+str(args['pool_registration']['operator']),pool_stats_data)
        db_pool_details=pool_update
        #fpledge=%s, fmargin=%s, fcost=%s, fpledge_epoch=%s, fmargin_epoch=%s, fcost_epoch=%s,
    else:
        db_pool_details={
            "pool_name":row['pool_name'],
            "ticker":row['ticker'],
            "pool_id":row['pool_id'],
            "pledge":row['pledge'],
            "cost":row['cost'],
            "margin":row['margin'],
            "reward_account":row['reward_account'],
            "pool_owners":row['pool_owners'],
            "relays":row['relays'],
            "metadata":row['metadata'],
            "metadata_hash":row['metadata_hash'],
            "fpledge":row['fpledge'],
            "fmargin":row['fmargin'],
            "fcost":row['fcost'],
            "fpledge_epoch":row['fpledge_epoch'],
            "fmargin_epoch":row['fmargin_epoch'],
            "fcost_epoch":row['fcost_epoch']}
    updatevars={
        "pool_id":args['pool_registration']['operator'], 
        "pledge":args['pool_registration']['pledge'],
        "cost":args['pool_registration']['cost'],
        "margin":args['pool_registration']['margin'],
        "reward_account":args['pool_registration']['reward_account'],
        "pool_owners":list((args['pool_registration']['pool_owners'])),
        "relays":list((args['pool_registration']['relays'])),
        "metadata":args['pool_registration']['pool_metadata'],
        "genesis":False,
        "metadata_hash":args['pool_registration']['pool_metadata_hash'],
        "first_epoch":(epoch+2),
    }
    if pool_update['pool_owners']!=db_pool_details['pool_owners']:
        pool_stats_data={
            "owners":list((args['pool_registration']['pool_owners']))
        }
        fb.updateFb(baseNetwork+"/pool_stats/"+str(args['pool_registration']['operator']),pool_stats_data)
    if pool_update['reward_account']!=db_pool_details['reward_account']:
        pool_stats_data={
            "reward_address":args['pool_registration']['reward_account']
        }
        fb.updateFb(baseNetwork+"/pool_stats/"+str(args['pool_registration']['operator']),pool_stats_data)
    
    if pool_update['pledge']!=db_pool_details['pledge']:
        #check if we have already recorded a change.
        if db_pool_details['fpledge'] is None or db_pool_details['fpledge']!=pool_update['pledge']:
            changes.append({'type':'pledge','old':int(db_pool_details['pledge']),'new':int(pool_update['pledge'])}) 
            updatestring=updatestring+", fpledge=%(fpledge)s, fpledge_epoch=%(fpledge_epoch)s"
            updatevars['fpledge']=int(pool_update['pledge'])
            updatevars['fpledge_epoch']=(epoch+1)
            if sendTelegramBotMessages:
                aws.awsbroadcast({"type":"wallet_poolchange","data":  {"pool":row['pool_id'], "change":{"pledge":{"epoch_effective":(epoch+3),"old_value":int(db_pool_details['pledge']),"new_value":int(pool_update['pledge'])}}}})
            else:
                print({"type":"wallet_poolchange","data":  {"pool":row['pool_id'], "change":{"pledge":{"epoch_effective":(epoch+3),"old_value":int(db_pool_details['pledge']),"new_value":int(pool_update['pledge'])}}}})
            if allowChanges:
                fb.updateFb(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"fp":int(pool_update['pledge'])})
            else:
                print(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"fp":int(pool_update['pledge'])})
                
    if format(float(pool_update['margin']),".4f")!=format(float(db_pool_details['margin']),".4f"):
        if db_pool_details['fmargin'] is None or format(float(db_pool_details['fmargin']),".4f")!=format(float(pool_update['margin']),".4f"): 
            changes.append({'type':'margin_cost','old':float(db_pool_details['margin']),'new':float(pool_update['margin'])}) 
            updatestring=updatestring+", fmargin=%(fmargin)s, fmargin_epoch=%(fmargin_epoch)s"
            updatevars['fmargin']=float(pool_update['margin'])
            updatevars['fmargin_epoch']=(epoch+1)
            if sendTelegramBotMessages:
                aws.awsbroadcast({"type":"wallet_poolchange","data":  {"pool":row['pool_id'], "change":{"margin":{"epoch_effective":(epoch+3),"old_value":(db_pool_details['margin']),"new_value":str(pool_update['margin'])}}}})
            else:
                print({"type":"wallet_poolchange","data":  {"pool":row['pool_id'], "change":{"margin":{"epoch_effective":(epoch+3),"old_value":(db_pool_details['margin']),"new_value":str(pool_update['margin'])}}}})
            if allowChanges:
                fb.updateFb(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"fm":(round(pool_update['margin']*100,2))})
            else:
                print(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"fm":(pool_update['margin']*100)})
    if pool_update['cost']!=db_pool_details['cost']:
        if db_pool_details['fcost'] is None or db_pool_details['fcost']!=pool_update['cost']: 
            changes.append({'type':'fixed_cost','old':int(db_pool_details['cost']),'new':int(pool_update['cost'])})
            updatestring=updatestring+", fcost=%(fcost)s, fcost_epoch=%(fcost_epoch)s"
            updatevars['fcost']=int(pool_update['cost'])
            updatevars['fcost_epoch']=(epoch+1)
            if sendTelegramBotMessages:
                aws.awsbroadcast({"type":"wallet_poolchange","data":  {"pool":row['pool_id'], "change":{"cost":{"epoch_effective":(epoch+3),"old_value":int(db_pool_details['cost']),"new_value":int(pool_update['cost'])}}}})
            else:
                print({"type":"wallet_poolchange","data":  {"pool":row['pool_id'], "change":{"cost":{"epoch_effective":(epoch+3),"old_value":int(db_pool_details['cost']),"new_value":int(pool_update['cost'])}}}})
            if allowChanges:
                fb.updateFb(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"ff":int(pool_update['cost'])})
            else:
                print(baseNetwork+"/stake_pools/"+str(row['pool_id']),{"ff":int(pool_update['cost'])})
    if pool_update['metadata']!=db_pool_details['metadata'] or pool_update['metadata_hash']!=db_pool_details['metadata_hash']:
        pg.cur1_execute("update pools set metadata_last_check=0 where pool_id=%s",[args['pool_registration']['operator']])
        print("update metadata")
    if pool_update['relays']!=db_pool_details['relays']:
        pg.cur1_execute("update pools set relays_last_check=0 where pool_id=%s",[args['pool_registration']['operator']])
        print("update relays")
    if len(changes) and not replay:
        fb.poolUpdateLedgerWrite(args['pool_registration']['operator'],db_pool_details['pool_name'],db_pool_details['ticker'],changes,args['context']['timestamp'])
        

    pg.cur1_execute("insert into pool_updates (vrf_keyhash, pledge, cost, margin, reward_account, pool_owners, relays, metadata, metadata_hash, epoch_effective, blocknum, slot, epoch, pool_id,timestamp, certificate_idx) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
    [args['pool_registration']['vrf_keyhash'],args['pool_registration']['pledge'],args['pool_registration']['cost'],args['pool_registration']['margin'],args['pool_registration']['reward_account'],
    list((args['pool_registration']['pool_owners'])),list((args['pool_registration']['relays'])),args['pool_registration']['pool_metadata'],args['pool_registration']['pool_metadata_hash'],(epoch+3),args['context']['block_number'],args['context']['slot'],epoch,args['pool_registration']['operator'],
    args['timestamp'],args['context']['certificate_idx']
    ])
    pg.cur1_execute("""insert into pools (pool_id, pledge, cost, margin, reward_account, pool_owners, relays, metadata, genesis, metadata_hash, first_epoch) values (%(pool_id)s,%(pledge)s,%(cost)s,%(margin)s,%(reward_account)s,%(pool_owners)s,%(relays)s,%(metadata)s,%(genesis)s,%(metadata_hash)s,%(first_epoch)s) ON CONFLICT ON CONSTRAINT tick_id DO UPDATE 
        SET reward_account=%(reward_account)s, pool_owners=%(pool_owners)s, relays=%(relays)s, metadata=%(metadata)s, metadata_hash=%(metadata_hash)s, fretired=false""" + updatestring,updatevars)
    if not replay:
        pg.cur1_execute("update sync_status set tx_fingerprints = array_append(tx_fingerprints,%s)  where key='height'",[args['fingerprint']])
    pg.conn_commit()

def consumePoolRetirement(args,current_epoch,replay=False):
    pg.cur1_execute("select epoch from blocks where block=%s",[args['context']['block_number']])
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row and "epoch" in row:
        epoch=row['epoch']
    
    pg.cur1_execute("insert into pool_updates (epoch_effective, blocknum, slot, epoch, pool_id,timestamp, retiring,certificate_idx) values(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
    [args['pool_retirement']['epoch'], args['context']['block_number'],args['context']['slot'],epoch,args['pool_retirement']['pool'],args['timestamp'],True,args['context']['certificate_idx']
    ])
    pg.cur1_execute("update pools set fretired=true, fretired_epoch=%s where pool_id=%s",[args['pool_retirement']['epoch'],args['pool_retirement']['pool']])
    if not replay:
        pg.cur1_execute("update sync_status set tx_fingerprints = array_append(tx_fingerprints,%s)  where key='height'",[args['fingerprint']])
    pg.conn_commit()



        

