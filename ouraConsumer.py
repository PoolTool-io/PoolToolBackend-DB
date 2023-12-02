
from flask import Flask, request, jsonify
from config import *
import hashlib
import json
import binascii
import logging
from consumerTools import *
from pg_utils import *

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

pg=pg_utils("ouraConsumer")

current_epoch=None
# insure we always start up with a valid epoch
pg.cur1_execute("select slot,hash, epoch from blocks order by block desc limit 1")
row=pg.cur1_fetchone()
pg.conn_commit()
if row and row is not None:
  current_epoch=int(row['epoch'])
else:
  exit("failed to get current epoch")
app = Flask(__name__)

@app.route('/ouraconsume', methods=['POST'])
def ouraconsumeHandler():
  global current_epoch
  args = request.get_json()
  print(args['context']['block_number'],args['variant'])
  
  ###############################################################
  if args['variant']=="Transaction" and current_epoch is not None:
      #print(args['context']['block_number'], args['variant'])
      consumeTransaction(args,current_epoch)
  
  ###############################################################    
  if args['variant']=="TxInput":
      #print(args['context']['block_number'], args['variant'])
      consumeTxInput(args,current_epoch)
  
  ###############################################################
  if args['variant']=="TxOutput":
      #print(args['context']['block_number'], args['variant'])
      consumeTxOutput(args,current_epoch)
  
  ###############################################################
  if args['variant']=="BlockEnd":
    consumeBlockEnd(args,current_epoch)
  
  ###############################################################
  if args['variant']=="Block":
    pg.cur1_execute("select bool from sync_status where key='pause_block'")
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row['bool']:
      
      print(args['context']['block_number'], args['variant'])
      print("processing paused")
      return {"success":False ,"message":"processing paused"},404
    
    pg.cur1_execute("select block from blocks where block=%s",[int(args['context']['block_number'])])
    blockexists=pg.cur1_fetchone()
    pg.conn_commit()
    print("blockexists",blockexists)
    # due to restarts we may have already processed this block.
    if blockexists is None:
      print(args['block']['epoch'], args['context']['block_number'], args['variant'])
          
      if current_epoch==None:
        current_epoch = args['block']['epoch']
      if current_epoch!=args['block']['epoch']:
        epochProcessing(args,current_epoch)
      current_epoch = args['block']['epoch']
        
      consumeBlock(args,current_epoch)
    else:
      print(args['block']['epoch'], args['context']['block_number'], args['variant'], "SKIPPED")


  if args['variant']=="StakeDeregistration":
    # need to be able to skip over older blocks.
    pg.cur1_execute("select block, tx_fingerprints from sync_status where key='height'")
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row['block']==args['context']['block_number'] and args['fingerprint'] not in row['tx_fingerprints']:
      print(args['context']['block_number'], args['variant'])
      consumeStakeDeRegistration(args,current_epoch,False)
      
    else:
      print("XXX", args['context']['block_number'], args['variant'],"SKIPPED")

  ###############################################################  
  if args['variant']=="StakeDelegation":
    # need to be able to skip over older blocks.
    pg.cur1_execute("select block, tx_fingerprints from sync_status where key='height'")
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row['block']==args['context']['block_number'] and args['fingerprint'] not in row['tx_fingerprints']:
      print(args['context']['block_number'], args['variant'])
      consumeStakeDelegation(args,current_epoch,False)
    else:
      print("XXX", args['context']['block_number'], args['variant'],"SKIPPED")

  ###############################################################  
  if args['variant']=="PoolRegistration":
    pg.cur1_execute("select block, tx_fingerprints from sync_status where key='height'")
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row['block']==args['context']['block_number'] and args['fingerprint'] not in row['tx_fingerprints']:
      # get the epoch from the block since its not in the context
      print(args['context']['block_number'], args['variant'])
      consumePoolRegistration(args,current_epoch,False)
    
    else:
      print("XXX", args['context']['block_number'], args['variant'],"SKIPPED")

  ###############################################################  
  if args['variant']=="PoolRetirement":
    pg.cur1_execute("select block, tx_fingerprints from sync_status where key='height'")
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row['block']==args['context']['block_number'] and args['fingerprint'] not in row['tx_fingerprints']:
      print(args['context']['block_number'], args['variant'])
      consumePoolRetirement(args,current_epoch,False)
      
    else:
      print("XXX", args['context']['block_number'], args['variant'],"SKIPPED")

  ###############################################################  
  if args['variant']=="MoveInstantaneousRewardsCert":
    print(args)
    with open("cbormoves/"+args['fingerprint']+'.json', 'w') as out_file:
      json.dump(args, out_file, sort_keys = True, indent = 4,
               ensure_ascii = False)
  

    
  
  return {"success":True ,"message":""}

if __name__ == '__main__':
  #testing change
  app.run(debug = True, host = '0.0.0.0', port="5660",use_reloader=False)

