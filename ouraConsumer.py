
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
busy=False
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
  global current_epoch, busy
  args = request.get_json()
  
  ###############################################################
  if args['variant']=="Transaction" and current_epoch is not None:
    if busy:
      print("busy")
      return {"success":False ,"message":"busy"},404
    busy=True
    try:
      consumeTransaction(args,current_epoch)
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False
  
  ###############################################################    
  if args['variant']=="TxInput":
    if busy:
      print("busy")
      return {"success":False ,"message":"busy"},404
    busy=True
    try:
      consumeTxInput(args,current_epoch)
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False
  
  ###############################################################
  if args['variant']=="TxOutput":
    if busy:
      print("busy")
      return {"success":False ,"message":"busy"},404
    busy=True
    try:
      consumeTxOutput(args,current_epoch)
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False
  
  ###############################################################
  if args['variant']=="BlockEnd":
    if busy:
        print("busy")
        return {"success":False ,"message":"busy"},404
    busy=True
    try:
      consumeBlockEnd(args,current_epoch)
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False
  
  ###############################################################
  if args['variant']=="Block":
    print("new block")
    pg.cur1_execute("select bool from sync_status where key='pause_block'")
    row=pg.cur1_fetchone()
    pg.conn_commit()
    if row['bool']:
      
      print(args['context']['block_number'], args['variant'])
      print("processing paused")
      busy=False # turn off the busy flag just in case we need to cycle block pausing to flip the flag
      return {"success":False ,"message":"processing paused"},404
    if busy:
        print("busy")
        return {"success":False ,"message":"busy"},404
    busy=True
    try:
      pg.cur1_execute("select block from blocks where block=%s",[int(args['context']['block_number'])])
      blockexists=pg.cur1_fetchone()
      pg.conn_commit()
      
      # due to restarts we may have already processed this block.
      if blockexists is None:
        print("block does not exist")
        print(args['block']['epoch'], args['context']['block_number'], args['variant'])
            
        if current_epoch==None:
          current_epoch = args['block']['epoch']
        if current_epoch!=args['block']['epoch']:
          epochProcessing(args,current_epoch)
        current_epoch = args['block']['epoch']
          
        consumeBlock(args,current_epoch)
      else:
        print(args['block']['epoch'], args['context']['block_number'], args['variant'], "SKIPPED")
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False


  if args['variant']=="StakeDeregistration":
    if busy:
      print("busy")
      return {"success":False ,"message":"busy"},404
    busy=True
    # need to be able to skip over older blocks.
    try:
      pg.cur1_execute("select block, tx_fingerprints from sync_status where key='height'")
      row=pg.cur1_fetchone()
      pg.conn_commit()
      if row['block']==args['context']['block_number'] and args['fingerprint'] not in row['tx_fingerprints']:
        print(args['context']['block_number'], args['variant'])
        consumeStakeDeRegistration(args,current_epoch,False)
        
      else:
        print("XXX", args['context']['block_number'], args['variant'],"SKIPPED")
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False

  ###############################################################  
  if args['variant']=="StakeDelegation":
    # need to be able to skip over older blocks.
    if busy:
      print("busy")
      return {"success":False ,"message":"busy"},404
    busy=True
    try:
      pg.cur1_execute("select block, tx_fingerprints from sync_status where key='height'")
      row=pg.cur1_fetchone()
      pg.conn_commit()
      if row['block']==args['context']['block_number'] and args['fingerprint'] not in row['tx_fingerprints']:
        print(args['context']['block_number'], args['variant'])
        consumeStakeDelegation(args,current_epoch,False)
      else:
        print("XXX", args['context']['block_number'], args['variant'],"SKIPPED")
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False

  ###############################################################  
  if args['variant']=="PoolRegistration":
    if busy:
      print("busy")
      return {"success":False ,"message":"busy"},404
    busy=True
    try:
      pg.cur1_execute("select block, tx_fingerprints from sync_status where key='height'")
      row=pg.cur1_fetchone()
      pg.conn_commit()
      if row['block']==args['context']['block_number'] and args['fingerprint'] not in row['tx_fingerprints']:
        # get the epoch from the block since its not in the context
        print(args['context']['block_number'], args['variant'])
        consumePoolRegistration(args,current_epoch,False)
      
      else:
        print("XXX", args['context']['block_number'], args['variant'],"SKIPPED")
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False

  ###############################################################  
  if args['variant']=="PoolRetirement":
    if busy:
      print("busy")
      return {"success":False ,"message":"busy"},404
    busy=True
    try:
      pg.cur1_execute("select block, tx_fingerprints from sync_status where key='height'")
      row=pg.cur1_fetchone()
      pg.conn_commit()
      if row['block']==args['context']['block_number'] and args['fingerprint'] not in row['tx_fingerprints']:
        print(args['context']['block_number'], args['variant'])
        consumePoolRetirement(args,current_epoch,False)
        
      else:
        print("XXX", args['context']['block_number'], args['variant'],"SKIPPED")
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False

  ###############################################################  
  if args['variant']=="MoveInstantaneousRewardsCert":
    if busy:
      print("busy")
      return {"success":False ,"message":"busy"},404
    busy=True
    try:
      print(args)
      with open("cbormoves/"+args['fingerprint']+'.json', 'w') as out_file:
        json.dump(args, out_file, sort_keys = True, indent = 4,
                ensure_ascii = False)
    except Exception as e:
      print("error",e)
      return {"success":False ,"message":"error"},404
    busy=False
  

    
  
  return {"success":True ,"message":""}

if __name__ == '__main__':
  #testing change
  app.run(debug = True, host = '0.0.0.0', port="5660",use_reloader=False)

