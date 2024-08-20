
from flask import Flask, request, jsonify
from config import *
import hashlib
import json
import binascii
import logging
from justTheTipTools import *
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
  
  ###############################################################
  if args['variant']=="Transaction" and current_epoch is not None:
      consumeTransaction(args,current_epoch)
      
  
  return {"success":True ,"message":""}

if __name__ == '__main__':
  app.run(debug = True, host = '0.0.0.0', port="5664",use_reloader=False)

