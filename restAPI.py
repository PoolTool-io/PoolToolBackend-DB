
from config import *
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import subprocess, platform
from fb_utils import *
from pt_utils import *
from pg_utils import *
import uuid
import re
import bcrypt
import random
import string
fb=fb_utils()
uuid4hex = re.compile('^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z', re.I)
pg=pg_utils('restApi')



app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/zapierdeleteapikey',methods=['POST'])
@cross_origin()
def zapierDeleteApiKeyHandler():
    args=request.get_json()
    if 'user_id' not in args or not bool(uuid4hex.match(args['user_id'])):
        return {"success":False,"message":"You must supply a valid user_id"}
    if 'api_key' not in args or not bool(uuid4hex.match(args['api_key'])):
        return {"success":False,"message":"You must supply an api_key"}
    # delete keys in postgres
    pg.cur1_execute("delete from zapier_auth where user_id=%s and api_key=%s",[args['user_id'],args['api_key']])
    # delete all triggers
    pg.cur1_execute("delete from zapier_triggers where user_id=%s",[args['user_id']])
    pg.conn_commit()
    # delete keys in firebase
    fb.deleteFb(baseNetwork+"/users/privMeta/"+args['user_id']+"/myZapierApiKey")
    return {"success":True}

@app.route('/zapiercreateapikey',methods=['POST'])
@cross_origin()
def zapierCreateApiKeyHandler():
    args=request.get_json()
    if 'user_id' not in args  or not bool(uuid4hex.match(args['user_id'])):
        return {"success":False,"message":"You must supply a user_id"}
    print(args)
    # check to make sure a secret does not already exist
    zapierkey= fb.getKey(baseNetwork+"/users/privMeta/"+args['user_id']+"/myZapierApiKey")
    if zapierkey is not None:
        return {"success":False,"message":"Key already exists"}
    # create a uuid for the key, and create the secret
    zapierkey=uuid.uuid4()
    letters = string.ascii_letters
    apisecret=''.join(random.choice(letters) for i in range(20))
    packet = {"myZapierApiKey":str(zapierkey)}
    # store the key firebase
    fb.updateFb(baseNetwork+"/users/privMeta/"+args['user_id'],packet)
    # store the key and secret in postgres
    pg.cur1_execute("insert into zapier_auth (user_id,api_key,api_secret) values(%s,%s,%s)",[args['user_id'],str(zapierkey),apisecret])
    pg.conn_commit()
    # return the secret
    return {"success":True,"apisecret":apisecret}

@app.route('/zapiersubscribehook',methods=['POST'])
@cross_origin()
def zapierSubscribeHandler():
    args=request.get_json()
    print(args)
    if 'trigger_type' not in args or args['trigger_type'] is None or args['trigger_type']=='':
        print("Need a Trigger type")
        return "Need a Trigger type", 400
    if 'api_key' not in args  or not bool(uuid4hex.match(args['api_key'])):
        print("You must supply an api_key")
        return "You must supply an api_key", 400
    if 'api_secret' not in args:
        print("You must supply an api_secret")
        return "You must supply an api_secret", 400
    if 'hookUrl' not in args:
        print("You must supply a hookUrl")
        return "You must supply a hookUrl", 400
    if args['trigger_type']=='new_block':
        if 'pool_id' not in args:
            print("You must supply a pool_id")
            return "You must supply a pool_id", 400
        data_key=args['pool_id']
    else:
        data_key=''
    
    pg.cur1_execute("select user_id from zapier_auth where api_key=%s and api_secret=%s",[args['api_key'],args['api_secret']])
    row=pg.cur1_fetchone()
    if row is None:
        print("Cannot find that api key and secret")
        return "Cannot find that api key and secret", 400
    if args['trigger_type']=='new_post':
        # for new post types make sure we have enabled posting from pooltool
        packet = {"myZapierEnablePost":True}
        # store the key firebase
        fb.updateFb(baseNetwork+"/users/privMeta/"+row['user_id'],packet)

    pg.cur1_execute("insert into zapier_triggers (trigger_type,data_key,zapier_url,user_id) values(%s,%s,%s,%s)",[args['trigger_type'],data_key,args['hookUrl'],row['user_id']])
    pg.conn_commit()
    return {"success":True}

@app.route('/zapierunsubscribehook',methods=['POST'])
@cross_origin()
def zapierUnSubscribeHandler():
    args=request.get_json()
    if 'trigger_type' not in args or args['trigger_type'] is None or args['trigger_type']=='':
        print("Need a Trigger type")
        return "Need a Trigger type", 400
    if 'api_key' not in args  or not bool(uuid4hex.match(args['api_key'])):
        print("You must supply an api_key")
        return "You must supply an api_key", 400
    if 'api_secret' not in args:
        print("You must supply an api_secret")
        return "You must supply an api_secret", 400
    if 'hookUrl' not in args:
        print("You must supply a hookUrl")
        return "You must supply a hookUrl", 400
    if args['trigger_type']=='new_block':
        if 'pool_id' not in args:
            print("You must supply a pool_id")
            return "You must supply a pool_id", 400
        data_key=args['pool_id']
    else:
        data_key=''
    pg.cur1_execute("select user_id from zapier_auth where api_key=%s and api_secret=%s",[args['api_key'],args['api_secret']])
    row=pg.cur1_fetchone()
    if row is None:
        print("Cannot find that api key and secret")
        return "Cannot find that api key and secret", 400
    
    pg.cur1_execute("delete from zapier_triggers where trigger_type=%s and zapier_url=%s and user_id=%s and data_key=%s",[args['trigger_type'],args['hookUrl'],row['user_id'],data_key])
    pg.conn_commit()


    if args['trigger_type']=='new_post':
        # for new post types make sure we have disabled posting from pooltool if there are no triggers setup
        pg.cur1_execute("select * from zapier_triggers where trigger_type=%s and user_id=%s",[args['trigger_type'],row['user_id']])
        row=pg.cur1_fetchone()
        if not row:
            # no post rows left
            packet = {"myZapierEnablePost":False}
            
            fb.updateFb(baseNetwork+"/users/privMeta/"+row['user_id'],packet)
    print(args)
    return {"success":True}


@app.route('/zapierauth',methods=['POST'])
@cross_origin()
def zapierAuthHandler():
    args=request.get_json()
    if 'api_key' not in args or not bool(uuid4hex.match(args['api_key'])):
        return "Record not found", 400
    if 'api_secret' not in args:
        return "Record not found", 400
    pg.cur1_execute("select user_id from zapier_auth where api_key=%s and api_secret=%s",[args['api_key'],args['api_secret']])
    row=pg.cur1_fetchone()
    if row is None:
        return "Record not found", 400
    print(args)
    return {"success":True,"user_id":row['user_id']}

@app.route('/login',methods=['POST'])
@cross_origin()
def loginHandler():
    args=request.get_json()
    stake_key=convertBech32(args['address'])[2:]
    print(stake_key)
    user_id = fb.getKey(baseNetwork+"/users/addr2user/"+stake_key)
    print(user_id)
    pwhash= fb.getKey(baseNetwork+"/users/auth/"+user_id+"/passwordHash")
    print(pwhash)
    result = bcrypt.checkpw(args['password'].encode('utf-8'),pwhash.encode('utf-8'))
    print(result)
    return {"success":True}

@app.route('/queryaddress',methods=['POST'])
@cross_origin()
def queryaddressHandler():
    args=request.get_json()
    if 'stake_key' in args and len(args['stake_key'])==56:
        pg.cur1_execute("select stake_key, epoch, amount, delegated_to_pool, delegated_to_ticker from stake_history where stake_key=%s order by epoch desc limit 1",[args['stake_key']])
        row=pg.cur1_fetchone()
        pg.conn_commit()
        if row:
            return {"success":True,"epoch":int(row['epoch']) if row['epoch'] is not None else 0,"amount":int(row['amount']) if row['amount'] is not None else 0,"delegatedToTicker":row['delegated_to_ticker'].strip() if row['delegated_to_ticker'] is not None else '', "delegatedTo":row['delegated_to_pool'].strip() if row['delegated_to_pool'] is not None else None}
        else:
            return {"success":False}
    else:
        return {"success":False}

@app.route('/pivotrewards',methods=['POST'])
@cross_origin()
def pivotrewardsHandler():
    args=request.get_json()
    user_agent=request.headers.get('User-Agent')
    
    print(user_agent)
    print(args)
    if user_agent.find('PetalBot')==-1:
        if 'stake_key' in args and isinstance(args['stake_key'], list):
            print("we have an array")
            for address in args['stake_key']:
                print(address)
                if len(address)==56:
                    print("pivoting")
                    subprocess.Popen(['nohup','python3','-u', 'pivotRewards.py',address], shell=False,stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
            
        elif 'stake_key' in args and len(args['stake_key'])==56:
            subprocess.Popen(['nohup','python3','-u', 'pivotRewards.py',args['stake_key']], shell=False,stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        
    else:
        print("Deny PedalBot")    
    return {"success":True}

  

if __name__ == '__main__':
    #testing change
  app.run(debug = True, host = '0.0.0.0',port=8313,use_reloader=False)
