#syncportfolios.py
import subprocess, platform
from os import environ,path
import threading
import json
import time
import boto3
import requests
import math
from config import *
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

environ["CNODE_HOME"] = "/opt/cardano/cnode"
environ["CARDANO_NODE_SOCKET_PATH"] = environ["CNODE_HOME"] + "/sockets/node0.socket"
cardanocli = "cardano-cli"
environ['AWS_DEFAULT_REGION'] = "us-west-2"
environ['AWS_PROFILE'] = "s3writeprofile"
session = boto3.Session(profile_name='s3writeprofile')
sqs = boto3.client('sqs')

conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
cur2 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
cur3 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

s3 = boto3.resource("s3").Bucket("data.pooltool.io")

json.load_s3 = lambda f: json.load(s3.Object(key=f).get()["Body"])
json.dump_s3 = lambda obj, f: s3.Object(key=f).put(Body=json.dumps(obj),ACL='public-read')


# Fetch the service account key JSON file contents
cred = credentials.Certificate('firebase-account.json')

# Initialize the app with a service account, granting admin privileges
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://pegasus-pool.firebaseio.com'
})

currentportfolios = db.reference(baseNetwork+"/portfolios").get() #https://pegasus-pool.firebaseio.com/Mainnet/portfolios
       
r = requests.get('https://adafolio.com/json/ids')
adafolios = r.json()
for folio in adafolios:
    print(folio)
    if folio['id'] not in currentportfolios:
        p = requests.get('https://adafolio.com/portfolio/json/'+folio['id'])
        foliopools=p.json()
        print(foliopools)
        pools = []
        for p in foliopools['pools']:
            pools.append(p['pool_id'])
        wfoliopools={"name":foliopools['name'],"description":foliopools['description'],"id":foliopools['id']}
        wfoliopools['pools']=pools
        wfoliopools['display']=False
        wfoliopools['id']=folio['id']
        wfoliopools['disabled']=False
        wfoliopools['created_at']=int(time.time())
        wfoliopools['adafolio']=True
        db.reference(baseNetwork+"/portfolios").update({folio['id']:wfoliopools})
    else:
        wfoliopools=currentportfolios[folio['id']]
        if 'id' not in wfoliopools:
            db.reference(baseNetwork+"/portfolios/"+folio['id']).update({"id":folio['id']}) 
        if wfoliopools['disabled']:
            currentportfolios[folio['id']]={}
            currentportfolios[folio['id']]['id']=folio['id']
            currentportfolios[folio['id']]['disabled']=True
            currentportfolios[folio['id']]['display']=False
            db.reference(baseNetwork+"/portfolios/"+folio['id']).update({"disabled":True,"display":False}) 
        else:
            p = requests.get('https://adafolio.com/portfolio/json/'+folio['id'])
            foliopools=p.json()
            print(foliopools)
            pools = []
            for p in foliopools['pools']:
                pools.append(p['pool_id'])
            currentportfolios[folio['id']]['name']=foliopools['name']
            wfoliopools["name"]=foliopools['name']
            currentportfolios[folio['id']]['description']=foliopools['description']
            wfoliopools["description"]=foliopools['description']
            currentportfolios[folio['id']]['pools']=foliopools['pools']
            wfoliopools['pools']=pools
            wfoliopools['id']=folio['id']
            currentportfolios[folio['id']]['disabled']=wfoliopools['disabled']
            currentportfolios[folio['id']]['display']=wfoliopools['display']
            db.reference(baseNetwork+"/portfolios").update({folio['id']:wfoliopools})

json.dump_s3(currentportfolios,baseNetwork+"/portfolios.json")
        