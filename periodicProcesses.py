#import websockets
import time
import json
import socket
import asyncio
import re

import ipaddress
from urllib.parse import urlparse

from config import *
from pt_utils import * 
from metadata_tools import *
from fb_utils import fb_utils
from aws_utils import aws_utils
from pg_utils import *
from pt_utils import *



MAX_IP_API_REQUESTS_PER_DAY=9
POOL_RANKING_PERIOD=60*58*1 # 1 hours (new data only updated every 5 hours which gives a max update period of around 6 hours)
METADATA_PERIOD=60*62 # 1 hour
POOL_RELAYS=60*63*1 # about 1 hours
WRITE_TICKERS=60*64 # every hour
BATTLE_TRENDS=60*65 # every hour
PLEDGE_CHECK=60*66*24 # every day

fb=fb_utils()
aws=aws_utils()
pg=pg_utils("PeriodicProcesses")


api_requests_today=0
skipped_api_requests_today=0
today=time.strftime("%Y-%m-%d")




def send_pledge_violation_notification(pool_id,poolTicker,poolName,pledge,actualpledge, gsm_token):
    
    if poolTicker =='':
        name = "Pool id " + str(pool_id)
    else:
        name="[" + str(poolTicker).strip() + "] " + str(poolName).strip()
    body = name + " does not meet pledge.  If not resolved by end of epoch then no rewards." 
    title = "\u2620 Pledge Alert for " + name + "! "
    
    try:
        fb.push_notification(title,body,gsm_token)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    


async def check_pledge_violations():
    print("check_pledge_violations")
    pg.cur1_execute("select pool_id, pledge, ticker, pool_name, poolpledgevalue, fpledge, fpledge_epoch, fretired, fretired_epoch, retired from pools where (poolpledgevalue<pledge) and retired=false and genesis=false;")  
    rows=pg.cur1_fetchall()
    pools_not_meeting_pledge={}

    for row in rows:
        pools_not_meeting_pledge[row['pool_id']]=row
    alerts=fb.getKey(baseNetwork+"/legacy_alerts")
    for pool in alerts:
        for alert_type in alerts[pool]:
            if alert_type=="pledge":
                if pool in pools_not_meeting_pledge:
                    print(pool)
                    for gsm_token in alerts[pool][alert_type]:
                        if alerts[pool][alert_type][gsm_token]['enabled']:
                            print(pool)
                            print(gsm_token)
                            #my_gsm_token="cH2ge3e8qkTFuktIqlH_g_:APA91bGFk1JUV3c1Uhacz7iAXi0RP4AkssCIwukPKtqgpV6dzkJC0MGsBl1f4wpuAV_-loFM_jXdLqQ2OdmRZaP1SHupVir232ehFhCjQnIfNnLyUEmTZ_KUA3VIVarBTiepfxyI_Z9s"
                            send_pledge_violation_notification(pools_not_meeting_pledge[pool]['pool_id'],pools_not_meeting_pledge[pool]['ticker'],pools_not_meeting_pledge[pool]['pool_name'],pools_not_meeting_pledge[pool]['pledge'],pools_not_meeting_pledge[pool]['poolpledgevalue'], gsm_token)
                        
    return True

async def update_pool_relays():
    global today,api_requests_today,skipped_api_requests_today
    if today!=time.strftime("%Y-%m-%d"):
        api_requests_today=0
        skipped_api_requests_today=0
        today=time.strftime("%Y-%m-%d")
    counter = 0
    remoteProtocolVersion={}
    #load in datacenter lookup info
    try:
        pg.cur1_execute("select * from ip_datacenter_lookup")
        row=pg.cur1_fetchone()
        datacenter_data={}
        while row:
            datacenter_data[row['ip_address']]=row['json_data']
            row=pg.cur1_fetchone()

        pg.cur1_execute("select pool_id, online_relays, offline_relays, relays from pools where retired=false and genesis=false and ((offline_relays>0 and relays_failure_count<100 and relays_last_check+(3600*relays_failure_count)<extract(epoch from now())::int) or relays_last_check=0 or(offline_relays=0 and (relays_last_check+(3600*2))<extract(epoch from now())::int))")
        row=pg.cur1_fetchone()
        while row:
            counter = counter + 1
            #print("#" + str(counter) + " - Processing relays for non-retired pool: " + row['pool_id'])
            onlineCount = 0
            offlineCount = 0
            visitedIps = []
            relayDetails={}
            if row['relays'] is not None:
                for relay in row['relays']:
                    # split relay into ip and port
                    relay=relay.strip()
                    print("testing relay",relay)
                    
                    if relay not in relayDetails:
                        relayDetails[relay]={}
                    iplist = parse_ip_port(relay)
                    for item in iplist:
                        protocolVersion=0
                        if item in visitedIps:
                            continue
                        if item['ip'] not in relayDetails[relay]:
                            relayDetails[relay][item['ip']]={}
                        
                        if item['type']==4:
                            res = isOnlineIpRelayCardanoCli(item['ip'], item['port'])
                            
                            if res['status']:
                                onlineCount = onlineCount + 1
                                protocolVersion=res['remoteProtocolVersion']
                                print(res)
                                #exit()
                            else:
                                offlineCount = offlineCount + 1
                            visitedIps.append(item)
                        if item['type']==6:
                            res = isOnlineIpRelayCardanoCli(item['ip'], item['port'])
                            
                            if res['status']:
                                onlineCount = onlineCount + 1
                                protocolVersion=res['remoteProtocolVersion']
                                print(res)
                                #exit()
                            else:
                                offlineCount = offlineCount + 1
                            visitedIps.append(item)
                        if item['port'] not in relayDetails[relay][item['ip']]:
                            relayDetails[relay][item['ip']][item['port']]={"type":item['type'],"online":res['status'],"protocolVersion":protocolVersion}
                        if item['ip'] not in datacenter_data:
                            if api_requests_today<MAX_IP_API_REQUESTS_PER_DAY:
                                api_requests_today=api_requests_today+1
                                print("api requests today: "+str(api_requests_today))
                                result = getIpHostProvider([item['ip']])
                                print(result)
                                if "failure" not in result:
                                    if  item['ip'] in result and 'datacenter' not in result[item['ip']]:
                                        print(result[item['ip']])
                                    if item['ip'] in result and 'datacenter' in result[item['ip']]:
                                        #this means its in a datacenter
                                        datacenter_data[item['ip']]={"is_datacenter":result[item['ip']]['is_datacenter'],"datacenter":result[item['ip']]['datacenter']}
                                        datacenter_data_pkg=result[item['ip']]
                                    else:
                                        datacenter_data[item['ip']]={"is_datacenter":False,"datacenter":''}
                                        datacenter_data_pkg=datacenter_data[item['ip']]
                                    pg.cur2_execute("insert into ip_datacenter_lookup (ip_address,json_data) values(%s,%s) ON CONFLICT DO NOTHING",[str(item['ip']),Json(datacenter_data_pkg)])
                                    # conn.commit()
                                    pg.conn_commit()
                            else:
                                skipped_api_requests_today=skipped_api_requests_today+1
                                print("max api requests today, skipping until tomorrow.  total skipped: " + str(skipped_api_requests_today))
                        if item['ip'] in datacenter_data:
                            relayDetails[relay][item['ip']][item['port']]['is_datacenter']=datacenter_data[item['ip']]['is_datacenter']
                            relayDetails[relay][item['ip']][item['port']]['datacenter']=datacenter_data[item['ip']]['datacenter']
                        else:
                            #we will try to get it the next go around
                            relayDetails[relay][item['ip']][item['port']]['is_datacenter']=False
                            relayDetails[relay][item['ip']][item['port']]['datacenter']=''
        
                        
            
            if (row['online_relays']!=onlineCount or row['offline_relays']!=offlineCount):
                poolRelayUpdate = {
                    "o": onlineCount,
                    "oo": offlineCount
                }
                fb.updateFb(baseNetwork+"/stake_pools/" + row['pool_id'],poolRelayUpdate)
                

            if offlineCount>0:
                pg.cur2_execute("update pools set relay_details=%s,online_relays=%s,offline_relays=%s,relays_last_check=extract(epoch from now())::int, relays_failure_count=pools.relays_failure_count+1 where pool_id=%s",[Json(relayDetails),onlineCount,offlineCount,row['pool_id']])
            else:
                pg.cur2_execute("update pools set relay_details=%s,online_relays=%s,offline_relays=%s,relays_last_check=extract(epoch from now())::int,relays_failure_count=0 where pool_id=%s",[Json(relayDetails),onlineCount,offlineCount,row['pool_id']])
            pg.conn_commit()
            # conn.commit()
            row=pg.cur1_fetchone()
        
        totalOnlineCount = 0
        totalOfflineCount = 0
        totalIpv4Count=0
        totalIpv6Count=0
        remoteProtocolVersion={}
        dataCenters={}
        iptypes={}
        pg.cur1_execute("select pool_id, online_relays, offline_relays,relay_details, live_stake from pools where retired=false and genesis=false")
        row=pg.cur1_fetchone()
        print("summing and saving pool relays data")
        while row:
            if row['online_relays'] is not None:
                totalOnlineCount = totalOnlineCount + row['online_relays']
            if row['offline_relays'] is not None:
                totalOfflineCount = totalOfflineCount + row['offline_relays']
            total_relays=int(row['online_relays']) if row['online_relays'] is not None else 0+int(row['offline_relays']) if row['offline_relays'] is not None else 0
            if row['relay_details'] is not None:
                
                for relay_name in row['relay_details']:
                    for relay_ip in  row['relay_details'][relay_name]:
                        for relay_port in row['relay_details'][relay_name][relay_ip]:
                            if 'online' in row['relay_details'][relay_name][relay_ip][relay_port] and row['relay_details'][relay_name][relay_ip][relay_port]['online'] and 'protocolVersion' in row['relay_details'][relay_name][relay_ip][relay_port]:
                                if row['relay_details'][relay_name][relay_ip][relay_port]['protocolVersion'] not in remoteProtocolVersion:
                                    remoteProtocolVersion[row['relay_details'][relay_name][relay_ip][relay_port]['protocolVersion']]={}
                                    remoteProtocolVersion[row['relay_details'][relay_name][relay_ip][relay_port]['protocolVersion']]['qty']=1
                                    remoteProtocolVersion[row['relay_details'][relay_name][relay_ip][relay_port]['protocolVersion']]['stake']=((int(row['live_stake']) if row['live_stake'] is not None else 0)/total_relays) if total_relays>0 else int(row['live_stake']) if row['live_stake'] is not None else 0
                                else:
                                    remoteProtocolVersion[row['relay_details'][relay_name][relay_ip][relay_port]['protocolVersion']]['qty']+=1
                                    remoteProtocolVersion[row['relay_details'][relay_name][relay_ip][relay_port]['protocolVersion']]['stake']+=((int(row['live_stake']) if row['live_stake'] is not None else 0)/total_relays) if total_relays>0 else int(row['live_stake']) if row['live_stake'] is not None else 0
                            
                            if 'is_datacenter' in row['relay_details'][relay_name][relay_ip][relay_port]:
                                if row['relay_details'][relay_name][relay_ip][relay_port]['is_datacenter'] and 'datacenter' in row['relay_details'][relay_name][relay_ip][relay_port]:
                                    # print(row['pool_id'])
                                    # print(relay_ip)
                                    # print(row['relay_details'][relay_name][relay_ip][relay_port]['datacenter'])
                                    #for some records will will have a datacenter detail inside datacenter apparently.
                                    dc=None
                                    if isinstance(row['relay_details'][relay_name][relay_ip][relay_port]['datacenter'],str):
                                        dc=row['relay_details'][relay_name][relay_ip][relay_port]['datacenter']
                                    elif 'datacenter' in row['relay_details'][relay_name][relay_ip][relay_port]['datacenter']:
                                        dc=row['relay_details'][relay_name][relay_ip][relay_port]['datacenter']['datacenter']
                                    else:
                                        print("don't know how to deal with this datacenter record")
                                        row['relay_details'][relay_name][relay_ip][relay_port]['datacenter']
                                    if dc is not None:
                                        if dc not in dataCenters:
                                            dataCenters[dc]={}
                                            dataCenters[dc]['qty']=1
                                            dataCenters[dc]['stake']=((int(row['live_stake']) if row['live_stake'] is not None else 0)/total_relays) if total_relays>0 else int(row['live_stake']) if row['live_stake'] is not None else 0
                                        else:
                                            dataCenters[dc]['qty']+=1
                                            dataCenters[dc]['stake']+=((int(row['live_stake']) if row['live_stake'] is not None else 0)/total_relays) if total_relays>0 else int(row['live_stake']) if row['live_stake'] is not None else 0
                                else:
                                    if not row['relay_details'][relay_name][relay_ip][relay_port]['is_datacenter']:
                                        if 'Not in Datacenter' not in dataCenters:
                                            dataCenters['Not in Datacenter']={}
                                            dataCenters['Not in Datacenter']['qty']=1
                                            dataCenters['Not in Datacenter']['stake']=((int(row['live_stake']) if row['live_stake'] is not None else 0)/total_relays) if total_relays>0 else int(row['live_stake']) if row['live_stake'] is not None else 0
                                        else:
                                            dataCenters['Not in Datacenter']['qty']+=1
                                            dataCenters['Not in Datacenter']['stake']+=((int(row['live_stake']) if row['live_stake'] is not None else 0)/total_relays) if total_relays>0 else int(row['live_stake']) if row['live_stake'] is not None else 0

                            if 'type' in row['relay_details'][relay_name][relay_ip][relay_port]:
                                if row['relay_details'][relay_name][relay_ip][relay_port]['type'] not in iptypes:
                                    iptypes[row['relay_details'][relay_name][relay_ip][relay_port]['type']]=1
                                else:
                                    iptypes[row['relay_details'][relay_name][relay_ip][relay_port]['type']]=iptypes[row['relay_details'][relay_name][relay_ip][relay_port]['type']]+1
            row=pg.cur1_fetchone()
        sortedremoteProtocolVersion = dict(sorted(remoteProtocolVersion.items(), key=lambda x: x[1]['stake'],reverse=True))
        print("saving sorted data")
        ecosystemUpdate = {
            "onlineRelays": totalOnlineCount,
            "offlineRelays": totalOfflineCount,
            "protocolsRelays": json.dumps(sortedremoteProtocolVersion, separators=(',', ':')),
            "iptypesRelays": json.dumps(iptypes, separators=(',', ':')),
            "datacentersRelays":json.dumps(dataCenters, separators=(',', ':'))
        }
        fb.updateFb(baseNetwork+"/ecosystem",ecosystemUpdate)

        pg.cur1_execute("select slot,hash, epoch from blocks order by block desc limit 1")
        row=pg.cur1_fetchone()
        pg.conn_commit()
        if row and row is not None:
            epoch=row['epoch']
            writetime=str(int(float(time.time()))) #stats_history_write_point
            pg.cur1_execute("""insert into stats_history (timestamp, epoch, online_relays, offline_relays,protocol_relays, ip_types_relays, datacenter_relays)
            values(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            [writetime,epoch,totalOnlineCount,totalOfflineCount,Json(remoteProtocolVersion),Json(iptypes),Json(dataCenters)])
            pg.conn_commit()
        
        

        return True
    except Exception as e:
        print("failure in pool relays")
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return True
  

def get_ip_type(ip):
    try:
        if isinstance(ip, ipaddress.IPv4Address):
            return 4
        elif isinstance(ip, ipaddress.IPv6Address):
            return 6
    except ValueError:
        return 0

def parse_ip_port(netloc):
    iplist=[]
    try:
        ip = ipaddress.ip_address(netloc)
        port = None
        return [{"ip":str(ip),"port":port,"type":get_ip_type(ip)}]
    except ValueError:
        parsed = urlparse('//{}'.format(netloc))
        try:
            port = parsed.port
            ip = ipaddress.ip_address(parsed.hostname)
            return [{"ip":str(ip),"port":port,"type":get_ip_type(ip)}]
        except Exception as err:
            try:
                data = socket.gethostbyname_ex(parsed.hostname)
                ips = repr(data[2])
                for ip in data[2]:
                    try:
                        ip = ipaddress.ip_address(ip)
                        port = parsed.port
                        iplist.append({"ip":str(ip),"port":port,"type":get_ip_type(ip)})
                    except Exception as err:
                        print(Exception, err)
            except Exception as err:
                print(Exception, err)
    return iplist

def isOnlineIpRelayCardanoCli(ip,port):
    te=""
    te = runcli2(f"/home/ubuntu/.cabal/bin/cardano-cli ping -h {ip} -p {port} -Q -j -q", timeout=10, timeout_return="")
    print(te)
    print(f"//home/ubuntu/.cabal/bin/cardano-cli ping -h {ip} -p {port} -Q -j -q")
   
    

    if te!="" and te!=None:
        versions = re.findall(r'NodeToNodeVersionV(\d+)', te)
        #print(versions)
        if not versions:
            return {"status":False,"errorMessage":te}
        else:
            max_version = max(int(version) for version in versions)
            return {"status":True,"remoteProtocolVersion":max_version}
            print(max_version)  # Output will be 11
    else:
        return {'status':False,"errorMessage":"Timeout"}     
        


def isOnlineIpRelay(ip, port):
    te=""
    while te=="":
        print(f"cncli ping --host {ip} --port {port} --timeout-seconds 3")
        te = runcli(f"cncli ping --host {ip} --port {port} --timeout-seconds 3",timeout=4,timeout_return={'status':False,"errorMessage":"Timeout"} )
        if te!="":
            try:
                tej=json.loads(te)
                if  tej['status']=='ok':
                    if 'remoteProtocolVersion' not in tej:
                        tej['remoteProtocolVersion']=tej['networkProtocolVersion']
                    return {"status":True,"remoteProtocolVersion":tej['remoteProtocolVersion']}
                else:
                    return {"status":False,"errorMessage":tej['errorMessage']}
            except Exception as err:
                return {"status":False,"errorMessage":"Json Decode Failure"}
        else:
            print("retrying cncli")
            time.sleep(10)
        # retry
    

async def update_pool_ranking():

    lastRankedPool = 0
    pg.cur1_execute("select block as latestpoolrank from sync_status where key='latestpoolrank'")
    row=pg.cur1_fetchone()
    if row:
        latestpoolrank=row['latestpoolrank']
        if latestpoolrank>0:
            try:
                poolRanks=aws.load_s3("poolranks/poolranks"+str(latestpoolrank)+".json")
        
                # update firebase
                pg.cur1_execute("select rank, pool_id from pools where retired=false and genesis=false")
                row=pg.cur1_fetchone()
                
                while row:
                    pool=row['pool_id']  
                    if pool in poolRanks:
                        if row["rank"]!=poolRanks[pool]:
                            print("writing: " + str(pool) + " rank: " + str(poolRanks[pool]) )
                            fb.updateFb(baseNetwork+"/stake_pools/"+pool,{"r":poolRanks[pool]})
                            pg.cur2_execute("update pools set rank=%s where pool_id=%s",[poolRanks[pool],pool])
                            pg.conn_commit()
                            
                    else:
                        if row["rank"]!=(lastRankedPool + 1):
                            print("cleanup writing: " + str(pool) + " rank: " + str((lastRankedPool + 1)) )
                            fb.updateFb(baseNetwork+"/stake_pools/"+pool,{"r":(lastRankedPool + 1)})
                            pg.cur2_execute("update pools set rank=%s where pool_id=%s",[(lastRankedPool + 1),pool])
                            pg.conn_commit()
                    row=pg.cur1_fetchone()        
            except Exception as e:
                print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
                return False

    return True


async def update_metadata():
    pg.cur1_execute("select ticker, description, pool_name, itn_verified, pool_homepage, pool_id, metadata_hash, metadata_last_check, metadata_failure_count, metadata_error_string, metadata from pools where retired=false and genesis=false and ((metadata_error_string!='' and metadata_failure_count<100 and metadata_last_check+(3600*metadata_failure_count)<extract(epoch from now())::int) or metadata_last_check=0 or metadata_last_check is null)")
    row=pg.cur1_fetchone()
    while row:
        result = verifyAndLoadMetadata(row['metadata'],row['metadata_hash'],row['pool_id'])
        if result['success']:
            #update metadata details in pool records
            print(result)
            if result['poolMDerrorString']=='':
                

                pg.cur2_execute("update pools set ticker=%s, description=%s, pool_name=%s, itn_verified=%s, metadata_last_check=%s,metadata_error_string=%s, pool_homepage=%s, extended_json=%s, metadata_failure_count=0 where pool_id=%s",
                [result['ticker'],result['pool_description'],result['pool_md_name'],result['itn_verified'],result['last_checked'],result['poolMDerrorString'],result['pool_homepage'],Json(result['info']),row['pool_id']])
                pg.conn_commit()
                pool_update={}
                if row['ticker'] is None or (row['ticker'].strip()!=(result['ticker'].strip() if result['ticker'] is not None else '')):
                    pool_update["t"]=result['ticker'].strip() if result['ticker'] is not None else ''
                if row['pool_name'] is None or (row['pool_name'].strip()!=(result['pool_md_name'].strip() if result['pool_md_name'] is not None else '')):
                    pool_update["n"]=result['pool_md_name'].strip() if result['pool_md_name'] is not None else ''
                if (row['itn_verified'] if row['itn_verified'] is not None else False)!=result['itn_verified']:
                    pool_update["i"]=result['itn_verified'] if result['itn_verified'] is not None else False
                
                if len(pool_update):
                    fb.updateFb(baseNetwork+"/stake_pools/"+row['pool_id'],pool_update)
                
                fb.updateFb(baseNetwork+"/pool_stats/"+row['pool_id'],{"description":result['pool_description'],"homePage":result['pool_homepage'],"mdLastCheck":result['last_checked'],"metadataHash":row['metadata_hash'],"metadataUrl":row['metadata'],"poolMDerrorString":''})
            else:
                pg.cur2_execute("update pools set metadata_last_check=%s,metadata_error_string=%s, metadata_failure_count=pools.metadata_failure_count+1 where pool_id=%s",
                [result['last_checked'],result['poolMDerrorString'],row['pool_id']])
                pg.conn_commit()
                fb.updateFb(baseNetwork+"/pool_stats/"+row['pool_id'],{"mdLastCheck":result['last_checked'],"metadataHash":row['metadata_hash'],"metadataUrl":row['metadata'],"poolMDerrorString":result['poolMDerrorString']})
            # conn.commit()
        else:
            print(result)
            pg.cur2_execute("update pools set metadata_last_check=%s, metadata_error_string=%s, metadata_failure_count=pools.metadata_failure_count+1 where pool_id=%s",[result['last_checked'],result['poolMDerrorString'],row['pool_id']])
            pg.conn_commit()
            fb.updateFb(baseNetwork+"/pool_stats/"+row['pool_id'],{"mdLastCheck":result['last_checked'],"metadataHash":row['metadata_hash'],"metadataUrl":row['metadata'],"poolMDerrorString":result['poolMDerrorString']})
            # conn.commit()
        row=pg.cur1_fetchone()
    return True
    
async def write_tickers():
    print("write tickers")
    tickers={}
    pg.cur1_execute("select pool_id, ticker, itn_verified from pools where genesis=false")
    row=pg.cur1_fetchone()
    while row:
        tickers[row['pool_id']]={"ticker":str(row['ticker']).strip(),"itn_verified":True if row['itn_verified'] else False }  
        row=pg.cur1_fetchone()
    
    aws.dump_s3(tickers,"stats/tickers.json")
    tickerhash = myHash(json.dumps(tickers))
    aws.dump_s3({"hash":tickerhash,"tickers":tickers},"stats/tickers2.json")
    fb.updateFb(baseNetwork+"/ecosystem",{"tickerHash":tickerhash})
    return True
async def battle_trends():
    # load the existing trending battle data from s3
    trendingbattles=aws.load_s3("stats/trendingbattles.json")
    # print(trendingbattles)
    # return True
    battlestats={}
    pg.cur1_execute("select epoch, count(block_no) as battles, count(block_no) filter (where forker=true) as forkerblocks,count(block_no) filter (where slot_battle=true and forker=false) as slotbattleblocks,count(block_no) filter (where slot_battle=false and forker=false) as heightbattleblocks from (select epoch,block_no, bool_or(competitive) as competitive, bool_or(forker) as forker, case when  min(block_slot_no)=max(block_slot_no) then true else false end as slot_battle, array_agg(DISTINCT pool_id) as pool_ids,array_agg(DISTINCT pool_ticker) as pool_tickers from competitive_blocks where epoch>350 and (competitive=true or forker=true) group by epoch,block_no)  as a group by a.epoch order by epoch desc limit 10")
    rows=pg.cur1_fetchall()
    for row in rows:
        battlestats[int(row['epoch'])]={"epoch":int(row['epoch']),"battles":int(row['battles']),"forkerblocks":int(row['forkerblocks']),"slotbattleblocks":int(row['slotbattleblocks']),"heightbattleblocks":int(row['heightbattleblocks'])}
    pg.cur1_execute("select epoch, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY median) as prop_delay from competitive_blocks where epoch>350 group by epoch order by epoch desc limit 10")
    rows=pg.cur1_fetchall()
    for row in rows:
        if int(row['epoch']) not in battlestats:
            battlestats[int(row['epoch'])]={"epoch":int(row['epoch']),"battles":0,"forkerblocks":0,"slotbattleblocks":0,"heightbattleblocks":0}
        battlestats[int(row['epoch'])]['prop_delay']=int(row['prop_delay'])

    pg.cur1_execute("select epoch, avg(cbor_size) as avg_size, avg(transactions) as avg_tx, max(cbor_size) as max_size, min(cbor_size) as min_size from blocks where epoch>350 group by epoch order by epoch desc limit 10")
    rows=pg.cur1_fetchall()
    for row in rows:
        battlestats[int(row['epoch'])]['avg_size']=int(row['avg_size']/1000)
        battlestats[int(row['epoch'])]['avg_tx']=int(row['avg_tx'])
    trendingbattles=sorted(battlestats.values(),key=lambda x: x['epoch'], reverse=False)
    trendingbattleshash = myHash(json.dumps(trendingbattles))
    fb.updateFb(baseNetwork+"/ecosystem",{"trendingbattles":trendingbattleshash})
    aws.dump_s3({"hash":trendingbattleshash,"trendingbattles":trendingbattles},'stats/trendingbattles.json')  
    return True

async def main():
    pool_ranking=POOL_RANKING_PERIOD#POOL_RANKING_PERIOD #every 1 hour
    metadata=METADATA_PERIOD#METADATA_PERIOD#METADATA_PERIOD #every 10 minutes
    poolrelays=POOL_RELAYS#POOL_RELAYS#POOL_RELAYS#POOL_RELAYS#POOL_RELAYS
    writetickers=WRITE_TICKERS
    battletrends=BATTLE_TRENDS#BATTLE_TRENDS
    pledge_check=PLEDGE_CHECK
   
    
    while True:
        print("metadata: ",metadata," pool_ranking: ",pool_ranking,"poolrelays: ",poolrelays,"writetickers: ",writetickers,"battletrends",battletrends,"pledge_check",pledge_check)
        
        pledge_check=pledge_check-1
        if pledge_check<=0:
            if not await check_pledge_violations():
                print("check_pledge_violations failed.  pausing for 60 seconds before restarting")
                time.sleep(60)
                break
            else:
                pledge_check=PLEDGE_CHECK


        battletrends=battletrends-1
        if battletrends<=0:
            if not await battle_trends():
                print("battletrends failed.  pausing for 60 seconds before restarting")
                time.sleep(60)
                break
            else:
                print("battletrends succeeded")
                battletrends=BATTLE_TRENDS
        
        writetickers=writetickers-1
        if writetickers<=0:
            if not await write_tickers():
                print("write_tickers failed.  pausing for 60 seconds before restarting")
                time.sleep(60)
                break
            else:
                writetickers=WRITE_TICKERS


        poolrelays=poolrelays-1
        if poolrelays<=0:
            if not await update_pool_relays():
                print("poolrelays failed.  pausing for 60 seconds before restarting")
                time.sleep(60)
                break
            else:
                poolrelays=POOL_RELAYS

    
        pool_ranking=pool_ranking-1
        if pool_ranking<=0:
            if not await update_pool_ranking():
                print("update_pool_ranking failed.  pausing for 60 seconds before restarting")
                time.sleep(60)
                break
            else:
                pool_ranking=POOL_RANKING_PERIOD
        
        metadata=metadata-1
        if metadata<=0:
            if not await update_metadata():
                # if we fail then wait 10 seconds and then restart websockets
                print("update_metadata failed.  pausing for 10 seconds before restarting")
                time.sleep(60)
                break
            else:
                print("updated metadata")
                metadata=METADATA_PERIOD
        
        
        time.sleep(1)

# isOnlineIpRelayCardanoCli("relay1.ada.vegas",4001)

# exit()        
asyncio.run(main())
