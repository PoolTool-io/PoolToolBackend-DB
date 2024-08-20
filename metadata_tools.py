from datetime import datetime, timedelta
from pt_utils import * 
from config import *
import json
import psycopg2.extras
from psycopg2.extras import Json


conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

def verifyAndLoadMetadata(url,expectedhash,pool_id):
    print(url)
    ret = {'success': False,'poolMDerrorString': '','pool_description':'','pool_homepage':'','ticker':'','pool_md_name':'','itn_verified':None, 'last_checked': int(datetime.now().timestamp()) }
    #te = runcli(f"wget --spider -T 10 -t 1 { url } 2>&1 | awk '/Length/ {{ print $2 }}'",None,True)
    te = runcli(f"curl -m 10 -k -s { url } -L  -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:33.0) Gecko/20100101 Firefox/33.0' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' -H 'Accept-Language: en-US,en;q=0.5' -H 'Referer: https://pooltool.io' -H 'Cookie: all required cookies will appear here' -H 'Connection: keep-alive'  --compressed  | head -c 1024 | wc -c",None,True)
    
    if te is not None and int(te) > 0 and int(te) <= 512: #is not None and te!='' and te.rstrip()!="unspecified":
        if int(te) < 200000:
            te = runcli(f"wget -T 10 -t 1 --no-check-certificate -O /tmp/testjson { url } 2>&1 ",None,True)

            if te is not None:
                te = runcli(f"{cardanocli} stake-pool metadata-hash --pool-metadata-file /tmp/testjson",None,True)
                if te is not None and expectedhash == te.rstrip():
                    # decode the file and load the data
                    with open('/tmp/testjson') as f:
                        try:
                            data = json.load(f)
                            ret['success']=True
                            ret['info']=None
                            ret['pool_description']=data['description']
                            ret['ticker']=data['ticker']
                            ret['pool_homepage']=data['homepage']
                            ret['pool_md_name']=data['name']
                            if ('itn_witness' in data or 'extended' in data):
                                # we are also validating a witness ITN
                                # 1.  get file
                                if 'itn_witness' in data:
                                    url = data['itn_witness']
                                else:
                                    url = data['extended']
                                # te = runcli(f"wget --spider -T 10 -t 1 { url } 2>&1 | awk '/Length/ {{ print $2 }}'",None,True)
                                # if te is not None and te!='' and te.rstrip()!="unspecified":
                                te = runcli(f"curl -m 10 -k -s { url } -L | head -c 204800 | wc -c",None,True)
                                if int(te) > 0 and int(te) <= 200000: #is not None and te!='' and te.rstrip()!="unspecified":

                                    te = runcli(f"wget -T 10 -t 1 --no-check-certificate -O /tmp/testwitnessjson { url } 2>&1 ",None,True)
                                    with open('/tmp/testwitnessjson') as g:
                                            try:
                                                extended_data = json.load(g)
                                                if 'itn' in extended_data and extended_data['itn'] is not None and extended_data['itn']!='':
                                                    data = extended_data['itn']
                                                    if ('itn_owner' in data or 'owner' in data) and 'witness' in data:
                                                        # 2.  extract itn_owner and witness from it
                                                        if 'itn_owner' in data and data['itn_owner'] is not None and data['itn_owner']!='':
                                                            itn_owner = data['itn_owner']
                                                        elif 'owner' in data and data['owner'] is not None and data['owner']!='':
                                                            itn_owner = data['owner']
                                                        else:
                                                            itn_owner = None
                                                        if itn_owner is not None:
                                                            witness = data['witness']
                                                            # 3.  get itn_details from metadata registry and extract ticker from that
                                                            url = "https://raw.githubusercontent.com/cardano-foundation/incentivized-testnet-stakepool-registry/master/registry/"+itn_owner+".json"
                                                            
                                                            te = runcli(f"wget --spider -T 10 --no-check-certificate -t 1 { url } 2>&1 | awk '/Length/ {{ print $2 }}'",None,True)
                                                            if te is not None and te!='' and te.rstrip()!="unspecified":

                                                                if int(te) < 200000:
                                                                    te = runcli(f"wget -T 10 -t 1 -O /tmp/itn_registry_data { url } 2>&1 ",None,True)
                                                                    with open('/tmp/itn_registry_data') as h:
                                                                        try:
                                                                            data = json.load(h)
                                                                            if 'ticker' in data:
                                                                                # 4.  verify tickers are the same
                                                                                if data['ticker']==ret['ticker']:
                                                                                    # 5.  verify witness matches for this pool id
                                                                                    # save pubkey, poolsig, mainnetpoolid to file
                                                                                    file_object  = open("/tmp/pubkey", "w")
                                                                                    file_object.write(itn_owner)
                                                                                    file_object.close()

                                                                                    file_object  = open("/tmp/poolsig", "w")
                                                                                    file_object.write(witness)
                                                                                    file_object.close()
                                                                                    te = runcli(f"echo \"{pool_id}\" > /tmp/mainnetpoolid",None,True)
                                                                                    te = runcli(f"./jcli key verify --public-key /tmp/pubkey --signature /tmp/poolsig /tmp/mainnetpoolid",None,True)
                                                                                    # print(te)
                                                                                    # exit()
                                                                                    if te is not None and te.strip() == "Success":
                                                                                        ret['itn_verified']=True
                                                                                        print("Confirmed with echo mainnetpoolid")
                                                                                    else:
                                                                                        # try one more time without a new line character
                                                                                        te = runcli(f"echo \"{pool_id}\c\" > /tmp/mainnetpoolid",None,True)
                                                                                        te = runcli(f"./jcli key verify --public-key /tmp/pubkey --signature /tmp/poolsig /tmp/mainnetpoolid",None,True)

                                                                                        if te is not None and te.strip() == "Success":
                                                                                            ret['itn_verified']=True
                                                                                            print("Confirmed with echo mainnetpoolid removed end of line")
                                                                                        else:
                                                                                            ret['itn_verified']=False
                                                                                            ret['poolMDerrorString'] = 'ITN metadata could not be verified.  Usually that means the method you used to create your poolid file is not the same.  We use: echo "<pool id>" > poolid.file'


                                                                                else:
                                                                                    ret['poolMDerrorString'] = 'ITN metadata ticker does not match mainnet ticker.  Unable to validate ticker.'

                                                                            else:
                                                                                ret['poolMDerrorString'] = 'Unable to find ticker in itn registry data'

                                                                        except ValueError as e:
                                                                            ret['poolMDerrorString'] = 'Unable to extract itn registry data'
                                                                else:
                                                                    ret['poolMDerrorString'] = 'itn registry data too large'
                                                            else:
                                                                ret['poolMDerrorString'] = 'Unable to find itn registry data at ' + url

                                                    else:
                                                        ret['poolMDerrorString'] = 'ITN witness datafile found, but unable to extract itn_owner and witness from it'
                                                else:
                                                    ret['poolMDerrorString'] = ''
                                                if 'info' in extended_data:
                                                    ret['info']=extended_data['info']
                                                
                                            except ValueError as e:
                                                ret['poolMDerrorString'] = 'metadata is valid but unable to decode extended data file'
                                else:
                                    ret['poolMDerrorString']=""





                                # 6.  add ITN verified

                        except ValueError as e:
                            ret['poolMDerrorString']="json is not valid"
                elif te is not None:
                    ret['poolMDerrorString']="hash does not match, expecting: " + expectedhash + " but got: " + te.rstrip()
                else:
                    ret['poolMDerrorString']="hash does not match, expecting: " + expectedhash + " but got an error from cardano-cli with this file"
                te = runcli(f"rm /tmp/testjson",None,True)

            else:
                ret['poolMDerrorString']="failure to download file"
        else:
            ret['poolMDerrorString']="file size too large (pooltool limit is 200kb max)"
    else:
        ret['poolMDerrorString']="file cannot be found"
    # if pool_id == "63481af9b33b8c46f7a5b35147fb37a385f6133801c4595b8bd524f2":
    #     print(ret)
    #     exit()
    # if pool_id=="aada0ed2b22d9d7304d1052821a16a460ff058ab1dae9cfb71d26e1f":
    #     exit()
    return ret
