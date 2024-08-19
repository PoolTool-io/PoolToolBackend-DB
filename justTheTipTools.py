import hashlib
import binascii
import time
from config import *
from pt_utils import *
import subprocess, platform
from fb_utils import fb_utils
from aws_utils import aws_utils
from pg_utils import *
from demeter_run_utils import *
demeter= demeter_run_utils()
fb=fb_utils()
aws=aws_utils()
pg=pg_utils("consumerTools")
watchAddresses=['addr1qykeqj78jj98ulnkmjt0dxh5ya4pcw27qy7l4z5dkzyr5grszxlk5cj4w7utxxngjzjc2rc0ht9hmxr0zh493gyyks0qh0cexs','addr1q9hpgu76s9usmqg8kwjtehs32ux6pkq4e394hzvk7ffnd50czdv6hfcmfgr09y6lpvlpl5nfnemg8c768kvl0vada5tqdmvnmd']




def consumeTransaction(args,current_epoch):
    if 'outputs' in args['transaction'] and args['transaction']['outputs'] is not None:
        for output in args['transaction']['outputs']:
            if 'address' in output and 'amount' in output:
                if output['address'] in watchAddresses:

                    input_addresses=[]
                    input_stake_keys=[]
                    print("verification payment")
                    # get all of the input utxos to insert into the auth table.
                    # so now we will query db sync for this transaction to see what the inputs are:
                    demeter.cur1_execute("select address from tx left join tx_in on tx_in.tx_in_id=tx.id left join tx_out on tx_out.tx_id=tx_in.tx_out_id and tx_in.tx_out_index=tx_out.index  where hash = decode(%s, 'hex')",[args['transaction']['hash']])
                    row=demeter.cur1_fetchone()
                    while row:
                        if row['address'] not in input_addresses:
                            input_addresses.append(row['address'])
                            if row['address'][0:4]=="addr":
                                addr=convertBech32(row['address'])
                            else:
                                addr = row['address']
                            if len(addr)==114:
                                stadd = addr[58:114]
                            else:
                                stadd = None
                            if stadd is not None:
                                input_stake_keys.append(stadd)
                        row=demeter.cur1_fetchone()
                    print("input addresses",input_addresses)
                    print("input stake keys",input_stake_keys)
                    
                    if len(input_stake_keys):
                        print("found tx, inserting into auth_watch")
                        pg.cur1_execute("insert into auth_watch(to_address,amount,from_address_array,from_stakekey_array,timestamp) values(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        [output['address'],output['amount'],input_addresses,input_stake_keys,args['context']['timestamp']])
                        pg.conn_commit()


        

