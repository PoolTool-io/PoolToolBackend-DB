
from config import *
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import subprocess, platform
from fb_utils import *
from pt_utils import *
from pg_utils import *
import uuid
import sys
import re
import bcrypt
import random
import string
fb=fb_utils()
uuid4hex = re.compile('^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z', re.I)
pg=pg_utils('restApiCnodeServer')



app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})



@app.route('/waitstakewritten',methods=['POST'])
@cross_origin()
def waitstakewrittenHandler():
    args=request.get_json()
    if 'epoch' in args:
        epoch=args['epoch']
        

        # done with waitstake writing, allow reward pivoting to start assuming all reward processing is done
        fb.updateFb(baseNetwork+"/epoch_processing",{"waitStakeEpochDone":int(epoch)})



        
    return {"success":True}

@app.route('/saveepochdata',methods=['POST'])
@cross_origin()
def saveepochdataHandler():
    args=request.get_json()
    if 'pool_blocks' in args and 'epoch_params' in args and 'epoch' in args:
        #print(args)

        #    {'00000036d515e12e18cd3c88c74f09a67984c2c279a5296aa96efe89': 66, '000000f66e28b0f18aef20555f4c4954234e3270dfbbdcc13f54e799': 54, '00000110093effbf3ce788aebd3e7506b80322bd3995ad432e61fad5': 4, '000001b844f4e4c900ae0dfdc84a8845f71090b82fb473e6c70a31ee': 12, '00000368a25a58d3c46bea611ddabf77679b8d1fd854c75a41ea399c': 4, '000006d97fd0415d2dafdbb8b782717a3d3ff32f865792b8df7ddd00': 62, '00000a1e448bdd902bb3884d1df7f26efaec3afadfeb719f65cf0051': 1, '00000aedaa1a5abf500cbc4c5c23761a2dff570bc4a299983c893f8e': 24, '00004614332ac81201d8302d4cb8262502af229256e143dc2156156e': 51, '0000558092d1645b110130bb1b9d449dced79bfccf313e2880e6cf38': 3, '0000fc522cea692e3e714b392d90cec75e4b87542c5f9638bf9a363a': 6, '0001a003afb844ce6d9409fc49e049db654a78dc77f1151cb6cd548f': 38, '0019cb5ac91c786a809f4d87622a29d2c7f57f4697f8c8e8457f4de4': 33, '00333b89543da962cc92e7ffa45848f42a98f7276780670728ce5256': 17, '003da6afe37eca8d1bec9a040a51f23eb085c3e1b924948a214067e7': 14, '003e98e43df6cfe3bc2a91a9a3978a07dfa5b9ead7e91f79615d9428': 13, '0054fc7a4e34f9a1eef15a05fdfc9d323deeb9b72a4b5ec514247e66': 17, '0084f4fee5502c87ee5c4f5c592856f2bfb6269355b9d87ed549e551': 2, '00beef0a9be2f6d897ed24a613cf547bb20cd282a04edfc53d477114': 28, '00beef284975ef87856c1343f6bf50172253177fdebc756524d43fc1': 11, '00beef373356ff27a77bd510f4cd35d309715afb6f52f49d9186a16e': 34, '00beef534478b59ba0b2b646348c8d1f81f60bf40a2195c6378dd5db': 4, '00beef68425d90f50c8e2102476a0f42ec850836e9affd601ecf7804': 4, '00beef8710427e328a29555283c74b202b40bec9a62630a9f03b1e18': 28, '00beef9385526062d41cd7293746048c6a9a13ab8b591920cf40c706': 16, '015f52ee0b1b29813884f85313666f3876c8260858f9572622660bc3': 20, '01cd4b51c0d0a10bb658d9058409dd034bb4a0a9207bdcb8e6850be8': 19, '01fbe26f514707ff29c38aa597ad8ff86a1572e760858d0e7ae4fc32': 8, '0236ec1a0769c1b1bd8b6400d2f0e8e206cccd77a3700e5b7cf172a8': 24, '024dcb42f0aa6d81a7e26ccdd525a2ed3e9665d126b38ba0f8b77b50': 60}
        #{'treasury': 1305029315722194, 'reserves': 9063032557660485, 'epoch_fees': 141402900482, 'decentralisation': 0, 'maxBlockSize': 90112, 'maxBhSize': 1100, 'maxEpoch': 18, 'optimalPoolCount': 500, 'influence': 0.3, 'monetaryExpandRate': 0.003, 'treasuryGrowthRate': 0.2, 'protocolMajor': 8, 'protocolMinor': 0, 'reward_pot': 2}
        reward_pot=args['epoch_params']['reward_pot']
        epoch=args['epoch']
        block_production_epoch=epoch+1
        epoch_params=args['epoch_params']
        epoch_feess=args['epoch_params']['epoch_fees']
        print(f"insert into epoch_params (reward_pot, epoch,epoch_feess) values(%s,%s,%s) ON CONFLICT ON CONSTRAINT epparams_idx DO UPDATE set reward_pot=%s, epoch_feess=%s",[reward_pot,(epoch-2),epoch_feess,reward_pot,epoch_feess])
        pg.cur1_execute("insert into epoch_params (reward_pot, epoch,epoch_feess) values(%s,%s,%s) ON CONFLICT ON CONSTRAINT epparams_idx DO UPDATE set reward_pot=%s, epoch_feess=%s",[reward_pot,(epoch-2),epoch_feess,reward_pot,epoch_feess])
        pg.conn_commit()

        total_epoch_blocks=0
        epoch_blocks={}
        # blocks produced epoch-1
        total_epoch_blocks=sum(args['pool_blocks'].values())
        print(total_epoch_blocks)

        pg.cur1_execute("select pool_id,block_count from pool_epoch_blocks where epoch=%s",[epoch - 1])
        row=pg.cur1_fetchone()
        pg_epoch_blocks={}
        while row:
            pg_epoch_blocks[row['pool_id']]=row['block_count']
            row=pg.cur1_fetchone()
        for pool_id in args['pool_blocks']:
            if pg_epoch_blocks[pool_id]==args['pool_blocks'][pool_id]:
                
                del pg_epoch_blocks[pool_id]
            else:
                print("mismatch",block_production_epoch,pool_id,pg_epoch_blocks[pool_id],args['pool_blocks'][pool_id])
                
                pg.cur1_execute("update pool_epoch_blocks set block_count=%s where pool_id=%s and epoch=%s",[args['pool_blocks'][pool_id],pool_id,epoch - 1])
                pg.conn_commit()
                print("fixing - please recheck lifetime totals")
                del pg_epoch_blocks[pool_id]
        if len(pg_epoch_blocks):
            for pool_id in pg_epoch_blocks:
                if pool_id not in genesispools:
                    print("we have extra pool blocks line the list")
                    #print(pg_epoch_blocks)
                    return jsonify({'error': 'Some specific error message'}), 400
        print("blocks verified for ",(epoch -1))

        pg.cur1_execute("select count(*) as blockcount,sum(fees) as fees from blocks where epoch=%s",[(epoch-1)])
        row=pg.cur1_fetchone()
        total_epoch_blocks=int(row['blockcount']) #19345
        total_epoch_fees=int(row['fees'])
        epoch_params["expectedBlocks"]=432000*0.05
        print(total_epoch_blocks,total_epoch_fees)
        calculatedRewardPot = int((((epoch_params["reserves"] * (total_epoch_blocks /epoch_params["expectedBlocks"])  * epoch_params["monetaryExpandRate"])+ int(total_epoch_fees)) * (1-epoch_params["treasuryGrowthRate"])) )
        print(calculatedRewardPot)


        

        print(epoch_params)
        fb.updateFb(baseNetwork+"/epoch_params/"+str(block_production_epoch),epoch_params)

        pg.cur1_execute("""insert into epoch_params (decentralisation, expected_blocks, influence, max_bh_size, max_block_size, max_epoch, 
            monetary_expansion_rate, optimal_pool_count, protocol_major, protocol_minor, treasury_growth_rate, epoch, reserves, 
            treasury,reward_pot,epoch_feess,epoch_blocks) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT epparams_idx DO UPDATE 
            set decentralisation=%s,expected_blocks=%s, influence=%s, max_bh_size=%s, max_block_size=%s, max_epoch=%s, 
            monetary_expansion_rate=%s, optimal_pool_count=%s, protocol_major=%s, protocol_minor=%s, treasury_growth_rate=%s, reserves=%s, 
            treasury=%s,reward_pot=%s,epoch_feess=%s,epoch_blocks=%s """,[
            epoch_params["decentralisation"],epoch_params['expectedBlocks'],epoch_params['influence'],epoch_params['maxBhSize'],epoch_params['maxBlockSize'],
            epoch_params['maxEpoch'],epoch_params['monetaryExpandRate'],epoch_params['optimalPoolCount'],epoch_params['protocolMajor'],epoch_params['protocolMinor'],epoch_params['treasuryGrowthRate'],(epoch-1),epoch_params['reserves'],epoch_params['treasury'],calculatedRewardPot,total_epoch_fees,total_epoch_blocks,
            epoch_params["decentralisation"],epoch_params['expectedBlocks'],epoch_params['influence'],epoch_params['maxBhSize'],epoch_params['maxBlockSize'],
            epoch_params['maxEpoch'],epoch_params['monetaryExpandRate'],epoch_params['optimalPoolCount'],epoch_params['protocolMajor'],epoch_params['protocolMinor'],epoch_params['treasuryGrowthRate'],epoch_params['reserves'],epoch_params['treasury'],calculatedRewardPot,total_epoch_fees,total_epoch_blocks
        ])
        print(f"""insert into epoch_params (decentralisation, expected_blocks, influence, max_bh_size, max_block_size, max_epoch, 
            monetary_expansion_rate, optimal_pool_count, protocol_major, protocol_minor, treasury_growth_rate, epoch, reserves, 
            treasury,reward_pot,epoch_feess,epoch_blocks) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT epparams_idx DO UPDATE 
            set decentralisation=%s,expected_blocks=%s, influence=%s, max_bh_size=%s, max_block_size=%s, max_epoch=%s, 
            monetary_expansion_rate=%s, optimal_pool_count=%s, protocol_major=%s, protocol_minor=%s, treasury_growth_rate=%s, reserves=%s, 
            treasury=%s,reward_pot=%s,epoch_feess=%s,epoch_blocks=%s """,[
            epoch_params["decentralisation"],epoch_params['expectedBlocks'],epoch_params['influence'],epoch_params['maxBhSize'],epoch_params['maxBlockSize'],
            epoch_params['maxEpoch'],epoch_params['monetaryExpandRate'],epoch_params['optimalPoolCount'],epoch_params['protocolMajor'],epoch_params['protocolMinor'],epoch_params['treasuryGrowthRate'],(epoch-1),epoch_params['reserves'],epoch_params['treasury'],calculatedRewardPot,total_epoch_fees,total_epoch_blocks,
            epoch_params["decentralisation"],epoch_params['expectedBlocks'],epoch_params['influence'],epoch_params['maxBhSize'],epoch_params['maxBlockSize'],
            epoch_params['maxEpoch'],epoch_params['monetaryExpandRate'],epoch_params['optimalPoolCount'],epoch_params['protocolMajor'],epoch_params['protocolMinor'],epoch_params['treasuryGrowthRate'],epoch_params['reserves'],epoch_params['treasury'],calculatedRewardPot,total_epoch_fees,total_epoch_blocks
        ])
        pg.conn_commit()
        print("writing epoch params for epoch ",(epoch-1))
        print(epoch_params)

        #reward processing is ready to proceed
        fb.updateFb(baseNetwork+"/epoch_processing",{"epochParamsEpochDone":int(epoch)})
        #call reward processing tools, perhaps set a semaphore
        


    return {"success":True}


@app.route('/savepoolranking',methods=['POST'])
@cross_origin()
def savepoolrankingHandler():
    args=request.get_json()
    if 'latestpoolrank' in args:
        pg.cur1_execute("update sync_status set block=%s where key='latestpoolrank'",[int(args['latestpoolrank'])])
        pg.conn_commit()
    
    return {"success":True}



if __name__ == '__main__':
    #testing change
  app.run(debug = True, host = '0.0.0.0',port=8333,use_reloader=False)
