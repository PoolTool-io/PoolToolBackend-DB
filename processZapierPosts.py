from config import *
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
pg=pg_utils('processZapierPosts')

def send_media_post_zapier(package):
    print(package)
    pg.cur1_execute("select zapier_url from zapier_triggers where trigger_type='new_post' and user_id=%s",[user_id])
    row=pg.cur1_fetchone()
    while row:
        result=trigger_zapier_hook(row['zapier_url'],package)
        print(result)
        if 'failure' in result or ('status' in result and result['status']!='success'):
            # log to the retry queue for later
            pg.cur2_execute("insert into zapier_retry_triggers (zapier_url,package) values(%s,%s)",[row['zapier_url'],Json(package)])
            pg.conn_commit()
        row=pg.cur1_fetchone()
        

while True:
    print("checking for new posts")
    new_posts = fb.getKey(baseNetwork+"/mediaPosts/New/")
    if new_posts is not None:
        for user_id in new_posts:
            posts=[]
            for key in  new_posts[user_id]:
                value=new_posts[user_id][key]
                value['key']=key
                posts.append(value)
            posts.sort(key=lambda x: x['createdAt'])
            print(posts)
            for post in posts:
                try:
                    send_media_post_zapier(post)
                    fb.pushFb(baseNetwork+"/mediaPosts/Complete/"+user_id,post)
                    fb.deleteFb(baseNetwork+"/mediaPosts/New/"+user_id+"/"+post['key'])
                    
                except Exception as e:

                    print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    
    time.sleep(30)
