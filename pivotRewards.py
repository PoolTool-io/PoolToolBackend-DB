from config import *
from fb_utils import *
from pg_utils import *
from pivotRewardsFunction import *
import sys


silent=False
fb=fb_utils()
pg=pg_utils("pivotRewards")

if len(sys.argv)>1:
    if not silent:
        print(sys.argv[1])
    if len(sys.argv[1])==56:
        stake_key=sys.argv[1]
        pivotRewardsArchived(stake_key,pg,fb,baseNetwork)
        if not silent:
            print("success")