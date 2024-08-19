import os
from config import *
print("doing epoch processing")

print("Gating epoch processing until epoch parameters and waitstake are written")
if os.system('python3 -u gateOnEpochParams.py') != 0:
    print("failed gateOnEpochParams.py")
    exit()

print("processing rewards")
if os.system('python3 -u processRewards.py') != 0:
    print("failed processRewards.py")
    exit()
print("processing forecast rewards")
if os.system('python3 -u processRewards.py forecast') != 0:
    print("failed processRewards.py forecast")
    exit()
print("processing postProcessRewards")
if os.system('python3 -u postProcessRewards.py') != 0:
    print("failed postProcessRewards.py")
    exit()
print("processing fmquery")
if os.system('python3 -u fmQuery.py') != 0:
    print("failed fmQuery.py")
    exit()
print("processing loyalty")
if os.system('python3 -u processLoyalty.py') != 0:
    print("failed processLoyalty.py")
    exit()
print("processing fixPoolBlocks")
if os.system('python3 -u fixPoolBlocks.py') != 0:
    print("failed fixPoolBlocks.py")
    exit()

print("processing calculatePropDelays")
if os.system('python3 -u calculatePropDelays.py') != 0:
    print("failed calculatePropDelays.py")
    exit()

#Note this will pause in a tight loop until at least 1 hour into epoch so will gate further processing behind it
print("processing assignedPerformanceAnalysis")
if os.system('python3 -u assignedPerformanceAnalysis.py') != 0:
    print("failed assignedPerformanceAnalysis.py")
    exit()

#note, this will archive all reward history to s3 to save database storage.  it takes about 8 hoursrun for each epoch  we wait until slot 40,000 to make sure 
#the telegram bot is done
#disablling this for now until we build at least 10 epochs worth of history (not forecasted epochs) so we can run loyalty again.
# print("archive stake history")
# if os.system('python3 -u archiveStakeHistoryFast.py') != 0:
#     print("failed archiveStakeHistoryFast.py")
#     exit()
