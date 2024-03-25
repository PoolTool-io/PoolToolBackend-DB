# PoolToolBackend
PoolTool Backend Processing Toolset (python)

## Overall architecture
Pooltool is currently operating across two different servers.
CNODE SERVER:  Has cardano node, ogmios and txpipe oura system.   The focus is on following the chain, parsing ledger state dumps, and sending all data to the database server for processing.
DATABASE SERVER:  Has a postgres database and all remaining processing for pooltool.  In addition, due to legacy code, we use a sqlite database with the telegram bot on this server.

## Databases
We use a postgres database on the DATABASE SERVER to maintain high speed data access.  In addition we use a firebase instance to maintain live data for the website and the pooltool mobile app.  Finally we store static information on aws s3 as much as possible to minimize cost and keep it out of the expensive postgres database system.

## DATABASE SERVER:

### Primary Files
- ouraConsumer.py - Process oura dataset writes from CNODE server
- epoch_processing.py - master control file for epoch processing steps as triggered by CNODE server.
- periodicProcesses.py - process pool ranking, metadata checks, pool relays, ticker summaries, battle trends and pledge checking on a routine basis.
- processTips.py - processes all dynamo db data from tipdata loaded in through aws api.  executes one loop per second.
- processOrphans.py - continuously scans all recent blocks and matches up heights/slots to flag orphans, battles, forkers.
- processZapierPosts.py - continuously scan for zapier posts to push up
- cnodeServerAPI.py - 

### Utility and Helper Files
- pt_utils.py - misc utility pooltool and crypto functions
- fb_utils.py - misc utilty functions for interacting with the firebase database
- pg_utils.py - misc utility functions for itneracting with local postgres database
- aws_utils.py - misc utility functions for interacting with AWS (mostly s3)
- consumerTools.py - utility functions for processing blocks/transactions/etc as they come in from the oura daemons.
- restAPI.py - consume api calls through the aws api front end.  Handles pivoting rewards, zapier hooks.

- gateOnEpochParams.py - First step in epoch processing.   write live pool params, snapshot stake from ledger state data dump.
- processRewards.py - Second step in epoch processing.  calculate rewards (optional forecasted rewards) and write to databases
- postProcessRewards.py - Third step in epoch processing.  Pivot rewards for legacy users and trigger start of notify telegram users of reward details.
- fmQuery.py - output pool rewards to a csv file.  This is a legacy setup that may not be required any longer. 
- processLoyalty.py - Calculate and post loyalty details for delegators
- fixPoolBlocks.py - make sure we have entries in pool blocks data sets for pools with 0 blocks in an epoch
- calculatePropDelays.py - calculate all prop delays and post data
- assignedPerformanceAnalysis.py - calculate and post all assigned performance details
- archiveStakeHistoryFast.py - This will transfer the reward calculation data from postgres to s3 for long term storage and to reduce postgres db size
- pivotRewards.py - shell to call pivotRewardsFunction.py
- pivotRewardsFunction.py - rotates rewards for consumption by front end.