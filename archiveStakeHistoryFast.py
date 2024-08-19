from concurrent.futures import ThreadPoolExecutor,  as_completed
from threading import Lock, Thread, Semaphore
import time
from config import *
from pg_utils import *
from aws_utils import *
from fb_utils import *

pg = pg_utils("archiveStakeHistoryFast")
fb = fb_utils()
aws = aws_utils()

stop_flag = False  # Global flag to signal all threads to stop
sem = Semaphore(10)  # Limit to 1000 concurrent tasks

def process_row(row, target_epoch, counter, lock):
    global stop_flag, sem
    with sem:
        if stop_flag:
            return
        stake_shard = row['stake_key'][0:6]
        try:
            stake_history = aws.load_s3(f"stake_history/{stake_shard}/{row['stake_key']}.json")
        except:
            stake_history = {}
        #print(f"stake_history/{stake_shard}/{row['stake_key']}.json")
        
        stake_history[str(target_epoch)]={"epoch":int(row['epoch']),"stake_key":row['stake_key'],"amount":int(row['amount']),"delegated_to_pool":str(row['delegated_to_pool']) if row['delegated_to_pool'] is not None else 'None',"delegated_to_ticker":(str(row['delegated_to_ticker'])).strip() if row['delegated_to_ticker'] is not None else "","operator_rewards":int(row['operator_rewards'])
            ,"stake_rewards":int(row['stake_rewards']),"rewards_sent_to":str(row['rewards_sent_to']),"reward_address_details":(row['reward_address_details'])}
        aws.dump_s3(stake_history, f"stake_history/{stake_shard}/{row['stake_key']}.json")
        
        with lock:
            counter[0] += 1

def print_statistics(counter, lock, total_rows, start_time):
    global stop_flag
    while not stop_flag:
        time.sleep(10)  # Sleep for 60 seconds
        elapsed_time = time.time() - start_time
        with lock:
            updates_per_minute = counter[0] / (elapsed_time / 60)
            remaining_rows = total_rows - counter[0]
            if remaining_rows > 0:
                estimated_time_remaining = (remaining_rows / updates_per_minute) * 60  # in seconds
                print(f"Updates per minute: {updates_per_minute}")
                print(f"Estimated time remaining: {estimated_time_remaining:.2f} seconds")
            else:
                print("Processing complete.")
                break


def archive_epoch(target_epoch):
    pg.cur1_execute("select * from stake_history where epoch=%s", [target_epoch])
    
    rows = pg.cur1_fetchall()
    total_rows = len(rows)
    
    counter = [0]  # Shared counter
    lock = Lock()  # Lock to protect shared counter

    futures = []
    start_time = time.time()
    # Start the statistics printing thread
    stats_thread = Thread(target=print_statistics, args=(counter, lock, total_rows, start_time))
    stats_thread.start()

    with ThreadPoolExecutor(max_workers=20) as executor:
        for row in rows:
            future = executor.submit(process_row, row, target_epoch, counter, lock)
            futures.append(future)
        for future in as_completed(futures):
            pass  # You can add additional logic here if needed

    # Wait for the statistics thread to complete
    stats_thread.join()

    
    pg.cur1_execute("update sync_status set block=%s where key='arch_stake_hist'", [target_epoch])
    pg.conn_commit()
    print(f"Archived stake history for epoch {target_epoch}")
    print(f"Now deleting stake history for epoch {target_epoch}")
    pg.cur1_execute("delete from stake_history where epoch=%s", [target_epoch])
    pg.conn_commit()

try:

    # wait until at least slot 40000 to process.  thats one hour after the epoch switch
    pg.cur1_execute("select epoch_slot,epoch from blocks order by block desc limit 1")
    row=pg.cur1_fetchone()
    epoch_slot=row['epoch_slot']
    while epoch_slot<20000:
        print("waiting for slot 40000, currently at slot: ", epoch_slot)
        time.sleep(60)
        pg.cur1_execute("select epoch_slot,epoch from blocks order by block desc limit 1")
        row=pg.cur1_fetchone()
        epoch_slot=row['epoch_slot']

    pg.cur1_execute("select max(epoch) as epoch from epoch_params")
    row=pg.cur1_fetchone()
    if row is not None:
        finish_epoch=int(row['epoch'])-5
        print(finish_epoch)
        pg.cur1_execute("select block as epoch from sync_status where key='arch_stake_hist'")
        row=pg.cur1_fetchone()
        print(row)
        if row is not None:
            target_epoch = int(row['epoch'])+1
            print(target_epoch)
            while target_epoch<=finish_epoch:
                print("Archiving epoch "+str(target_epoch))
                archive_epoch(target_epoch)
                target_epoch = target_epoch+1
except KeyboardInterrupt:
    stop_flag = True
    print("Stopping...")
    
