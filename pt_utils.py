from os import environ,path,popen
import subprocess, platform
from subprocess import TimeoutExpired
import requests
from six.moves import urllib
import json
import math
from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()
environ["CNODE_HOME"] = "/opt/cardano/cnode"
environ["CARDANO_NODE_SOCKET_PATH"] = environ["CNODE_HOME"] + "/sockets/node0.socket"

def myHash(text:str):
    hash=0
    for ch in text:
        hash = ( hash*281  ^ ord(ch)*997) & 0xFFFFFFFF
    return hash


def human_format(number):
    if number is not None and number != 0:
        units = ['', 'K', 'M', 'G', 'T', 'P']
        k = 1000.0
        magnitude = int(math.floor(math.log(number, k)))
        if number <10:
            return '%.2f%s' % (number / k**magnitude, units[magnitude])
        else:
            return '%.1f%s' % (number / k**magnitude, units[magnitude])
    else:
        return '0'


def runcli2(runstring, timeout=10, timeout_return="", raw=False, _parse=False):
    completed_process = None
    try:
        completed_process = subprocess.run(
            str(runstring),
            shell=True,
            executable=get_exec_sh(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            text=True
        )

        output = completed_process.stdout

        if "failed to make a REST request" in output:
            return errorstring if errorstring is not None else None
        
        if raw:
            return output

        if _parse:
            return parse_yaml(output)

        return output.strip()  # Replaces all whitespace sequences and newlines with a single space

    except TimeoutExpired:
        # Handle the timeout exception by terminating the process
        if completed_process:
            completed_process.kill()
        # You can also gather the stderr output if needed:
        # stderr_output = completed_process.stderr
        return timeout_return
    except Exception as e:
        # Handle other potential exceptions
        print(f"An error occurred: {e}")
        return errorstring if errorstring is not None else None

def runcli(runstring, errorstring=None, raw=False, _parse=False, timeout=None,timeout_return=False):
    try:
        output = subprocess.check_output(
            str(runstring),
            shell=True,
            executable=get_exec_sh(),
            timeout=timeout
        ).decode()

        if output.find("failed to make a REST request") < -1:
            return
        if raw is True:
            return output

        if _parse is True:
            return parse_yaml(output)

        return output.replace("\n", "")

    except subprocess.CalledProcessError:
        print(f'Error running command: {runstring}\n')
    except subprocess.TimeoutExpired:
        return timeout_return

def get_exec_sh():
    os = platform.platform().lower()
    executable = None
    if "darwin-19" in os:
        executable = "/bin/sh"
    elif "debian" in os:
        executable = "/bin/sh"
    return executable

def convertBech32(bech32):
    try:
        output = subprocess.check_output(
            str("echo " + bech32 + " | /home/ubuntu/.cabal/bin/bech32"),
            shell=True,
            executable=get_exec_sh()
        ).decode()
        return output.replace("\n", "")
    except subprocess.CalledProcessError:
        print(f'Error running command convertBech32\n')

def convertFromBech32(hex,prefix):
    try:
        output = subprocess.check_output(
            str("echo " + hex + " | /home/ubuntu/.cabal/bin/bech32 "+prefix),
            shell=True,
            executable=get_exec_sh()
        ).decode()
        return output.replace("\n", "")
    except subprocess.CalledProcessError:
        print(f'Error running command convertBech32\n')

def trigger_zapier_hook(url,package):
    try:
        x = requests.post(url,json=package,timeout=2.50)
        
        return x.json()
    except:
        print("failed to trigger zapier")
        return {"failure":True}

def getIpHostProvider(ips):
    package={"ips":ips}
    try:
        x = requests.post(f"https://api.incolumitas.com/datacenter",json=package)
        return x.json()
    except:
        print("failed to get iphost")
        return {"failure":True}
        
    
def ptGetPrices():
    currencies = [     "btc",  "eth",  "ltc",  "bch",  "bnb",  "eos",  "xrp",  "xlm",  "link",  "dot",  "yfi",  "usd",  "aed",  "ars",  "aud",  "bdt",  "bhd",  "bmd",  "brl",  "cad",  "chf",  "clp",  "cny",  "czk",  "dkk",  "eur",  "gbp",  "hkd",  "huf",  "idr",  "ils",  "inr",  "jpy",  "krw",  "kwd",  "lkr",  "mmk",  "mxn",  "myr",  "nok",  "nzd",  "php",  "pkr",  "pln",  "rub",  "sar",  "sek",  "sgd",  "thb",  "try",  "twd",  "uah",  "vef",  "vnd",  "zar",  "xdr",  "xag",  "xau"]
    try:
        # url = "https://api.coingecko.com/api/v3/simple/price?ids=cardano&vs_currencies="+",".join(currencies)
        # print(url)
        todayprices=cg.get_price(ids='cardano', vs_currencies=",".join(currencies))
        #todayprices=json.loads(urllib.request.urlopen(url).read())
        print(todayprices)
        todayprices['cardano']['ada']=1
        return todayprices
    except:
        return None
