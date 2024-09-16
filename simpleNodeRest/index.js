var express = require('express');
var cors = require('cors')
var bodyParser = require('body-parser');
var app = express();
const dotenv = require('dotenv');
dotenv.config();
var blake2b = require('blake2b')
app.use(cors())
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.set('port', process.env.PORT || 3002);


const { Client } = require('pg');
// Database configuration
const dbConfig = {
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'postgres',
    database: 'pooltool'
  }

let db;

// Function to connect to the database
async function connectToDB() {
  try {
    db = new Client(dbConfig);
    await db.connect();
    console.log('Connected to the database');
  } catch (err) {
    console.error('Initial connection error:', err.message);
    process.exit(1); // Exit if unable to connect initially
  }

  // Listen for any errors and handle reconnection
  db.on('error', async (err) => {
    console.error('Database error:', err.message);
    await reconnectDB(); // Try to reconnect on error
  });
}

// Function to attempt reconnection
async function reconnectDB() {
  let retries = 5;
  while (retries) {
    try {
      console.log('Attempting to reconnect...');
      db = new Client(dbConfig); // Reinitialize client
      await db.connect();
      console.log('Reconnected to the database');
      return;
    } catch (err) {
      retries -= 1;
      console.error(`Reconnection failed. Retries left: ${retries}`);
      if (retries === 0) {
        console.error('Max reconnection attempts reached. Exiting...');
        process.exit(1);
      }
      await new Promise(res => setTimeout(res, 5000)); // Wait 5 seconds before retrying
    }
  }
}


const firebase = require('firebase-admin');
const serviceAccount = require('../firebase-account.json');

firebase.initializeApp({credential: firebase.credential.cert(serviceAccount), 
    databaseURL: 'https://pegasus-pool.firebaseio.com'
});

var api_user_table = {}
var api_pool_table = {}
var pool_epoch_blocks = {}
var connections = {}

console.log("starting: ")
// Query function
async function queryDB(query) {
  try {
    const res = await db.query(query); // Perform the query
    return res;
  } catch (err) {
    console.error('Query error:', err.message, err.stack);
    throw err;
  }
}

function returnsuccess(message) {
    return {
        statusCode: 200,
        headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},

        body: JSON.stringify({success: true, message}),
    }
}
function returnerror(message) {
    return {
        statusCode: 200,
        headers: {
        "Access-Control-Allow-Origin": "*"
        },
        body: JSON.stringify({success: false, message}),
    }
}

function verifyvaliduuid(uuid) {
  var uuidregex = new RegExp("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$");
  return uuidregex.test(uuid);
}
async function  validatePoolAccess(api_key,pool_id) {
  if (typeof api_key =="undefined"||typeof pool_id=="undefined") {
    return false
  }
  if(!verifyvaliduuid(api_key)) {
    return false
  }
  if (typeof(api_user_table[api_key])=="undefined") {
    var query = `
    SELECT user_id from api_user_table where api_key='${api_key}';
    `
    console.log(query)
    const usid = await queryDB(query);
    if(usid.rows.length) {
      api_user_table[api_key]=usid.rows[0].user_id
      console.log("found user from api key")
    }else{
      console.log("cannot find user")
      //cannot find user
      return false
    }
  }
  if (typeof(api_pool_table[api_key])=="undefined"||!api_pool_table[api_key].includes(pool_id)) {
    var query = `
    SELECT pool_id from api_pool_table where user_id='${api_user_table[api_key]}';
    `
    console.log(query)
    const poid = await queryDB(query);
    if(poid.rows.length) {
      api_pool_table[api_key]=poid.rows.map(x => x.pool_id)
      console.log("found pool_id from from api_key")
    }else{
      console.log("did not find any pool ids for this user")
      return false
    }
  }
  return api_pool_table[api_key].includes(pool_id)
}

app.post('/sendslots', async function(req, res) {
  var request = req.query
  if(typeof req.query.apiKey == "undefined") {
    if(typeof req.body.apiKey !== "undefined") {
      request = req.body
    }
  }
  console.log(request.poolId)
  //(typeof request.poolId != "undefined" && request.poolId=='8dcdf33740ee8e9da6e36337d875fb9222f5c8a1a315fda36886c615') || 
  if((typeof request.poolId != "undefined" && request.poolId=='2375efb30fae44b')) {
    //timing looks good
  }else{
    var curslot = (parseInt(Date.now()/1000) - 1607723091)%432000
    if(!(curslot > 0 && curslot < 60*60*24)) {
      res.json({
        statusCode: 400,
        headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        body: JSON.stringify({success: false, message:"You must send your slots in during the first 24 hours of the epoch.  Please try again next epoch."}),
      })
      return
    }
  }

  if(await validatePoolAccess(request.apiKey,request.poolId)) {
    console.log('/sendslots')
    if(typeof request.poolId == "undefined" || typeof request.apiKey == "undefined" || typeof request.epoch == "undefined" ) {
      console.log("400: missing params");
      res.json({
        statusCode: 400,
        headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        body: JSON.stringify({success: false, message:"Missing poolId, apiKey or epoch"}),
      })
      return
    }

    if(!/^[a-fA-F0-9]{56}$/.test(request.poolId)) {
      console.log("400: bad poolid");
      res.json({
        statusCode: 400,
        headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        body: JSON.stringify({success: false, message:"PoolId should be a 56 character hexadecimal string"}),
      })
      return
    }

    if(!/^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$/.test(request.apiKey)) {
      console.log("400: bad apikey");
      res.json({
        statusCode: 400,
        headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        body: JSON.stringify({success: false, message:"apiKey should be a uuid hexadecimal string"}),
      })
      return
    }

    if(!/^[0-9]+$/.test(request.epoch)) {
      console.log("400: bad epoch");
      res.json({
        statusCode: 400,
        headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        body: JSON.stringify({success: false, message:"epoch should be an integer"}),
      })
      return
    }
    var hashfeedback = ''
    var query = ''

    if(typeof request.slotQty != "undefined" && typeof request.hash != "undefined") {
      //this means we are loading in slots for epoch
      if(!/^[0-9]+$/.test(request.slotQty)) {
        console.log("400: bad slotQty");
        res.json({
          statusCode: 400,
          headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
          body: JSON.stringify({success: false, message:"slotQty should be an integer"}),
        })
        return
      }
      if(!/^[a-fA-F0-9]{64}$/.test(request.hash)) {
        console.log("400: bad hash");
        res.json({
          statusCode: 400,
          headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
          body: JSON.stringify({success: false, message:"hash should be a 64 character hexadecimal string"}),
        })
        return
      }
      //make sure we have not loaded slots yet
      console.log("Mainnet/pool_stats/"+request.poolId+"/assigned_slots/"+request.epoch+"/hash")
      var assigned_slots_hash
      await firebase.database().ref("Mainnet/pool_stats/"+request.poolId+"/assigned_slots/"+request.epoch+"/hash").get().then(function(snapshot) {
        if (snapshot.exists()) {
            console.log(snapshot.val());
            assigned_slots_hash=snapshot.val()
          } else {
            console.log("No data available");
            assigned_slots_hash=null
          }
      })

      if(assigned_slots_hash!=null) {
          //skipping...  you can only load slots once for a specific epoch
          hashfeedback = `You can only load slots and a new hash once in an epoch.  You previously loaded hash ${assigned_slots_hash}.  We have updated your expected slots on the main page, but have not modified your epoch slots history used to calculate performance.`
      } else {
          console.log("updating pool stats")
          query =`insert into pool_assigned_slots (pool_id,epoch,hash, verified, slots) values('${request.poolId}',${request.epoch},'${request.hash}',false,${request.slotQty}) ON CONFLICT ON CONSTRAINT pool_assigned_slots_pkey DO UPDATE set slots=${request.slotQty}, verified=false, hash='${request.hash}';`
          await queryDB(query);
          await firebase.database().ref("Mainnet/pool_stats/"+request.poolId+"/assigned_slots/"+request.epoch).update({"slots":request.slotQty,"hash":request.hash})
          
          hashfeedback=`We have updated your assigned slots for epoch ${request.epoch} to be ${request.slotQty} with a hash of ${request.hash}.  You must provide an array of slots that matches this hash to have your performance counted.`
      }
      console.log("updating stake pools")
      await firebase.database().ref("Mainnet/stake_pools/"+request.poolId).update({"z":request.slotQty,"ez":request.epoch})
      query = `update pools set assigned_slots=${request.slotQty}, assigned_slots_epoch=${request.epoch} where \"pool_id\"='${request.poolId}';`
      poid = await queryDB(query);
    }
    var js = null
    var verifyfeedback = ''
    if(typeof request.prevSlots != "undefined") {
      //this means we are validating slots for epoch-1
      try {
        js = JSON.parse(request.prevSlots);
      } catch (e) {
        js=null
        console.log("400: cannot decode");
        res.json({
          statusCode: 400,
          headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
          body: JSON.stringify({success: false, message:"prevSlots could not be decoded.  It should be a stringified json encoded array of slot numbers.  Note no slots should be the string [].  " + hashfeedback}),
        })
        return
      }

      console.log("verify slots")
      var output = new Uint8Array(32)
      var input = Buffer.from(request.prevSlots)
      var hash = blake2b(output.length).update(input).digest('hex')
      await firebase.database().ref("Mainnet/pool_stats/"+request.poolId+"/assigned_slots/"+(request.epoch-1)).get().then(function(snapshot) {
        if (snapshot.exists()) {
            snap=snapshot.val()
            console.log(snapshot.val());
            assigned_slots_hash=snap['hash']
            assigned_slots=snap['slots']
          }
          else {
            console.log("No data available");
            assigned_slots_hash=null
            assigned_slots=null
          }
      })
      if(assigned_slots_hash==null||assigned_slots==null) {
        console.log("400: no slots");
        res.json({
          statusCode: 400,
          headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
          body: JSON.stringify({success: false, message:`We don't have any slot counts or hashes saved for epoch ${request.epoch - 1}.  Did you send them in previously? ` + hashfeedback}),
        })
        return
      }else{
        if(js.length==assigned_slots&&(assigned_slots_hash==hash||assigned_slots_hash=='dahash')) {
          //slots verified
          await firebase.database().ref("Mainnet/pool_stats/"+request.poolId+"/assigned_slots/"+(request.epoch-1)).update({"verified":true,"json":js})
          query =`update pool_assigned_slots set verified=true, jsondata='${request.prevSlots}' where \"pool_id\"='${request.poolId}' and epoch=${request.epoch - 1};`
          await queryDB(query);
          verifyfeedback = `Slots validated for epoch ${request.epoch - 1}.  Assigned Performance will be calculated around 1 hour  into the epoch.`
        }else{
          console.log("js.length==assigned_slots",js.length==assigned_slots)
          console.log("assigned_slots_hash==hash",assigned_slots_hash==hash)
          console.log("assigned_slots_hash==dahash",assigned_slots_hash=='dahash')
          console.log(js.length,assigned_slots,assigned_slots_hash,hash)
          console.log("400: bad slots");
          res.json({
            statusCode: 400,
            headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            body: JSON.stringify({success: false, message:`We could not match either the number of slots in your prevSlots array or the hash for epoch ${request.epoch - 1}.  Expected: ${assigned_slots} slots with hash ${assigned_slots_hash} but got ${js.length} slots with hash ${hash}.  ` + hashfeedback}),
          })
          return
        }
      }
    }
    console.log("200: ok");
    res.json({
      statusCode: 200,
      headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
      body: JSON.stringify({success: true, message:verifyfeedback + hashfeedback}),
    })
    return


  }else{
      console.log("401: need api key");
      res.json({
        statusCode: 401,
        headers: {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        body: JSON.stringify({success: false, body: null, message:"Unauthorized.  You must provide a valid API key"}),
      })
    }
});

app.listen(3002,async () => {
  await connectToDB(); // Ensure DB connection is established at server start
  console.log(`Server is running on port 3002`);
});


