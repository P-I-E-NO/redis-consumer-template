use std::env::var;
use redis::from_redis_value;

use log::{info, error};
use redis::{streams::{StreamReadOptions, StreamReadReply, StreamKey, StreamId}, Commands, Value, FromRedisValue, ErrorKind};

mod log_util;

struct SimpleStreamAutoClaimReply {
    // next item from where to start claiming
    pub next: String,
    // number of claimed items
    pub claimed_items: usize
}

impl FromRedisValue for SimpleStreamAutoClaimReply {
    fn from_redis_value(v: &Value) -> redis::RedisResult<Self> {

        let arr: Vec<Value> = from_redis_value(v)?;
        if arr.len() != 3 {
            return Err((ErrorKind::TypeError, "bad redis response").into())
        }
        
        let next: String = from_redis_value(&arr[0])?; // this should represent the id 
        let dead_items: Vec<Value> = from_redis_value(&arr[1])?; // these are the items in the dead-letter queue
                                                                 // we don't care about their type, since we are claiming them

        Ok(SimpleStreamAutoClaimReply { next, claimed_items: dead_items.len() })
        
    }
}

/*
    this microservice will simply wait for stream events to happen.
    once an event (i.e. the fuel-meter places a new item into the stream) happens, this microservice
    will trigger a routine and will notify the corresponding user
*/
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    pretty_env_logger::init();

    let mut redis_conn = redis::Client::open(var("REDIS_URL").unwrap()).unwrap().get_connection()?;

    let mut alive_stream_keys = ["0-0".to_string()]; // this will be used to process the backlog queue
    let mut dead_stream_key = "0-0".to_string(); // this will be used to process the autoclaiming queue
    let stream_name = var("STREAM_NAME").unwrap();
    let streams = [&stream_name.as_str()];
    let notification_group = var("NOTIFICATION_GROUP").unwrap();
    let consumer_name = var("CONSUMER_NAME").unwrap();
    let item_count = var("ITEM_COUNT").unwrap_or("10".to_string()).parse::<usize>().unwrap();
    let block_time = var("BLOCK_TIME").unwrap_or("10000".to_string()).parse::<usize>().unwrap();
    let dead_key_expiry = var("DEAD_KEY_EXPIRY").unwrap_or("10000".to_string()).parse::<usize>().unwrap();

    loop {

        let xreadgroup_options = StreamReadOptions::default()
            .group(&notification_group, &consumer_name)
            .count(item_count)
            .block(block_time);
        
        let item: StreamReadReply = redis_conn
            .xread_options(&streams, &alive_stream_keys, &xreadgroup_options).expect("error while reading from stream");

        if item.keys.len() == 0 {
            // if keys.len() is 0, that means we timed out, meaning there is no
            // realtime data being pushed on the stream
            // we use this time to check if there are any claimable messages
            info!("timeout reached while waiting for items, searching dead-letter queue");
            match redis::cmd("xautoclaim")
                .arg(&stream_name)
                .arg(&notification_group)
                .arg(&consumer_name)
                .arg(&dead_key_expiry) // key age
                .arg(&dead_stream_key)
                .arg("COUNT")
                .arg(item_count)
                .query::<SimpleStreamAutoClaimReply>(&mut redis_conn) {
                    Ok(v) => {
                        // the xautoclaim command parsed the output correctly
                        // this means we might have claimed some items
                        // if we did, we need to reset the local backlog queue and re-read items from the beginning.
                        // we also need to update our key pointer to match the 
                        if v.claimed_items != 0 {
                            println!("claimed {} items", v.claimed_items);
                            alive_stream_keys[0] = "0-0".to_string(); // we need to reset the backlog queue and reprocess the items 
                        }
                        dead_stream_key = v.next.clone(); // we also update
                    },
                    Err(_) => { error!("error while parsing data from xautoclaim"); },
            };
        }
        for StreamKey { key: _, ids } in item.keys {
            // redis will output queued messages until there are
            // when there are no queued messages at the given index, redis will output something like:
            // 1) 1) "streams:notifications"
            // 2) (empty array)

            // an empty array means that we processed the entirety of the backlog queue, so we caught up
            // any message we didn't process yet.
            // this means we can start listening to "new" messages, we do that by using the ">" index

            if ids.len() == 0 && alive_stream_keys[0] != ">".to_string() {
                // this means we have reached the end of the backlog queue for the given consumer
                info!("no queued items for {consumer_name}, listening to realtime data");
                alive_stream_keys[0] = ">".to_string();
            }
            for StreamId { id, map } in ids {
               
                for (n, s) in map {
                    if let Value::Data(bytes) = s {
                        if alive_stream_keys[0] != ">" {
                            info!("processing queued item {i}", i = &id);
                        }
                        println!("\t\t{}: {}", n, String::from_utf8(bytes).expect("utf8"));
                        // process item
                            redis_conn.xack(&streams, &notification_group, &[id.clone()])?;
                        // 
                        alive_stream_keys[0] = id.clone();
                    } else {
                        println!("Weird data")
                    }
                }
            }
        }

    }

}


