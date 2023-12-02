use std::{env::var, time::Duration};
mod simple_stream_autoclaim_reply;

use deadpool_redis::{Config, Runtime};
use log::{info, error};
use redis::{streams::{StreamReadOptions, StreamReadReply, StreamKey}, AsyncCommands, Value, RedisError, RedisResult};

use crate::simple_stream_autoclaim_reply::SimpleStreamAutoClaimReply;


/*
    this is a template for a microservice that will simply wait for stream events to happen.
    once an event (i.e. a new item is placed on the stream) happens, this microservice
    will trigger a routine and will notify the corresponding user
*/
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    pretty_env_logger::init();

    let cfg = Config::from_url(var("REDIS_URL").unwrap());
    let redis_pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap(); 
    let mut service_connection = redis_pool.get().await.unwrap();

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
        
        if let Ok(reply) = service_connection
            .xread_options(&streams, &alive_stream_keys, &xreadgroup_options).await as Result<StreamReadReply, RedisError> {
                if reply.keys.len() == 0 {
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
                        .query_async::<_, SimpleStreamAutoClaimReply>(&mut service_connection)
                        .await {
                            Ok(v) => {
                                // the xautoclaim command parsed the output correctly
                                // this means we might have claimed some items
                                // if we did, we need to reset the local backlog queue and re-read items from the beginning.
                                // we also need to update our key pointer to match the 
                                if v.claimed_items != 0 {
                                    info!("claimed {} items", v.claimed_items);
                                    alive_stream_keys[0] = "0-0".to_string(); // we need to reset the backlog queue and reprocess the items 
                                }
                                dead_stream_key = v.next.clone(); // we also update
                            },
                            Err(_) => { error!("error while parsing data from xautoclaim"); },
                    };
                }
        
                for StreamKey { key: _, ids } in reply.keys {
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
                    
                    for stream_id in ids {

                        let cloned = stream_id.clone();
                        let group = notification_group.clone();
                        let cloned_stream_name = stream_name.clone();

                        if let Ok(mut conn) = redis_pool.get().await {
                            alive_stream_keys[0] = stream_id.id;

                            tokio::spawn(async move {
                                
                                info!("processing {}", cloned.id);
                                tokio::time::sleep(Duration::from_secs(2)).await;

                                if let Ok(_) = conn.xack(&[cloned_stream_name], group, &[cloned.id.to_string()]).await as RedisResult<Value> {

                                    info!("acked message {}", cloned.id);

                                }else{
                                    error!("cannot ack message {}", cloned.id);
                                }
                                                                
                            });

                        }else{
                            error!("cannot reserve client connection")
                        }        
                        
                    }
        
                }
            }else{
                error!("cannot retrieve redis group! retrying...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }

    }

}