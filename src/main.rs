mod util;
use std::{env::var, time::Duration};

use redis_consumer::{Consumer, ConsumerOptions, ConsumerClaimOptions};
use log::{info, error};
use tokio::time::sleep;

/*
    this microservice will simply wait for stream events to happen.
    once an event (i.e. the fuel-meter places a new item into the stream) happens, this microservice
    will trigger a routine and will notify the corresponding user
*/
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    pretty_env_logger::init();

    let redis_url = var("REDIS_URL").unwrap(); 
    let mut alive_stream_keys = vec!("0-0".to_string()); // this will be used to process the backlog queue
    let dead_stream_key = "0-0".to_string(); // this will be used to process the autoclaiming queue
    let stream_name = var("STREAM_NAME").unwrap();
    let streams = vec!(stream_name.clone());
    let notification_group = var("NOTIFICATION_GROUP").unwrap();
    let consumer_name = var("CONSUMER_NAME").unwrap();
    let item_count = var("ITEM_COUNT").unwrap_or("10".to_string()).parse::<usize>().unwrap();
    let block_time = var("BLOCK_TIME").unwrap_or("10000".to_string()).parse::<usize>().unwrap();
    let dead_key_expiry = var("DEAD_KEY_EXPIRY").unwrap_or("10000".to_string()).parse::<usize>().unwrap();

    let mut consumer = Consumer::new(
        redis_url, 
        consumer_name,
        &streams, 
        notification_group,
        &mut alive_stream_keys,
        dead_stream_key,
    ).await?;

    loop {

        let xreadgroup_options = ConsumerOptions::new(item_count, block_time);
        match consumer.simple_read_group(&xreadgroup_options)
            .await {
                Ok(item) => { 
                    if item.keys.len() == 0 {
                        // if keys.len() is 0, that means we timed out, meaning there is no
                        // realtime data being pushed on the stream
                        // we use this time to check if there are any claimable messages
                        info!("timeout reached while waiting for items, searching dead-letter queue");
                        let xautoclaim_options = ConsumerClaimOptions::new(item_count, dead_key_expiry);
                        match consumer.autoclaim(&stream_name, &xautoclaim_options).await {
                                Ok(v) => {
                                    // the xautoclaim command parsed the output correctly
                                    // this means we might have claimed some items
                                    // if we did, we need to reset the local backlog queue and re-read items from the beginning.
                                    // we also need to update our key pointer to match the latest available key
                                    if v.claimed_items != 0 {
                                        println!("claimed {} items", v.claimed_items);
                                    }
                                },
                                Err(_) => { error!("error while parsing data from xautoclaim"); },
                        };
                    }
                
                    consumer.process_group(item, |stream_item| {
                        Box::pin(async move {
                            dbg!(stream_item);
                        })
                    }).await;
                    
                } 
                Err(e) => {
                    error!("cannot read group, retrying in 1000ms: {e}");
                    sleep(Duration::from_secs(1)).await;
                }
            }

    }

}


