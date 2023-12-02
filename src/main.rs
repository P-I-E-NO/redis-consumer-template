mod util;
use std::{env::var, time::{Duration, Instant}, sync::Arc, thread};

use redis::{streams::{StreamId, StreamKey}, Value};
use redis_consumer::{Consumer, ConsumerOptions, ConsumerClaimOptions};
use log::{info, error};
use teloxide::requests::Requester;
use tokio::{time::{sleep, sleep_until}};
use std::sync::Mutex;

/*
    this microservice will simply wait for stream events to happen.
    once an event (i.e. the fuel-meter places a new item into the stream) happens, this microservice
    will trigger a routine and will notify the corresponding user
*/
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    pretty_env_logger::init();

    let bot = Arc::new(Mutex::new(teloxide::Bot::from_env()));
    let redis_url = var("REDIS_URL").unwrap(); 
    let alive_stream_keys = vec!("0-0".to_string()); // this will be used to process the backlog queue
    let dead_stream_key = "0-0".to_string(); // this will be used to process the autoclaiming queue
    let stream_name = var("STREAM_NAME").unwrap();
    let streams = vec!(stream_name.clone());
    let notification_group = var("NOTIFICATION_GROUP").unwrap();
    let consumer_name = var("CONSUMER_NAME").unwrap();
    let item_count = var("ITEM_COUNT").unwrap_or("10".to_string()).parse::<usize>().unwrap();
    let block_time = var("BLOCK_TIME").unwrap_or("10000".to_string()).parse::<usize>().unwrap();
    let dead_key_expiry = var("DEAD_KEY_EXPIRY").unwrap_or("10000".to_string()).parse::<usize>().unwrap();

    let consumer = Arc::new(Mutex::new(
        Consumer::new(
            redis_url, 
            consumer_name,
            streams, 
            notification_group,
            alive_stream_keys,
            dead_stream_key,
    ).await?));

    loop {

        let mut schedule = false;
        {
            let mut mutex = consumer.lock().unwrap();
            let xreadgroup_options = ConsumerOptions::new(item_count, block_time);
            let items = mutex.simple_read_group(&xreadgroup_options).await.expect("cannot read from group");
            if items.keys.len() == 0 {
                // if keys.len() is 0, that means we timed out, meaning there is no
                // realtime data being pushed on the stream
                // we use this instant to check if there are any claimable messages
                println!("timeout reached while waiting for items, searching dead-letter queue");
                let xautoclaim_options = ConsumerClaimOptions::new(item_count, dead_key_expiry);
                match mutex.autoclaim(&stream_name, &xautoclaim_options).await {
                        Ok(v) => {
                            // the xautoclaim command parsed the output correctly
                            // this means we might have claimed some items
                            // if we did, we need to reset the local backlog queue and re-read items from the beginning.
                            // we also need to update our key pointer to match the latest available key
                            println!("{}", v.next);
                            if v.claimed_items != 0 {
                                println!("claimed {} items", v.claimed_items);
                            }else{
                                // println!("no items claimed");
                            }
                        },
                        Err(_) => { error!("error while parsing data from xautoclaim"); },
                };
            }else{
                schedule = true;
            }
        }

        /*match consumer.simple_read_group(&xreadgroup_options)
            .await {
                Ok(item) => { 
                    if item.keys.len() == 0 {
                        // if keys.len() is 0, that means we timed out, meaning there is no
                        // realtime data being pushed on the stream
                        // we use this instant to check if there are any claimable messages
                        println!("timeout reached while waiting for items, searching dead-letter queue");
                        let xautoclaim_options = ConsumerClaimOptions::new(item_count, dead_key_expiry);
                        match consumer.autoclaim(&stream_name, &xautoclaim_options).await {
                                Ok(v) => {
                                    // the xautoclaim command parsed the output correctly
                                    // this means we might have claimed some items
                                    // if we did, we need to reset the local backlog queue and re-read items from the beginning.
                                    // we also need to update our key pointer to match the latest available key
                                    println!("{}", v.next);
                                    if v.claimed_items != 0 {
                                        println!("claimed {} items", v.claimed_items);
                                    }else{
                                        // println!("no items claimed");
                                    }
                                },
                                Err(_) => { error!("error while parsing data from xautoclaim"); },
                        };
                    }
                    
                    consumer.process_group(item, |stream_id| {
                        let bot = bot.clone();
                        let c = async_consumer.clone();
                        Box::pin(async move {
                            process_item(bot, stream_id, c).await;
                        })
                    });
                    
                } 
                Err(e) => {
                    error!("cannot read group, retrying in 1000ms: {e}");
                    sleep(Duration::from_secs(1)).await;
                }
            }*/

    }

}

/*async fn process_item(
    bot: Arc<Mutex<teloxide::Bot>>, 
    item: StreamId,
    consumer: Arc<Mutex<Consumer>>
) {
    // thread::sleep(Duration::from_secs(2));
    for (_, v) in item.map {
        if let Value::Data(bytes) = v {
            let user_id = String::from_utf8(bytes).expect("utf8");
            {
                println!("processing {user_id}");
                {
                    let mut c = consumer.lock().await;
                    c.ack(&vec!(item.id.clone())).await.expect("cannot ack");
                }
                println!("acked {user_id}");
                /*let bot = bot.lock().await;
                match bot.send_message("-1002078353837".to_string(), format!("(ack) user_id: `{user_id}`")).await {
                    Ok(_) => {
                        
                    },
                    Err(e) => error!("couldn't send telegram message: {e}")
                }*/
            }
            
        } else {
            error!("bad redis data")
        }
    }
    
}*/