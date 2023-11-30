use futures::future::BoxFuture;
use log::info;
use redis::{RedisError, streams::{StreamReadReply, StreamReadOptions, StreamKey, StreamId}, AsyncCommands, FromRedisValue, Value};
use crate::util::types::simple_stream_auto_claim_reply::SimpleStreamAutoClaimReply;

type Job = BoxFuture<'static, ()>;
pub struct Consumer<'a> {
    redis_conn: redis::aio::Connection,
    streams: &'a Vec<String>,
    notification_group: String,
    consumer_name: String,
    main_indexes: &'a mut Vec<String>, // which indexes on the stream we want to get
    autoclaim_index: String,
}

// "active" stream monitoring: the main stream the consumer monitors
pub struct ConsumerOptions {
    // alive_stream_indexes: Vec<String>,
    item_count: usize, // how many items from the index
    block_time: usize,
}

pub struct ConsumerClaimOptions {
    key_expiry: usize,
    item_count: usize,
}

impl<'a> Consumer<'a> {
    pub async fn new(
        redis_url: String,
        consumer_name: String,
        streams: &'a Vec<String>,
        notification_group: String,
        main_indexes: &'a mut Vec<String>,
        autoclaim_index: String,
    ) -> Result<Consumer<'a>, RedisError> {

        let conn = redis::Client::open(redis_url)?.get_async_connection().await?;
        Ok(Consumer { 
            redis_conn: conn, 
            streams, 
            notification_group, 
            consumer_name,
            main_indexes,
            autoclaim_index
        })
        
    }

    pub async fn ack(
        &mut self,
        ids: &Vec<String>,
    ) -> Result<(), RedisError> {
        self.redis_conn.xack(&self.streams, &self.notification_group, ids).await?;
        Ok(())
    }

    pub async fn autoclaim(
        &mut self, 
        on: &str, 
        with: &ConsumerClaimOptions
    ) -> Result<SimpleStreamAutoClaimReply, RedisError> 
    {
            match redis::cmd("xautoclaim")
                .arg(on)
                .arg(&self.notification_group)
                .arg(&self.consumer_name)
                .arg(with.key_expiry) // key age
                .arg(&self.autoclaim_index)
                .arg("COUNT")
                .arg(with.item_count)
                .query_async::<_, SimpleStreamAutoClaimReply>(&mut self.redis_conn).await {
                    Ok(v) => {
                        // the xautoclaim command parsed the output correctly
                        // this means we might have claimed some items
                        // if we did, we need to reset the local backlog queue and re-read items from the beginning.
                        // we also need to update our key pointer to match the latest available key
                        if v.claimed_items != 0 {
                            println!("claimed {} items", v.claimed_items);
                            self.main_indexes[0] = "0-0".to_string(); // we need to reset the backlog queue and reprocess the items 
                        }
                        self.autoclaim_index = v.next.clone(); // we also update
                        Ok(v)
                    },
                    Err(e) => Err(e)
                }
            
    }

    // this performs a strongly typed xreadgroup, the passed type must implement FromRedisValue
    pub async fn read_group<T>(
        &mut self, 
        with: ConsumerOptions
    ) -> Result<T, RedisError> 
    where T: FromRedisValue
    {
        let xreadgroup_options = StreamReadOptions::default()
            .group(&self.notification_group, &self.consumer_name)
            .count(with.item_count)
            .block(with.block_time); 

        let reply = self.redis_conn.xread_options::<_, _, T>(
            &self.streams, 
            & *self.main_indexes, 
            &xreadgroup_options
        ).await?;

        Ok(reply)
    }
    // this performs a generic xreadgroup, not strongly typed
    pub async fn simple_read_group(
        &mut self, 
        with: &ConsumerOptions
    ) -> Result<StreamReadReply, RedisError> 
    {
        let xreadgroup_options = StreamReadOptions::default()
            .group(&self.notification_group, &self.consumer_name)
            .count(with.item_count)
            .block(with.block_time); 

        let reply: StreamReadReply = self.redis_conn.xread_options(
            &self.streams, 
            &* self.main_indexes, 
            &xreadgroup_options
        ).await?;        

        Ok(reply)
    }
    pub async fn simple_process_group(
        &mut self,
        items: StreamReadReply,
        mut job: impl FnMut() -> Job
    ){
        for StreamKey { key: _, ids } in items.keys {

            // redis will output queued messages until there are
            // when there are no queued messages at the given index, redis will output something like:
            // 1) 1) "streams:notifications"
            // 2) (empty array)

            // an empty array means that we processed the entirety of the backlog queue, so we caught up
            // any message we didn't process yet.
            // this means we can start listening to "new" messages, we do that by using the ">" index

            if ids.len() == 0 && self.main_indexes[0] != ">".to_string() {
                // this means we have reached the end of the backlog queue for the given consumer
                info!("no queued items for {name}, listening to realtime data", name = self.consumer_name);
                self.main_indexes[0] = ">".to_string();
            }
            for StreamId { id, map } in ids {
               
                for (n, s) in map {
                    if let Value::Data(bytes) = s {
                        if self.main_indexes[0] != ">" {
                            info!("processing queued item {i}", i = id);
                        }
                        // println!("\t\t{}: {}", n, String::from_utf8(bytes).expect("utf8"));
                        // process item
                        tokio::spawn(job());
                        // 
                        self.main_indexes[0] = id.clone();
                        self.ack(&vec!(id.clone())).await.expect("bad!!!");
                    } else {
                        println!("Weird data")
                    }
                }

            }
        }
    }
}

impl ConsumerClaimOptions {
    pub fn new(key_expiry: usize, item_count: usize) -> Self {
        ConsumerClaimOptions { key_expiry, item_count }
    }
}

impl ConsumerOptions {
    pub fn new(item_count: usize, block_time: usize) -> Self {
        // default values
        ConsumerOptions { item_count, block_time }
    }
}