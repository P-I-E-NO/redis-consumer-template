use redis::{FromRedisValue, from_redis_value, Value, ErrorKind};

pub struct SimpleStreamAutoClaimReply {
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