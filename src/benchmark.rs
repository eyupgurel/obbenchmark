use std::time::Instant;

use rdkafka::{consumer::{StreamConsumer, Consumer}, ClientConfig, config::RDKafkaLogLevel, Message};
use serde_derive::{Deserialize, Serialize};


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct BookOrder {
    pub is_buy: bool,

    pub reduce_only: bool,
    // quantity of asset to be traded
    pub quantity: u128,
    // price at which trade is to be made
    pub price: u128,
    // time of the order
    pub timestamp:i64,
    // stop order price
    pub trigger_price: u128,
    // leverage (in whole numbers) for trade
    pub leverage: u128,
    // time after which order becomes invalid
    pub expiration:u128,
    // order hash
    pub hash:String,
    // random number,
    pub salt: u128,
    // address of order maker
    pub maker: String,
    // /// encoded order flags, isBuy, decreasOnly
    pub flags: String,
}

#[tokio::main]
async fn main() {
    let consumer: StreamConsumer   = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "testing-consumer-group")
        .set("enable.auto.commit", "true")
        .set("enable.partition.eof", "false")
        .set("partition.assignment.strategy", "range")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");

    // start consuming the topic
    consumer
        .subscribe(&["test"])
        .expect("Topic subscribe failed");


    println!("Subscribed to provided topic...");
    let mut counter = 0;
    let mut start = Instant::now();
    // loop and wait till receive message on topic
    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(msg) => {
                if counter == 0 {
                    start=Instant::now();
                }

                let owned_message = msg.detach();
                let data_str = std::str::from_utf8(owned_message.payload().expect("Invalid payload data")).expect("Invalid UTF-8");
                let data_string = data_str.to_string();
                let book_order:BookOrder = serde_json::from_str(&data_string).expect("Can't parse");
                if counter % 10000 == 0
                {
                    println!("order hash: {:?}",book_order.hash);
                }
                counter+=1;
                if counter == 100000 {
                    let end = Instant::now();
                    println!("Duration of consumption of 100k messages: {:?}",end-start);
                    counter=0;
                }
                
            }
        }
    };


}