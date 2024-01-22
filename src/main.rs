mod sockets;
mod models;
mod env;

extern crate dotenv;


use std::{iter};
use std::collections::{BTreeMap, HashMap};

use std::time::{Duration, Instant};
use rand::distributions::Alphanumeric;
use rand::Rng;
use chrono::{Utc};
use crate::env::EnvVars;
use crate::sockets::common::{DepthUpdateStream, OrderBookStream};
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde_derive::{Deserialize, Serialize};


fn generate_random_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(len)
        .collect()
}

fn get_book_orders_ordered_by_price_timestamp<'a>(
    order_book: &'a BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,
) -> Vec<&'a BookOrder> {
    let mut orders: Vec<&'a BookOrder> = Vec::new();

    for (_, price_map) in order_book.iter() {
        for (_, timestamp_map) in price_map.iter() {
            for (_, order) in timestamp_map.iter() {
                orders.push(order);
            }
        }
    }
    orders
}


#[derive(Debug)]
pub struct Match {
    pub price: u128,
    pub quantity: u128,
}

impl Match {
    fn new() -> Match {
        Match { price: 0, quantity: 0 }
    }

    fn add(&mut self, price: u128, quantity: u128) {
        self.price = price; // Depending on your requirement, you might want to calculate the average price etc.
        self.quantity += quantity;
    }
}

fn match_and_process_orders(
    order_book: &mut BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,
    price_time_map: &mut HashMap<String,(u128,i64)>,
    mut quantity: u128
) -> Option<Match> {
    let mut orders_to_delete = Vec::new();
    let mut orders_to_update = Vec::new();
    let mut matched = Match::new();

    'outer: for (&price, price_map) in order_book.iter() {
        for (&timestamp, timestamp_map) in price_map.iter() {
            for (hash, order) in timestamp_map.iter() {
                if quantity == 0 {
                    break 'outer;
                }

                let matched_quantity = std::cmp::min(order.quantity, quantity);
                quantity -= matched_quantity;
                matched.add(price, matched_quantity);

                if order.quantity <= matched_quantity {
                    orders_to_delete.push((price, timestamp, hash.clone()));
                } else {
                    let updated_quantity = order.quantity - matched_quantity;
                    orders_to_update.push((price, timestamp, hash.clone(), updated_quantity));
                }
            }
        }
    }

    for (price, timestamp, hash) in orders_to_delete {
        delete_order(order_book, price_time_map, &hash).expect("TODO: panic message");
    }

    for (price, timestamp, hash, updated_quantity) in orders_to_update {
        update_order_quantity(order_book, price, timestamp, &hash, updated_quantity);
    }

    Some(matched)
}



fn generate_and_process_random_order( order_book: &mut BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,
                                      price_time_map: &mut HashMap<String,(u128,i64)>) -> Option<Match> {
    // Generate a random order
    let random_order = generate_rand_order();

    // Extract the quantity from the generated order
    let quantity = random_order.quantity;

    // Call match_and_process_orders with the generated quantity

    let start_time = Instant::now();

    let r = match_and_process_orders(order_book, price_time_map, quantity);

    let end_time = Instant::now();
    let duration = end_time - start_time;
    println!("match_and_process_orders duration: {:?} match speed: {:?}", duration, 1000000 / duration.as_micros());
    r
}

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

fn generate_rand_order() -> BookOrder {
    let mut rng = rand::thread_rng();

    // Calculate new range for price
    let min_price = 41_933_700_000_000_000 / 100_000_000_000;
    let max_price = 43_537_800_000_000_000 / 100_000_000_000;
    let price_step = (max_price - min_price) / 99;
    let price = (min_price + price_step * rng.gen_range(0..=99)) * 100_000_000_000;

    // Calculate new range for quantity
    let min_quantity = 187_000_000_000 / 1_000_000_000;
    let max_quantity = 617_000_000_000 / 1_000_000_000;
    let quantity_step = (max_quantity - min_quantity) / 99;
    let quantity = (min_quantity + quantity_step * rng.gen_range(0..=99)) * 1_000_000_000;


    BookOrder {
        is_buy: rng.gen_bool(0.5),
        reduce_only: rng.gen_bool(0.5),
        quantity,
        price,
        timestamp: Utc::now().timestamp(),
        trigger_price: rng.gen_range(1..=10_000),
        leverage: rng.gen_range(1..=10),
        expiration: rng.gen_range(1..=10_000),
        hash: generate_random_string(30), // Generate a unique hash for the order
        salt: rng.gen::<u128>(),
        maker: rng.gen::<u128>().to_string(),
        flags: rng.gen::<u128>().to_string(),
    }
}


fn generate_rand_orders(size: usize) -> Vec<BookOrder> {
    let mut orders = Vec::with_capacity(size);

    for _ in 0..size {
        let order = generate_rand_order();
        orders.push(order);
    }

    orders
}


use std::result::Result;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
struct OrderDeletionError;

impl fmt::Display for OrderDeletionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Order deletion failed")
    }
}

impl Error for OrderDeletionError {}

fn delete_order(
    order_book: &mut BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,
    price_time_map: &mut HashMap<String, (u128, i64)>,
    order_hash: &str,
) -> Result<BookOrder, OrderDeletionError> {
    if let Some(&(price, timestamp)) = price_time_map.get(order_hash) {
        if let Some(price_map) = order_book.get_mut(&price) {
            if let Some(timestamp_map) = price_map.get_mut(&timestamp) {
                if let Some(removed_order) = timestamp_map.remove(order_hash) {
                    if timestamp_map.is_empty() {
                        price_map.remove(&timestamp);
                    }
                    if price_map.is_empty() {
                        order_book.remove(&price);
                    }
                    price_time_map.remove(order_hash);
                    return Ok(removed_order);
                }
            }
        }
    }
    Err(OrderDeletionError)
}


 fn update_order(
    order_book: &mut BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,
    price: u128,
    timestamp: i64,
    hash: &str,
    new_order_data: BookOrder
) {
    if let Some(price_map) = order_book.get_mut(&price) {
        if let Some(timestamp_map) = price_map.get_mut(&timestamp) {
            if let Some(order) = timestamp_map.get_mut(hash) {
                *order = new_order_data;
            }
        }
    }
}

fn update_order_quantity(
    order_book: &mut BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,
    price: u128,
    timestamp: i64,
    hash: &str,
    new_quantity: u128,
) {
    if let Some(price_map) = order_book.get_mut(&price) {
        if let Some(timestamp_map) = price_map.get_mut(&timestamp) {
            if let Some(order) = timestamp_map.get_mut(hash) {
                order.quantity = new_quantity;
            }
        }
    }
}

fn upsert_order(
    order_book: &mut BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,
    price_time_map: &mut HashMap<String,(u128,i64)>,
    new_order: BookOrder
) {
    let price_map = order_book.entry(new_order.price).or_insert_with(BTreeMap::new);
    let timestamp_map = price_map.entry(new_order.timestamp).or_insert_with(HashMap::new);

    let order_hash = new_order.hash.to_owned();
    let price = new_order.price;
    let timestamp = new_order.timestamp;

    // Upsert the order: update if exists, insert if not
    timestamp_map.insert(new_order.hash.clone(), new_order);

    price_time_map.insert(order_hash,(price, timestamp));
}

fn count_orders_in_book(order_book: &BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>) -> usize {
    order_book.values()
        .flat_map(|price_map| price_map.values())
        .map(|timestamp_map| timestamp_map.len())
        .sum()
}

#[tokio::main]
async fn main() {

    let mut order_book: BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>> = BTreeMap::new();
    let mut price_time_map: HashMap<String,(u128,i64)> = HashMap::new();

    let vars: EnvVars = env::env_variables();
    let _guard = env::init_logger(vars.log_level);

    let vars: EnvVars = env::env_variables();

    println!("Brokers: {}", vars.brokers);
    println!("Consumption Topic: {}", vars.event_topic);

    let consumer: StreamConsumer   = ClientConfig::new()
        .set("bootstrap.servers", vars.brokers)
        .set("group.id", "testing-consumer-group")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("partition.assignment.strategy", "range")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");

    // start consuming the topic
    consumer
        .subscribe(&[vars.event_topic.as_str()])
        .expect("Topic subscribe failed");


    println!("Subscribed to provided topic...");
    // loop and wait till receive message on topic
    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(msg) => {

                let owned_message = msg.detach();
                let data_str = std::str::from_utf8(owned_message.payload().expect("Invalid payload data")).expect("Invalid UTF-8");
                let data_string = data_str.to_string();
                let book_order: BookOrder = serde_json::from_str(&data_string).expect("Can't parse");


                let start = Instant::now();

                if(book_order.quantity > 0) {
                    // Upsert the order in the in-memory order book
                    upsert_order(&mut order_book, &mut price_time_map, book_order);

                } else if (book_order.quantity == 0) {
                    delete_order(&mut order_book, &mut price_time_map, &book_order.hash)
                        .map(|_removed_order| {
                            println!("order removed");// Handle successful deletion. This block can be empty if there's nothing specific to do.
                        })
                        .unwrap_or_else(|err| {
                            // Handle the error case without panicking
                            eprintln!("Error deleting order: {}", err);
                        });

                }

                let update_duration = Instant::now() - start;

                println!("OB update duration: {:?} OB update speed: {:?}", update_duration,
                    if update_duration.as_micros() > 0 {
                        1000000 / update_duration.as_micros()
                    } else {
                        u128::MAX
                    });

                let order_count = count_orders_in_book(&order_book);
                tracing::info!("Total number of orders: {}", order_count);
                if(order_count>100){
                    let m = generate_and_process_random_order(&mut order_book, &mut price_time_map).expect("could not match");
                }


            }
        }
    };


}




#[cfg(test)]
mod tests {
    use super::*;
    use std::{time::{Instant, Duration}, hash::Hash};
    use rand::prelude::{IteratorRandom, SliceRandom};


    #[test]
    fn test_insert_orders_duration() {
        // Arrange: Setup the test database
        let mut order_book: BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>> = BTreeMap::new();
        let random_orders = generate_rand_orders(100000);


        let start_time = Instant::now();

        for order in random_orders {
            let price_map = order_book.entry(order.price).or_insert_with(BTreeMap::new);
            let timestamp_map = price_map.entry(order.timestamp).or_insert_with(HashMap::new);
            timestamp_map.insert(order.hash.clone(), order);
        }


        let end_time = Instant::now();
        let duration = end_time - start_time;

        println!("duration: {:?}",duration);


        let max_expected_duration = Duration::from_millis(160); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Inserting an order took longer than expected: {:?}",
            duration
        );
    }

    #[test]
    fn test_delete_orders() {
        let (mut order_book, mut price_time_map): (BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,HashMap<String,(u128,i64)>) = set_up_test_orders(100000);

        // Get the initial count of all orders
        let initial_order_count: usize = order_book.values()
            .map(|price_map| price_map.values().map(|timestamp_map| timestamp_map.len()).sum::<usize>())
            .sum();

        // Select random orders to verify and delete
        let ordered_orders = get_book_orders_ordered_by_price_timestamp(&order_book);
        let mut rng = rand::thread_rng();
        let orders_to_delete: Vec<(u128, i64, String)> = ordered_orders
            .choose_multiple(&mut rng, 50000)
            .map(|order| (order.price, order.timestamp, order.hash.clone()))
            .collect();

        // Verify existence of orders before deletion
        for (price, timestamp, hash) in &orders_to_delete {
            assert!(order_book.contains_key(price));
            assert!(order_book[price].contains_key(timestamp));
            assert!(order_book[price][timestamp].contains_key(hash));
        }

        let start_time = Instant::now();

        // Act: Delete the selected orders
        for (_, _, hash) in &orders_to_delete {
            delete_order(&mut order_book, &mut price_time_map, hash).expect("TODO: panic message");
        }

        let end_time = Instant::now();
        let duration = end_time - start_time;

        // Assert: Check if the deletion duration is within an expected range
        let max_expected_duration = Duration::from_millis(120); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Deleting orders took longer than expected: {:?}",
            duration
        );


        // Verify non-existence of orders after deletion
        for (price, timestamp, hash) in &orders_to_delete {
            assert!(!order_book.get(price).map_or(false, |p| p.get(timestamp).map_or(false, |t| t.contains_key(hash))));
        }

        // Assert the count of orders after deletion
        let remaining_order_count: usize = order_book.values()
            .map(|price_map| price_map.values().map(|timestamp_map| timestamp_map.len()).sum::<usize>())
            .sum();

        // Assert that the number of orders deleted is as expected
        assert_eq!(initial_order_count - remaining_order_count, orders_to_delete.len());

    }

    #[test]
    fn test_update_orders() {
        let (mut order_book, mut price_time_map): (BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,HashMap<String,(u128,i64)>) = set_up_test_orders(100000);

        // Select random orders to update
        let ordered_orders = get_book_orders_ordered_by_price_timestamp(&order_book);
        let mut rng = rand::thread_rng();
        let orders_to_update: Vec<(u128, i64, String)> = ordered_orders
            .choose_multiple(&mut rng, 50000)
            .map(|order| (order.price, order.timestamp, order.hash.clone()))
            .collect();

        // Verify existence of orders before update
        for (price, timestamp, hash) in &orders_to_update {
            assert!(order_book.contains_key(price));
            assert!(order_book[price].contains_key(timestamp));
            assert!(order_book[price][timestamp].contains_key(hash));
        }

        let start_time = Instant::now();

        // Act: Update the selected orders
        for (price, timestamp, hash) in &orders_to_update {
            let updated_order_data = generate_rand_order(); // Function to generate a new BookOrder with updated values
            update_order(&mut order_book, *price, *timestamp, hash, updated_order_data);
        }

        let end_time = Instant::now();
        let duration = end_time - start_time;

        // Assert: Check if the update duration is within an expected range
        let max_expected_duration = Duration::from_millis(900); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Updating orders took longer than expected: {:?}",
            duration
        );

        // Verify the updates of orders
        for (price, timestamp, hash) in &orders_to_update {
            let order = order_book.get(price).and_then(|p| p.get(timestamp)).and_then(|t| t.get(hash));
            assert!(order.is_some(), "Order should exist after update");
            // Additional assertions can be made here to check if the order's properties were updated as expected
        }
    }



    fn set_up_test_orders(size: usize) -> (BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,HashMap<String,(u128,i64)>) {
        let mut order_book: BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>> = BTreeMap::new();
        let mut price_time_map: HashMap<String,(u128,i64)> = HashMap::new();
        let random_orders = generate_rand_orders(100000);

        for order in random_orders {
            let order_hash = order.hash.to_owned();
            let price = order.price;
            let timestamp = order.timestamp;

            let price_map = order_book.entry(order.price).or_insert_with(BTreeMap::new);
            let timestamp_map = price_map.entry(order.timestamp).or_insert_with(HashMap::new);
            timestamp_map.insert(order.hash.clone(), order);
            price_time_map.insert(order_hash,(price, timestamp));

        }
        (order_book, price_time_map)
    }


    #[test]
    fn test_get_orders_ordered_by_price_timestamp_duration() {
        let (order_book, price_time_map): (BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,HashMap<String,(u128,i64)>) = set_up_test_orders(500);

        let start_time = Instant::now();
        let ordered_orders = get_book_orders_ordered_by_price_timestamp(&order_book);

        let end_time = Instant::now();
        let duration = end_time - start_time;

        let max_expected_duration = Duration::from_millis(5); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Fetching native orders in price time order took longer than expected: {:?}",
            duration
        );


        // Assert: Check if the orders are ordered correctly
        let mut prev_price = u128::MIN;
        let mut prev_timestamp = i64::MIN;

        for order in ordered_orders.iter() {
            let price = order.price;
            let timestamp = order.timestamp;

            //println!("order {:?}", order);

            assert!(price >= prev_price);
            if price == prev_price {
                assert!(timestamp >= prev_timestamp);
            }
            prev_price = price;
            prev_timestamp = timestamp;
        }

    }


/*    #[test]
    fn test_snapshot() {
        use std::fs::File;
        use std::io::Write;

        let random_orders = generate_rand_orders(1000);

        let start_time = Instant::now();
        let mut start = Instant::now();
        
        let snapshot = get_snapshot_data(&mut connection);

        let fetch_duration = Instant::now() - start;
        println!("Fetching snapshot data: {:?}",fetch_duration);

        start = Instant::now();
        let mut json_string:String = String::new();
        match snapshot {
            Ok(data) => {
                match serde_json::to_string(&data) {
                    Ok(data) => json_string=data,
                    Err(err) => eprintln!("Error converting to JSON: {}", err),
                }
            }
            Err(err) => eprintln!("Error fetching data: {}", err),
        }

        let mutating_data_to_string_duration = Instant::now() - start;
        println!("Mutating data to string duration: {:?}",mutating_data_to_string_duration);

        start = Instant::now();

        let mut file = File::create("snapshot.json").expect("Error creating a file");
        file.write_all(json_string.as_bytes()).expect("Error saving data into file");

        let inserting_data_to_file = Instant::now() - start;
        println!("Saving data to file duration: {:?}",inserting_data_to_file);

        let duration = Instant::now() - start_time;
        println!("Taking snapshot of all orders complete duration: {:?}",duration);

        let max_expected_duration = Duration::from_millis(20); // Adjust this as needed

        assert!(
            duration <= max_expected_duration,
            "Taking snapshot took longer than expected: {:?}",
            duration
        );
    }*/

    #[test]
    fn test_upsert_order() {
        let (mut order_book, mut price_time_map): (BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,HashMap<String,(u128,i64)>) = set_up_test_orders(100);
        // Choose a random order to update
        let mut rng = rand::thread_rng();
        let random_order_to_update = order_book.values()
            .flat_map(|price_map| price_map.values())
            .flat_map(|timestamp_map| timestamp_map.values())
            .choose(&mut rng)
            .cloned()
            .unwrap();

        // Update some fields of the order
        let updated_price = random_order_to_update.price + 1000; // Example modification
        let updated_order = BookOrder {
            price: updated_price,
            ..random_order_to_update
        };

        // Upsert the updated order
        upsert_order(&mut order_book, &mut price_time_map, updated_order.clone());

        // Verify that the order was updated
        let updated_order_in_book = order_book.get(&updated_price)
            .and_then(|price_map| price_map.get(&updated_order.timestamp))
            .and_then(|timestamp_map| timestamp_map.get(&updated_order.hash));

        assert!(updated_order_in_book.is_some());
        assert_eq!(updated_order_in_book.unwrap(), &updated_order);

        // Create a completely new order and upsert it
        let new_order = generate_rand_order();
        upsert_order(&mut order_book, &mut price_time_map, new_order.clone());

        // Verify that the new order was inserted
        let new_order_in_book = order_book.get(&new_order.price)
            .and_then(|price_map| price_map.get(&new_order.timestamp))
            .and_then(|timestamp_map| timestamp_map.get(&new_order.hash));

        assert!(new_order_in_book.is_some());
        assert_eq!(new_order_in_book.unwrap(), &new_order);
    }

}
