mod sockets;
mod models;
mod env;

extern crate diesel;
extern crate dotenv;


use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel::select;


use std::{fmt, fs, iter, thread};
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use rand::distributions::Alphanumeric;
use rand::Rng;
use chrono::{Utc};
use crate::env::EnvVars;
use crate::models::binance_models::DepthUpdate;
use crate::models::common::{BinanceOrderBook, Config};
use crate::sockets::binance_depth_update_socket::BinanceDepthUpdateStream;
use crate::sockets::binance_ob_socket::BinanceOrderBookStream;
use crate::sockets::common::{DepthUpdateStream, OrderBookStream};
use generational_token_list::GenerationalTokenList;
use generational_token_list::ItemToken;

table! {
    orders (id) {
        id -> BigInt,
        market -> Text,
        price -> BigInt,
        timestamp -> BigInt,
        quantity -> BigInt,
        trigger_price -> BigInt,
        leverage -> BigInt,
        expiration -> BigInt,
        salt -> BigInt,
        maker -> Text,
        flags -> Text,
    }
}

const CREATE_ORDERS_TABLE: &str = "
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    market TEXT NOT NULL,
    price INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    trigger_price INTEGER NOT NULL,
    leverage INTEGER NOT NULL,
    expiration INTEGER NOT NULL,
    salt INTEGER NOT NULL,
    maker TEXT NOT NULL,
    flags TEXT NOT NULL
);

CREATE INDEX idx_orders_market ON orders(market);
CREATE INDEX idx_orders_price ON orders(price);
CREATE INDEX idx_orders_timestamp ON orders(timestamp);
";


pub fn establish_connection() -> SqliteConnection {
    let mut connection = SqliteConnection::establish(":memory:")
        .expect("Error connecting to in-memory SQLite");

    diesel::sql_query(CREATE_ORDERS_TABLE)
        .execute(&mut connection)
        .expect("Error creating posts table");

    connection
}

#[derive(Insertable)]
#[table_name = "orders"]
struct NewOrder<'a> {
    id: i64,
    market: &'a str,
    price: i64,
    timestamp: i64,
    quantity: i64,
    trigger_price: i64,
    leverage: i64,
    expiration: i64,
    salt: i64,
    maker: &'a str,
    flags: &'a str,
}

// Define a struct to represent the fields you want to update
#[derive(AsChangeset)]
#[table_name = "orders"]
struct OrderUpdate<'a> {
    price: Option<i64>,
    quantity: Option<i64>,
    trigger_price: Option<i64>,
    leverage: Option<i64>,
    expiration: Option<i64>,
    maker: Option<&'a str>,
    flags: Option<&'a str>,
}

// Function to update an order
fn update_order<'a>(
    connection: &mut SqliteConnection,
    order_id: i64,
    new_price: Option<i64>,
    new_quantity: Option<i64>,
    new_trigger_price: Option<i64>,
    new_leverage: Option<i64>,
    new_expiration: Option<i64>,
    new_maker: Option<&'a str>,
    new_flags: Option<&'a str>,
) -> QueryResult<usize> {
    use self::orders::dsl::*;

    // Create an instance of OrderUpdate with the new values
    let updated_order = OrderUpdate {
        price: new_price,
        quantity: new_quantity,
        trigger_price: new_trigger_price,
        leverage: new_leverage,
        expiration: new_expiration,
        maker: new_maker,
        flags: new_flags,
    };

    // Update the order with the specified ID
    diesel::update(orders.filter(id.eq(order_id)))
        .set(&updated_order)
        .execute(connection)
}


fn generate_random_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(len)
        .collect()
}

fn get_orders_ordered_by_price_timestamp(connection: &mut SqliteConnection, perp: &str) -> QueryResult<Vec<(i64, String, i64, i64, i64, i64, i64, i64, i64, String, String)>> {
    use self::orders::dsl::*;

    let result = orders
        .filter(market.eq(perp)) // Filter orders by the specified market
        .order((price.asc(), timestamp.asc())) // Orders by price ascending, then by timestamp ascending
        .load::<(i64, String, i64, i64, i64, i64, i64, i64, i64, String, String)>(connection);

    result
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

fn get_snapshot_data(connection: &mut SqliteConnection) -> QueryResult<Vec<(i64, String, i64, i64, i64, i64, i64, i64, i64, String, String)>> {
    use self::orders::dsl::*;
    
    let result = orders
        // .order((price.asc(), timestamp.asc()))// Orders by price ascending, then by timestamp ascending
        .load::<(i64, String, i64, i64, i64, i64, i64, i64, i64, String, String)>(connection);
    
    result
}

fn generate_random_order<'a>(id:i64, markets: &'a [&'a str], makers: &'a Vec<String>, flags: &'a Vec<String>) -> NewOrder<'a> {
    let mut rng = rand::thread_rng();
    let market = markets[rng.gen_range(0..markets.len())]; // Randomly choose a market
    let maker = &makers[rng.gen_range(0..makers.len())];
    let flag = &flags[rng.gen_range(0..flags.len())]; // Select a random flag from the Vec<String>
    let timestamp = Utc::now().timestamp(); // Generate a real timestamp

    NewOrder {
        id,
        market,
        price: rng.gen_range(50_i64..=250),
        timestamp: timestamp + rng.gen_range(10..=250),
        quantity: rng.gen_range(10_i64..=25),
        trigger_price: rng.gen::<u64>() as i64,
        leverage: rng.gen_range(1..=20),
        expiration: rng.gen::<u64>() as i64,
        salt: rng.gen::<u64>() as i64,
        maker,
        flags: flag, // Assign the randomly selected flag as a &str
    }
}

fn generate_random_makers_and_flags(count: usize) -> (Vec<String>, Vec<String>) {
    let mut makers = Vec::new();
    let mut flags = Vec::new();

    for _ in 0..count {
        makers.push(generate_random_string(10));
        flags.push(generate_random_string(5));
    }

    (makers, flags)
}

fn generate_random_orders<'a>(markets: &'a [&'a str], makers: &'a Vec<String>, flags: &'a Vec<String>, count: usize) -> Vec<NewOrder<'a>> {
    let mut orders = Vec::new();
    for i in 0..count {
        let new_order = generate_random_order(i as i64, markets, makers, flags);
        orders.push(new_order);
    }
    orders
}

fn delete_all_orders(connection: &mut SqliteConnection) -> QueryResult<usize> {
    use self::orders::dsl::*;

    diesel::delete(orders).execute(connection)
}

fn delete_orders_by_ids(connection: &mut SqliteConnection, order_ids: Vec<i64>) -> QueryResult<usize> {
    use self::orders::dsl::*;

    //let old_count = orders.count().first::<i64>(connection);

    let deleted_rows = diesel::delete(orders.filter(id.eq_any(order_ids)))
        .execute(connection)?;

    //let new_count = orders.count().first::<i64>(connection);

    Ok(deleted_rows)
}

fn delete_order_by_id(connection: &mut SqliteConnection, order_id: i64) -> QueryResult<usize> {
    use self::orders::dsl::*;

    let deleted_rows = diesel::delete(orders.filter(id.eq(order_id)))
        .execute(connection)?;

    Ok(deleted_rows)
}

fn convert_order_book_entries_to_new_orders<'a>(
    orders_data: &'a Vec<(i64, i64)>,
    market: &'a str,
    maker: &'a str,
    flags: &'a str
) -> Vec<NewOrder<'a>> {
    orders_data.iter().map(|&(price, quantity)| {
        NewOrder {
            id:price,
            market,
            price,
            timestamp: Utc::now().timestamp(),
            quantity,
            trigger_price: 0,
            leverage: 1,
            expiration: 0,
            salt: rand::random(),
            maker,
            flags,
        }
    }).collect()
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
    mut quantity: u128
) -> QueryResult<Match> {
    let mut orders_to_process = Vec::new();
    let mut total_matched_quantity = 0;
    let mut matched = Match::new();

    'outer: for (&price, price_map) in order_book.iter() {
        for (&timestamp, timestamp_map) in price_map.iter() {
            for (_, order) in timestamp_map.iter() {
                if quantity == 0 {
                    break 'outer; // This breaks out of all loops, not just the innermost one
                }

                let matched_quantity = std::cmp::min(order.quantity, quantity);
                quantity -= matched_quantity;
                total_matched_quantity += matched_quantity;

                matched.add(price, matched_quantity);

                if order.quantity <= matched_quantity {
                    orders_to_process.push((price, timestamp, order.hash.clone(), order.quantity));
                } else {
                    let updated_quantity = order.quantity - matched_quantity;
                    orders_to_process.push((price, timestamp, order.hash.clone(), updated_quantity));
                }
            }
        }
    }

    // Perform mutations based on the collected data
    for (price, timestamp, hash, matched_quantity) in orders_to_process {
        if let Some(order) = order_book.get_mut(&price)
            .and_then(|price_map| price_map.get_mut(&timestamp))
            .and_then(|timestamp_map| timestamp_map.get_mut(&hash)) {
            if order.quantity <= matched_quantity {
                delete_order(order_book, price, timestamp, &hash);
            } else {
                let updated_quantity = order.quantity - matched_quantity;
                update_order_quantity(order_book, price, timestamp, &hash, updated_quantity);
            }
        }
    }

    Ok(matched)
}


fn generate_and_process_random_order( order_book: &mut BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,

) -> QueryResult<Match> {
    // Generate a random order
    let random_order = generate_rand_order(); // '0' for id, it will be auto-generated by the database

    // Extract the quantity from the generated order
    let quantity = random_order.quantity;

    // Call match_and_process_orders with the generated quantity

    let start_time = Instant::now();

    let r = match_and_process_orders(order_book, quantity);

    let end_time = Instant::now();
    let duration = end_time - start_time;
    println!("match_and_process_orders duration: {:?} match speed: {:?}", duration, 1000000 / duration.as_micros());
    r
}

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq)]
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
        hash: rng.gen::<u128>().to_string(),
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
fn delete_order(
    order_book: &mut BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>,
    price: u128,
    timestamp: i64,
    order_hash: &str,
) -> Option<BookOrder> {
    // Access the nested maps using the price and timestamp.
    if let Some(price_map) = order_book.get_mut(&price) {
        if let Some(timestamp_map) = price_map.get_mut(&timestamp) {
            // Remove the order with the specific hash.
            let removed_order = timestamp_map.remove(order_hash);

            // Remove the timestamp map if it becomes empty.
            if timestamp_map.is_empty() {
                price_map.remove(&timestamp);
            }

            // Remove the price map if it becomes empty.
            if price_map.is_empty() {
                order_book.remove(&price);
            }

            return removed_order;
        }
    }
    None // Return None if the order was not found.
}


 fn update_native_order(
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
    new_order: BookOrder
) {
    let price_map = order_book.entry(new_order.price).or_insert_with(BTreeMap::new);
    let timestamp_map = price_map.entry(new_order.timestamp).or_insert_with(HashMap::new);

    // Upsert the order: update if exists, insert if not
    timestamp_map.insert(new_order.hash.clone(), new_order);
}

fn count_orders_in_book(order_book: &BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>>) -> usize {
    order_book.values()
        .flat_map(|price_map| price_map.values())
        .map(|timestamp_map| timestamp_map.len())
        .sum()
}
fn main() {

    let mut native_order_book: BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>> = BTreeMap::new();

    let vars: EnvVars = env::env_variables();
    let _guard = env::init_logger(vars.log_level);
    let config_str =
        fs::read_to_string("src/config/config.json").expect("Unable to read config.json");
    let config: Config = serde_json::from_str(&config_str).expect("JSON was not well-formatted");
    let market = config.markets.first().unwrap();
    let binance_market = market.symbols.binance.to_owned();
    let binance_market_for_ob = binance_market.clone();
    let binance_market_for_depth_diff = binance_market.clone();


    //let (tx_binance_ob, rx_binance_ob) = mpsc::channel();
    let (tx_binance_depth_diff, rx_binance_depth_diff) = mpsc::channel();

    let vars: EnvVars = env::env_variables();

    let client = reqwest::blocking::Client::new();

    let order_book_result: Result<BinanceOrderBook, Box<dyn std::error::Error>> = client
        .get(&"https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000".to_string())
        .send()
        .map_err(|e| format!("Error making the request: {}", e).into())
        .and_then(|res| {
            res.text()
                .map_err(|e| format!("Error reading the response body: {}", e).into())
        })
        .and_then(|body|
            serde_json::from_str(&body).
                map_err(Into::into));


    let order_book = order_book_result
        .map(|response| response)
        .map_err(|e| {
            tracing::error!("Error: {}", e);
            e
        })
        .unwrap();


    for bid in order_book.bids.iter() {
        // Convert the bid to a BookOrder
        let book_order = BookOrder {
            is_buy: true, // Assuming these are buy orders; adjust if necessary
            reduce_only: false, // Set according to your logic
            quantity: bid.1 as u128,
            price: bid.0 as u128,
            timestamp: Utc::now().timestamp(),
            trigger_price: 0, // Set according to your logic
            leverage: 1, // Set according to your logic
            expiration: 0, // Set according to your logic
            hash: generate_random_string(10), // Generate a unique hash for the order
            salt: rand::random(),
            maker: "SomeMaker".to_string(), // Replace with actual maker
            flags: "SomeFlags".to_string(), // Replace with actual flags
        };

        // Upsert the order in the native order book
        upsert_order(&mut native_order_book, book_order);
    }

    // Print the native order book after upserting
    for (price, timestamp_map) in &native_order_book {
        println!("Price: {}", price);
        for (timestamp, orders) in timestamp_map {
            println!("\tTimestamp: {}", timestamp);
            for (hash, order) in orders {
                println!("\t\tHash: {}", hash);
                println!("\t\tOrder: {:?}", order);
            }
        }
    }


    let binance_websocket_url_for_depth_diff = vars.binance_websocket_url.clone();

// Now you can use binance_websocket_url_for_depth_diff for the second thread
    let handle_binance_diff = thread::spawn(move || {
        let diff_depth_stream = BinanceDepthUpdateStream::<crate::models::common::DepthUpdate>::new();
        let url = format!(
            "{}/stream?streams={}@depth@100ms",
            &binance_websocket_url_for_depth_diff, &binance_market_for_depth_diff
        );
        diff_depth_stream.stream_depth_update_socket(
            &url,
            &binance_market_for_depth_diff,
            tx_binance_depth_diff
        );
    });


    loop {
        match rx_binance_depth_diff.try_recv() {
            Ok(value) => {

                tracing::info!("bids count: {:?}", value.data.bids.len());

                let start_time = Instant::now();

                for bid in &value.data.bids {
                    // Convert the bid to a BookOrder
                    let book_order = BookOrder {
                        is_buy: true, // Assuming these are buy orders; adjust if necessary
                        reduce_only: false, // Set according to your logic
                        quantity: bid.1 as u128,
                        price: bid.0 as u128,
                        timestamp: Utc::now().timestamp(),
                        trigger_price: 0, // Set according to your logic
                        leverage: 1, // Set according to your logic
                        expiration: 0, // Set according to your logic
                        hash: bid.0.to_string(), // Generate a unique hash for the order
                        salt: rand::random(),
                        maker: generate_random_string(10), // Assuming 'maker' is defined in your scope
                        flags: generate_random_string(10), // Assuming 'flags' are defined in your scope
                    };
                    if(bid.1 > 0) {
                        // Upsert the order in the in-memory order book
                        upsert_order(&mut native_order_book, book_order);

                    } else if (bid.1 == 0) {
                        delete_order(&mut native_order_book, book_order.price, book_order.timestamp, &book_order.hash);
                    }
                }

                let end_time = Instant::now();
                let duration = end_time - start_time;
                tracing::info!("orderbook update duration: {:?}", duration);

                tracing::info!("binance depth diff: {:?}", value);

                let m = generate_and_process_random_order(&mut native_order_book).expect("could not match");

                let order_count = count_orders_in_book(&native_order_book);
                tracing::info!("Total number of orders: {}", order_count);


            }
            Err(mpsc::TryRecvError::Empty) => {
                // No message from binance yet
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                tracing::debug!("Binance worker has disconnected!");
            }
        }

    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use diesel::connection::SimpleConnection;
    use std::{time::{Instant, Duration}, hash::Hash};
    use diesel::dsl::Order;
    use diesel::sql_query;
    use rand::prelude::{IteratorRandom, SliceRandom};

    // Helper function to create and populate the test database
    fn setup_test_database(count: usize) -> SqliteConnection {
        let mut connection = establish_connection();

        let markets = ["ETH-PERP", "BTC-PERP", "SOL-PERP"];
        let (makers, flags) = generate_random_makers_and_flags(count);

        let random_orders = generate_random_orders(&markets, &makers, &flags, count);

        for new_order in &random_orders {
            diesel::insert_into(orders::table)
                .values(new_order)
                .execute(&mut connection)
                .expect("Error inserting new order");
        }

        connection
    }

    #[test]
    fn test_get_orders_ordered_by_price_timestamp() {
        // Arrange: Setup the test database with orders
        let mut connection = setup_test_database(100000);

        // Act: Get the orders and check if they are ordered correctly
        let orders = get_orders_ordered_by_price_timestamp(&mut connection, "ETH-PERP")
            .expect("Error fetching orders");

        // Assert: Check if the orders are ordered by price and then by timestamp
        let mut prev_price = i64::MIN;
        let mut prev_timestamp = i64::MIN;

        for (_, _, price, timestamp, _, _, _, _, _, _, _) in orders {
            assert!(price >= prev_price);
            if price == prev_price {
                assert!(timestamp >= prev_timestamp);
            }
            prev_price = price;
            prev_timestamp = timestamp;
        }
    }

    #[test]
    fn test_insert_orders_duration() {
        // Arrange: Setup the test database
        let mut connection = establish_connection();
        let markets = ["ETH-PERP", "BTC-PERP", "SOL-PERP"];
        let order_size: usize = 100000;
        let (makers, flags) = generate_random_makers_and_flags(order_size);
        let random_orders = generate_random_orders(&markets, &makers, &flags, order_size);

        // Act: Measure the time it takes to insert orders
        let start_time = Instant::now();
        for new_order in &random_orders {
            diesel::insert_into(orders::table)
                .values(new_order)
                .execute(&mut connection)
                .expect("Error inserting new order");
        }
        let end_time = Instant::now();
        let duration = end_time - start_time;
        println!("insert orders duration: {:?}", duration);

        // Assert: Check if the insertion duration is within an expected range
        let max_expected_duration = Duration::from_secs(3); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Insertion took longer than expected: {:?}",
            duration
        );
    }

    #[test]
    fn test_native_insert_orders_duration() {
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
    fn test_native_insert_orders_duration_3() {
        let mut order_book: BTreeMap<u128, GenerationalTokenList<BookOrder>> = BTreeMap::new();
        let mut orders_map:HashMap<String,ItemToken>=HashMap::new();

        let random_orders = generate_rand_orders(100000);

        let start_time = Instant::now();

        for order in random_orders {
            let hash = order.hash.clone();
            let price_list = order_book.entry(order.price).or_insert_with(GenerationalTokenList::new);
            let order_ref= price_list.push_back(order);
            orders_map.insert(hash,order_ref);
        }


        let end_time = Instant::now();
        let duration = end_time - start_time;

        println!("duration: {:?}",duration);
        let max_expected_duration = Duration::from_millis(140); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Inserting an order took longer than expected: {:?}",
            duration
        );
    }

    #[test]
    fn test_native_delete_orders() {
        let mut order_book: BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>> = set_up_native_test_orders(100000);

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
        for (price, timestamp, hash) in &orders_to_delete {
            delete_order(&mut order_book, *price, *timestamp, hash);
        }

        let end_time = Instant::now();
        let duration = end_time - start_time;

        // Assert: Check if the deletion duration is within an expected range
        let max_expected_duration = Duration::from_millis(70); // Adjust this as needed
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
    fn test_native_delete_orders_3() {
        let mut order_book: BTreeMap<u128, GenerationalTokenList<BookOrder>> = BTreeMap::new();
        let mut orders_map:HashMap<String,ItemToken>=HashMap::new();

        let random_orders = generate_rand_orders(100000);

        for order in random_orders.clone() {
            let hash = order.hash.clone();
            // let hash1 = order.hash.clone();  //open it if ran into ownership issue
            let price_list = order_book.entry(order.price).or_insert_with(GenerationalTokenList::new);
            let order_ref=price_list.push_back(order);
            orders_map.insert(hash,order_ref);

        }

        // Get the initial count of all orders
        let initial_order_count: usize = order_book.values()
            .map(|price_map|
                price_map.len())
            .sum();

       let mut rng = rand::thread_rng();
        let orders_to_delete: Vec<(u128, String)> = random_orders.clone()
            .choose_multiple(&mut rng, 50000)
            .map(|order| (order.price, order.hash.clone()))
            .collect();



       // Verify existence of orders before deletion
       for (price, hash) in &orders_to_delete {
           assert!(order_book.contains_key(price));
           assert!(order_book[price].get(*orders_map.get(hash).unwrap()).unwrap().hash.eq(hash));
       }

       let start_time = Instant::now();

        // Act: Delete the selected orders
        for (price, hash) in &orders_to_delete {
            if let Some(price_list) = order_book.get_mut(price) {
                if let Some(order_ref) = orders_map.get(hash) {
                    price_list.remove(*order_ref).unwrap();
                }
            }
        }

        let end_time = Instant::now();
        let duration = end_time - start_time;

        // Assert: Check if the deletion duration is within an expected range
        let max_expected_duration = Duration::from_millis(70); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Deleting orders took longer than expected: {:?}",
            duration
        );

        // Verify non-existence of orders after deletion
        let all_deleted = orders_to_delete.iter().all(|(price, hash)| {
            order_book.contains_key(price) &&
                order_book[price].get(*orders_map.get(hash).unwrap()).is_none()
        });

        assert!(all_deleted, "Some orders still exist after deletion");

        // Assert the count of orders after deletion
        let remaining_order_count: usize = order_book.values()
            .map(|price_map| price_map.len())
            .sum();

        // Assert that the number of orders deleted is as expected
        assert_eq!(initial_order_count - remaining_order_count, orders_to_delete.len());

    }



    #[test]
    fn test_update_native_orders() {
        let mut order_book: BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>> = set_up_native_test_orders(100000);

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
            update_native_order(&mut order_book, *price, *timestamp, hash, updated_order_data);
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



    fn set_up_native_test_orders(size: usize) -> BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>> {
        let mut order_book: BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>> = BTreeMap::new();
        let random_orders = generate_rand_orders(100000);

        for order in random_orders {
            let price_map = order_book.entry(order.price).or_insert_with(BTreeMap::new);
            let timestamp_map = price_map.entry(order.timestamp).or_insert_with(HashMap::new);
            timestamp_map.insert(order.hash.clone(), order);
        }
        order_book
    }


    #[test]
    fn test_get_native_orders_ordered_by_price_timestamp_duration() {
        let order_book: BTreeMap<u128, BTreeMap<i64, HashMap<String, BookOrder>>> = set_up_native_test_orders(500);

        let start_time = Instant::now();
        let ordered_orders = get_book_orders_ordered_by_price_timestamp(&order_book);

        let end_time = Instant::now();
        let duration = end_time - start_time;

        let max_expected_duration = Duration::from_millis(1); // Adjust this as needed
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
    #[test]
    fn test_get_orders_ordered_by_price_timestamp_duration() {
        // Arrange: Setup the test database
        let mut connection = setup_test_database(500);

        // Act: Measure the time it takes to insert orders
        let start_time = Instant::now();

        // Act: Get the orders and check if they are ordered correctly
        let orders = get_orders_ordered_by_price_timestamp(&mut connection, "ETH-PERP")
            .expect("Error fetching orders");
        let end_time = Instant::now();
        let duration = end_time - start_time;
        println!("get orders duration: {:?}", duration);


        // Assert: Check if the insertion duration is within an expected range
        let max_expected_duration = Duration::from_millis(1); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Getting orders ordered by price and timestamp took longer than expected: {:?}",
            duration
        );
    }

    #[test]
    fn test_delete_random_orders() {
        // Arrange: Setup the test database with orders
        let mut connection = setup_test_database(100000);
        let market = "ETH-PERP";

        // Get all order IDs in the database
        let order_ids: Vec<i64> = orders::table
            .select(orders::id)
            .load(&mut connection)
            .expect("Error loading order IDs");


        // Randomly select a subset of order IDs to delete
        let mut rng = rand::thread_rng();

        let random_order_ids: Vec<i64> = order_ids
            .choose_multiple(&mut rng, 50000) // Choose 10 random IDs
            .cloned()
            .collect();

        let start_time = Instant::now();

        let res = delete_orders_by_ids(&mut connection, random_order_ids).expect("Error deleting order");

        let end_time = Instant::now();
        let duration = end_time - start_time;
        println!("delete orders duration: {:?}", duration);

        let max_expected_duration = Duration::from_millis(100); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Deleting orders took longer than expected: {:?}",
            duration
        );
    }


    #[test]
    fn test_fetch_an_order() {
        // Arrange: Setup the test database with orders
        let mut connection = setup_test_database(100000);

        let start_time = Instant::now();
        // Get all order IDs in the database
        let order = orders::table.find(500).load::<(i64, String, i64, i64, i64, i64, i64, i64, i64, String, String)>(&mut connection);

        let end_time = Instant::now();
        let duration = end_time - start_time;
        println!("Order: {:?}", order);
        println!("Order fetch duration: {:?}", duration);

        let max_expected_duration = Duration::from_millis(2); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Fetching an order took longer than expected: {:?}",
            duration
        );
    }

    #[test]
    fn test_update_order() {
        // Arrange: Setup the test database with orders
        let mut connection = setup_test_database(100000); // Adjust the number as needed

        let start_time = Instant::now();
        // Act: Update the inserted order
        let updated_rows = update_order(
            &mut connection,
            1, // Use the ID of the inserted order
            Some(200), // New price
            Some(25), // New quantity
            None, // Not updating trigger_price
            None, // Not updating leverage
            None, // Not updating expiration
            Some("UpdatedMaker"), // New maker
            None,  // Not updating flags
        ).expect("Error updating order");

        let duration = Instant::now() - start_time;
        println!("Updating an order: {:?}",duration);

        // Assert: Verify that the order was updated correctly
        assert_eq!(updated_rows, 1);

        // Fetch the updated order and verify the changes
        let updated_order = orders::table.find(1)
            .first::<(i64, String, i64, i64, i64, i64, i64, i64, i64, String, String)>(&mut connection)
            .expect("Error fetching updated order");

        assert_eq!(updated_order.2, 200); // Check if price is updated to 200
        assert_eq!(updated_order.4, 25); // Check if quantity is updated to 25
        assert_eq!(updated_order.9, "UpdatedMaker"); // Check if maker is updated*/
    }


    #[test]
    fn test_delete_all_orders() {
        let mut connection = setup_test_database(100000);

        let start_time = Instant::now();
        // Perform the delete operation
        let delete_count = delete_all_orders(&mut connection).expect("Error deleting orders");
        
        let duration = Instant::now() - start_time;
        // Assert that 10 records were deleted
        assert_eq!(delete_count, 100000);
        
        println!("duration to delete all orders: {:?}",duration);

        let max_expected_duration = Duration::from_millis(3); // Adjust this as needed

        assert!(
            duration <= max_expected_duration,
            "Deleting all orders took longer than expected: {:?}",
            duration
        );

        let orders = get_orders_ordered_by_price_timestamp(&mut connection, "ETH-PERP")
            .expect("Error fetching orders");

        assert_eq!(orders.len(),0);
    }

    #[test]
    fn test_snapshot() {
        use std::fs::File;
        use std::io::Write;
        let mut connection = setup_test_database(1000);

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
    }

    #[test]
    fn test_upsert_order() {
        let mut order_book = set_up_native_test_orders(100); // Assume this sets up 100 random orders

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
        upsert_order(&mut order_book, updated_order.clone());

        // Verify that the order was updated
        let updated_order_in_book = order_book.get(&updated_price)
            .and_then(|price_map| price_map.get(&updated_order.timestamp))
            .and_then(|timestamp_map| timestamp_map.get(&updated_order.hash));

        assert!(updated_order_in_book.is_some());
        assert_eq!(updated_order_in_book.unwrap(), &updated_order);

        // Create a completely new order and upsert it
        let new_order = generate_rand_order();
        upsert_order(&mut order_book, new_order.clone());

        // Verify that the new order was inserted
        let new_order_in_book = order_book.get(&new_order.price)
            .and_then(|price_map| price_map.get(&new_order.timestamp))
            .and_then(|timestamp_map| timestamp_map.get(&new_order.hash));

        assert!(new_order_in_book.is_some());
        assert_eq!(new_order_in_book.unwrap(), &new_order);
    }

}
