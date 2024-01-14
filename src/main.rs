mod sockets;
mod models;
mod env;

extern crate diesel;
extern crate dotenv;


use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel::select;


use std::{fs, iter, thread};
use std::sync::mpsc;
use std::time::Instant;
use rand::distributions::Alphanumeric;
use rand::Rng;
use chrono::{Utc};
use crate::env::EnvVars;
use crate::models::binance_models::DepthUpdate;
use crate::models::common::{BinanceOrderBook, Config};
use crate::sockets::binance_depth_update_socket::BinanceDepthUpdateStream;
use crate::sockets::binance_ob_socket::BinanceOrderBookStream;
use crate::sockets::common::{DepthUpdateStream, OrderBookStream};

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

struct Match {
    price: i64,
    quantity: i64,
}

impl Match {
    fn new() -> Match {
        Match { price: 0, quantity: 0 }
    }

    fn add(&mut self, price: i64, quantity: i64) {
        self.price = price; // Depending on your requirement, you might want to calculate the average price etc.
        self.quantity += quantity;
    }
}


fn match_and_process_orders(connection: &mut SqliteConnection, perp: &str, mut quantity: i64) -> QueryResult<Match> {
    let orders = get_orders_ordered_by_price_timestamp(connection, perp)?;
    let mut matched = Match::new();

    for (id, _, price, _, order_quantity, _, _, _, _, _, _) in orders {
        if quantity == 0 {
            break;
        }

        if order_quantity <= quantity {
            // Match the whole order
            matched.add(price, order_quantity);
            delete_order_by_id(connection, id)?;
            quantity -= order_quantity;
        } else {
            // Partially match the order
            matched.add(price, quantity);
            let updated_quantity = order_quantity - quantity;
            update_order(
                connection,
                id,
                Some(price),
                Some(updated_quantity),
                None, // Rest of the fields remain unchanged
                None,
                None,
                None,
                None,
            )?;
            quantity = 0;
        }
    }

    if quantity > 0 {
        // Handle remaining unmatched quantity
        // e.g., log a warning or return a specific error
    }

    Ok(matched)
}


fn generate_and_process_random_order<'a>(
    connection: &mut SqliteConnection,
    markets: &'a [&'a str],
    makers: &'a Vec<String>,
    flags: &'a Vec<String>,
) -> QueryResult<()> {
    // Generate a random order
    let random_order = generate_random_order(0, markets, makers, flags); // '0' for id, it will be auto-generated by the database

    // Extract the quantity from the generated order
    let quantity = random_order.quantity * 1000000000000;

    // Call match_and_process_orders with the generated quantity
    match_and_process_orders(connection, random_order.market, quantity)
}


fn main() {
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
        .get(&"https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=100".to_string())
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

    let market = "BTC-PERP";
    let maker = "SampleMaker";
    let flags = "SampleFlags";

    let mut connection = establish_connection();

    let new_orders = convert_order_book_entries_to_new_orders(&order_book.bids, market, maker, flags);

    for new_order in &new_orders {
        diesel::insert_into(orders::table)
            .values(new_order)
            .execute(&mut connection)
            .expect("Error inserting new order");
    }

    let orders = get_orders_ordered_by_price_timestamp(&mut connection, "BTC-PERP")
        .expect("Error fetching orders");

    for (id, market, price, timestamp, quantity, trigger_price, leverage, expiration, salt, maker, flags) in orders {
        println!("Order ID: {}", id);
        println!("Market: {}", market);
        println!("Price: {}", price);
        println!("Timestamp: {}", timestamp);
        println!("Quantity: {}", quantity);
        println!("Trigger Price: {}", trigger_price);
        println!("Leverage: {}", leverage);
        println!("Expiration: {}", expiration);
        println!("Salt: {}", salt);
        println!("Maker: {}", maker);
        println!("Flags: {}", flags);
        println!("-----------------------------");
    }




    let binance_websocket_url_for_ob = vars.binance_websocket_url.clone();
    let binance_websocket_url_for_depth_diff = vars.binance_websocket_url.clone();

/*    let handle_binance_ob = thread::spawn(move || {
        let ob_stream = BinanceOrderBookStream::<DepthUpdate>::new();
        let url = format!(
            "{}/ws/{}@depth20@100ms",
            &binance_websocket_url_for_ob, &binance_market_for_ob
        );
        ob_stream.stream_ob_socket(
            &url,
            &binance_market_for_ob,
            tx_binance_ob
        );
    });*/

// Now you can use binance_websocket_url_for_depth_diff for the second thread
    let handle_binance_diff = thread::spawn(move || {
        let diff_depth_stream = BinanceDepthUpdateStream::<crate::models::common::DepthUpdate>::new();
        let url = format!(
            "{}/ws/{}@depth@100ms",
            &binance_websocket_url_for_depth_diff, &binance_market_for_depth_diff
        );
        diff_depth_stream.stream_depth_update_socket(
            &url,
            &binance_market_for_depth_diff,
            tx_binance_depth_diff
        );
    });


    loop {
/*        match rx_binance_ob.try_recv() {
            Ok(value) => {

                let start_time = Instant::now();

                delete_all_orders(&mut connection).expect("Error deleting orders");

                let market = "ETH-PERP";
                let maker = "SampleMaker";
                let flags = "SampleFlags";


                let new_orders = convert_order_book_entries_to_new_orders(&order_book.bids, market, maker, flags);
                for new_order in &new_orders {
                    diesel::insert_into(orders::table)
                        .values(new_order)
                        .execute(&mut connection)
                        .expect("Error inserting new order");
                }

                let end_time = Instant::now();
                let duration = end_time - start_time;
                tracing::info!("orderbook refresh duration: {:?}", duration);
                tracing::info!("binance ob: {:?}", value);
            }
            Err(mpsc::TryRecvError::Empty) => {
                // No message from binance yet
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                tracing::debug!("Binance worker has disconnected!");
            }
        }*/

        match rx_binance_depth_diff.try_recv() {
            Ok(value) => {

                tracing::info!("bids count: {:?}", value.bids.len());

                let start_time = Instant::now();

                for bid in &value.bids {
                    if(bid.0 > 0) {
                        let updated_rows = update_order(
                            &mut connection,
                            bid.0, // Use the ID of the inserted order
                            Some(bid.0), // New price
                            Some(bid.1), // New quantity
                            None, // Not updating trigger_price
                            None, // Not updating leverage
                            None, // Not updating expiration
                            None, // New maker
                            None,  // Not updating flags
                        ).expect("Error updating order");
                        tracing::info!("updated_rows: {:?}", updated_rows);
                        if(updated_rows == 0){
                          let new_order =  NewOrder {
                                id:bid.0,
                                market,
                                price:bid.0,
                                timestamp: Utc::now().timestamp(),
                                quantity:bid.1,
                                trigger_price: 0,
                                leverage: 1,
                                expiration: 0,
                                salt: rand::random(),
                                maker,
                                flags,
                            };
                            diesel::insert_into(orders::table)
                                .values(new_order)
                                .execute(&mut connection)
                                .expect("Error inserting new order");
                        }

                    } else {
                        let delete_rows = delete_order_by_id(&mut connection, bid.0).expect("Error deleting order by id");
                        tracing::info!("delete_rows: {:?}", delete_rows);
                    }
                }

                let end_time = Instant::now();
                let duration = end_time - start_time;
                tracing::info!("orderbook update duration: {:?}", duration);







                tracing::info!("binance depth diff: {:?}", value);
            }
            Err(mpsc::TryRecvError::Empty) => {
                // No message from binance yet
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                tracing::debug!("Binance worker has disconnected!");
            }
        }

        let markets = ["BTC-PERP"];
        let (makers, flags) = generate_random_makers_and_flags(1);

        match generate_and_process_random_order(&mut connection, &markets, &makers, &flags) {
            Ok(_) => println!("Order processed successfully"),
            Err(e) => println!("Error processing order: {:?}", e),
        }


    }

    //handle_binance_ob.join().expect("Thread failed to join main");
}


#[cfg(test)]
mod tests {
    use super::*;
    use diesel::connection::SimpleConnection;
    use std::time::{Instant, Duration};
    use diesel::dsl::Order;
    use diesel::sql_query;
    use rand::prelude::SliceRandom;

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
        let max_expected_duration = Duration::from_secs(2); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Insertion took longer than expected: {:?}",
            duration
        );
    }

    #[test]
    fn test_get_orders_ordered_by_price_timestamp_duration() {
        // Arrange: Setup the test database
        let mut connection = setup_test_database(10000);

        // Act: Measure the time it takes to insert orders
        let start_time = Instant::now();

        // Act: Get the orders and check if they are ordered correctly
        let orders = get_orders_ordered_by_price_timestamp(&mut connection, "ETH-PERP")
            .expect("Error fetching orders");
        let end_time = Instant::now();
        let duration = end_time - start_time;
        println!("get orders duration: {:?}", duration);


        // Assert: Check if the insertion duration is within an expected range
        let max_expected_duration = Duration::from_millis(10); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Getting orders ordered by price and timestamp took longer than expected: {:?}",
            duration
        );
    }

    #[test]
    fn test_delete_random_orders() {
        // Arrange: Setup the test database with orders
        let mut connection = setup_test_database(200000);
        let market = "ETH-PERP";

        // Get all order IDs in the database
        let order_ids: Vec<i64> = orders::table
            .select(orders::id)
            .load(&mut connection)
            .expect("Error loading order IDs");


        // Randomly select a subset of order IDs to delete
        let mut rng = rand::thread_rng();

        let random_order_ids: Vec<i64> = order_ids
            .choose_multiple(&mut rng, 100000) // Choose 10 random IDs
            .cloned()
            .collect();

        let start_time = Instant::now();

        let res = delete_orders_by_ids(&mut connection, random_order_ids).expect("Error deleting order");

        let end_time = Instant::now();
        let duration = end_time - start_time;
        println!("delete orders duration: {:?}", duration);

        let max_expected_duration = Duration::from_millis(120); // Adjust this as needed
        assert!(
            duration <= max_expected_duration,
            "Deleting orders took longer than expected: {:?}",
            duration
        );
    }

    #[test]
    fn test_update_order() {
        // Arrange: Setup the test database with orders
        let mut connection = setup_test_database(10); // Adjust the number as needed


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

        // Perform the delete operation
        let delete_count = delete_all_orders(&mut connection).expect("Error deleting orders");

        // Assert that 10 records were deleted
        assert_eq!(delete_count, 100000);

        let orders = get_orders_ordered_by_price_timestamp(&mut connection, "ETH-PERP")
            .expect("Error fetching orders");

        assert_eq!(orders.len(),0);
    }
}
