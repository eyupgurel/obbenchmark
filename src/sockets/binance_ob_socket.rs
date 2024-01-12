use crate::env;
use crate::env::EnvVars;
use crate::models::common::OrderBook;
use crate::sockets::common::OrderBookStream;
use serde::de::DeserializeOwned;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use tungstenite::protocol::Message;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, WebSocket};
use url::Url;

pub struct BinanceOrderBookStream<T> {
    phantom: std::marker::PhantomData<T>, // Use PhantomData to indicate the generic type usage
}

impl<T> BinanceOrderBookStream<T> {
    pub fn new() -> Self {
        BinanceOrderBookStream {
            phantom: std::marker::PhantomData,
        }
    }
}

// Implement OrderBookStream for any type T that meets the trait bounds
impl<T> OrderBookStream<T> for BinanceOrderBookStream<T>
where
    T: DeserializeOwned + Into<OrderBook>,
{
    fn get_ob_socket(&self, url: &str, _market: &str) -> WebSocket<MaybeTlsStream<TcpStream>> {
        let (socket, _response) = connect(Url::parse(&url).unwrap()).expect("Can't connect.");

        tracing::info!("Connected to Binance stream at url:{}.", &url);
        return socket;
    }

    fn stream_ob_socket(
        &self,
        url: &str,
        market: &str,
        tx: Sender<OrderBook>,
        tx_diff: Sender<OrderBook>,
    ) {
        let vars: EnvVars = env::env_variables();
        let mut socket = self.get_ob_socket(url, market);
        let mut last_first_ask_price: Option<f64> = None;
        let mut last_first_bid_price: Option<f64> = None;

        loop {
            let read = socket.read();

            match read {
                Ok(message) => {
                    match message {
                        Message::Text(msg) => {
                            let parsed: T = serde_json::from_str(&msg).expect("Can't parse");
                            let ob: OrderBook = parsed.into();

                            let current_first_ask_price = ob.asks.first().map(|ask| ask.0.clone());
                            let current_first_bid_price = ob.bids.first().map(|bid| bid.0.clone());

                            let is_first_ask_price_changed =
                                match (current_first_ask_price, last_first_ask_price) {
                                    (Some(current), Some(last)) => {
                                        (current - last).abs() / last * 10000.0
                                            >= vars.market_making_trigger_bps
                                    }
                                    _ => false, // Consider unchanged if either current or last price is None
                                };

                            let is_first_bid_price_changed =
                                match (current_first_bid_price, last_first_bid_price) {
                                    (Some(current), Some(last)) => {
                                        (current - last).abs() / last * 10000.0
                                            >= vars.market_making_trigger_bps
                                    }
                                    _ => false, // Consider unchanged if either current or last price is None
                                };

                            // Update the last known prices
                            last_first_ask_price = current_first_ask_price;
                            last_first_bid_price = current_first_bid_price;

                            if is_first_ask_price_changed || is_first_bid_price_changed {
                                // Send the order book through the channel
                                tx_diff.send(ob.clone()).unwrap();
                            }
                            tx.send(ob).unwrap();
                        }
                        Message::Ping(ping_data) => {
                            // Handle the Ping message, e.g., by sending a Pong response
                            socket.write(Message::Pong(ping_data)).unwrap();
                        }
                        other => {
                            tracing::error!("Error: Received unexpected message type: {:?}", other);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error during message handling: {:?}", e);
                    socket = self.get_ob_socket(url, market);
                }
            }
        }
    }
}
