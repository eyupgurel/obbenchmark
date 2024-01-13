use crate::env;
use crate::env::EnvVars;
use crate::models::common::{DepthUpdate, OrderBook};
use serde::de::DeserializeOwned;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use tungstenite::protocol::Message;
use tungstenite::stream::MaybeTlsStream;

pub trait OrderBookStream<T>
where
    T: DeserializeOwned + Into<OrderBook>,
{
    fn get_ob_socket(
        &self,
        url: &str,
        market: &str,
    ) -> tungstenite::WebSocket<MaybeTlsStream<TcpStream>>;
    fn stream_ob_socket(
        &self,
        url: &str,
        market: &str,
        tx: Sender<OrderBook>,
    ) {
        let vars: EnvVars = env::env_variables();
        let mut socket = self.get_ob_socket(url, market);

        loop {
            let read = socket.read();

            match read {
                Ok(message) => {
                    match message {
                        Message::Text(msg) => {
                            let parsed: T = serde_json::from_str(&msg).expect("Can't parse");
                            let ob: OrderBook = parsed.into();

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


pub trait DepthUpdateStream<T>
    where
        T: DeserializeOwned + Into<DepthUpdate>,
{
    fn get_depth_update_socket(
        &self,
        url: &str,
        market: &str,
    ) -> tungstenite::WebSocket<MaybeTlsStream<TcpStream>>;
    fn stream_depth_update_socket(
        &self,
        url: &str,
        market: &str,
        tx: Sender<DepthUpdate>,
    ) {
        let vars: EnvVars = env::env_variables();
        let mut socket = self.get_depth_update_socket(url, market);

        loop {
            let read = socket.read();

            match read {
                Ok(message) => {
                    match message {
                        Message::Text(msg) => {
                            let parsed: T = serde_json::from_str(&msg).expect("Can't parse");
                            let ob: DepthUpdate = parsed.into();

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
                    socket = self.get_depth_update_socket(url, market);
                }
            }
        }
    }
}
