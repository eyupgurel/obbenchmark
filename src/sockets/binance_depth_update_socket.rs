use crate::models::common::{DepthUpdate};
use crate::sockets::common::{DepthUpdateStream};
use serde::de::DeserializeOwned;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use tungstenite::protocol::Message;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, WebSocket};
use url::Url;

pub struct BinanceDepthUpdateStream<T> {
    phantom: std::marker::PhantomData<T>, // Use PhantomData to indicate the generic type usage
}

impl<T> BinanceDepthUpdateStream<T> {
    pub fn new() -> Self {
        BinanceDepthUpdateStream {
            phantom: std::marker::PhantomData,
        }
    }
}

// Implement OrderBookStream for any type T that meets the trait bounds
impl<T> DepthUpdateStream<T> for BinanceDepthUpdateStream<T>
where
    T: DeserializeOwned + Into<DepthUpdate>,
{
    fn get_depth_update_socket(&self, url: &str, _market: &str) -> WebSocket<MaybeTlsStream<TcpStream>> {
        let (socket, _response) = connect(Url::parse(&url).unwrap()).expect("Can't connect.");

        tracing::info!("Connected to Binance stream at url:{}.", &url);
        return socket;
    }

    fn stream_depth_update_socket(
        &self,
        url: &str,
        market: &str,
        tx: Sender<DepthUpdate>,
    ) {
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
