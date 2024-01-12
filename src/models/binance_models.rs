use serde::{Serialize, Deserialize};
use crate::models::common::OrderBook;
use crate::models::common::deserialize_as_string_tuples;
#[derive(Debug, Serialize, Deserialize)]
pub struct DepthUpdate {
    #[serde(rename = "e")]
    pub event_type: String,

    #[serde(rename = "E")]
    pub event_time: i64,

    #[serde(rename = "T")]
    pub transaction_time: i64,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "U")]
    pub u_id: i64, // First update ID in event

    #[serde(rename = "u")]
    pub u2_id: i64,  // Final update ID in event

    #[serde(rename = "pu")]
    pub pu_id: i64,  // Final update Id in last stream(ie `u` in last stream)

    #[serde(rename = "b")]
    #[serde(deserialize_with = "deserialize_as_string_tuples")]
    pub bid_orders: Vec<(f64, f64)>,

    #[serde(rename = "a")]
    #[serde(deserialize_with = "deserialize_as_string_tuples")]
    pub ask_orders: Vec<(f64, f64)>,
}

impl From<DepthUpdate> for OrderBook {
    fn from(depth_update: DepthUpdate) -> Self {
        OrderBook {
            asks: depth_update.ask_orders,
            bids: depth_update.bid_orders,
        }
    }
}