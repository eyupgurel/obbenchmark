use std::fmt;
use serde::{de, Deserialize, Deserializer};
use serde::de::{SeqAccess, Visitor};
use bigdecimal::{BigDecimal, ToPrimitive};
use std::str::FromStr;
use rust_decimal::Decimal;
use serde_derive::Serialize;
use thiserror::Error;

// Define a struct for the symbol mappings for each market
#[derive(Deserialize, Debug, Clone)]
pub struct Symbol {
    pub binance: String,
    pub kucoin: String,
    pub bluefin: String,
}

// Define a struct for each market entry
#[derive(Deserialize, Debug, Clone)]
pub struct Market {
    pub name: String,
    pub mm_lot_upper_bound: u128,
    pub lot_size:u128,
    pub min_size:String,
    pub price_precision: i32,
    pub skewing_coefficient:f64,
    pub symbols: Symbol,
}

//Config for Circuit Breakers
#[derive(Deserialize, Debug, Clone, Copy)]
pub struct CircuitBreakerConfig {
    pub num_retries: u8,
    pub failure_threshold: u8
}

// Define the overall structure
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub circuit_breaker_config: CircuitBreakerConfig,
    pub markets: Vec<Market>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderBook {
    pub asks: Vec<(i64, i64)>,
    pub bids: Vec<(i64, i64)>,
}

// Custom visitor to handle a vector of tuples
struct TupleVecVisitor;

impl<'de> Visitor<'de> for TupleVecVisitor {
    type Value = Vec<(i64, i64)>;


    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a vector of price-quantity tuples")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
    {
        let mut vec = Vec::new();
        while let Some((price, quantity)) = seq.next_element::<(String, String)>()? {
            let price_decimal = Decimal::from_str(&price).map_err(de::Error::custom)?;
            let quantity_decimal = Decimal::from_str(&quantity).map_err(de::Error::custom)?;
            let price_i64 = (price_decimal * Decimal::new(10i64.pow(12), 0))
                .to_i64()
                .ok_or_else(|| de::Error::custom("Conversion to i64 failed"))?;
            let quantity_i64 = (quantity_decimal * Decimal::new(10i64.pow(12), 0))
                .to_i64()
                .ok_or_else(|| de::Error::custom("Conversion to i64 failed"))?;
            vec.push((price_i64, quantity_i64));
        }
        Ok(vec)
    }
}

// Deserialize with the custom visitor
pub(crate) fn deserialize_tuple_vec<'de, D>(deserializer: D) -> Result<Vec<(i64, i64)>, D::Error>
    where
        D: Deserializer<'de>,
{
    deserializer.deserialize_seq(TupleVecVisitor)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOrderBook {
    pub last_update_id: u64,
    #[serde(rename = "E")]
    pub e: u64, // Message output time
    #[serde(rename = "T")]
    pub t: u64, // Transaction time
    #[serde(deserialize_with = "deserialize_tuple_vec")]
    pub asks: Vec<(i64, i64)>,
    #[serde(deserialize_with = "deserialize_tuple_vec")]
    pub bids: Vec<(i64, i64)>
}
/*#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DepthUpdate {
    #[serde(rename = "e")]
    event_type: String, // Event type
    #[serde(rename = "E")]
    event_time: u64,    // Event time
    #[serde(rename = "T")]
    transaction_time: u64, // Transaction time
    #[serde(rename = "s")]
    symbol: String,     // Symbol
    #[serde(rename = "U")]
    first_update_id: u64,  // First update ID in event
    #[serde(rename = "u")]
    final_update_id: u64,  // Final update ID in event
    #[serde(rename = "pu")]
    prev_final_update_id: u64, // Final update Id in last stream (i.e., `u` in last stream)
    #[serde(rename = "b")]
    #[serde(deserialize_with = "deserialize_tuple_vec")]
    pub bids: Vec<(i64, i64)>,         // Bids to be updated
    #[serde(rename = "a")]
    #[serde(deserialize_with = "deserialize_tuple_vec")]
    asks: Vec<(i64, i64)>,         // Asks to be updated
}
*/

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DepthUpdate {
    pub stream: String,
    pub data: Data,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Data {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub t: i64,
    pub s: String,
    #[serde(rename = "U")]
    pub bU: i64,
    pub u: i64,
    pub pu: i64,
    #[serde(rename = "b")]
    #[serde(deserialize_with = "deserialize_tuple_vec")]
    pub bids: Vec<(i64, i64)>,

    #[serde(rename = "a")]
    #[serde(deserialize_with = "deserialize_tuple_vec")]
    pub asks: Vec<(i64, i64)>,
}





impl From<DepthUpdate> for OrderBook {
    fn from(depth_update: DepthUpdate) -> Self {
        OrderBook {
            asks: depth_update.data.asks,
            bids: depth_update.data.bids,
        }
    }
}

pub trait BookOperations {
    fn calculate_mid_prices(&self) -> Vec<f64>;
    fn bid_shift(&self, shift:f64) -> Vec<f64>;
    fn ask_shift(&self, shift:f64) -> Vec<f64>;
}





pub fn deserialize_optional_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
    where
        D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) if s.is_empty() => Ok(None),
        Some(s) => s.parse::<f64>().map(Some).map_err(de::Error::custom),
        None => Ok(None),
    }
}

pub fn deserialize_string_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse::<f64>().map_err(de::Error::custom)
}

pub fn deserialize_as_string_tuples<'de, D>(deserializer: D) -> Result<Vec<(f64, f64)>, D::Error>
    where
        D: Deserializer<'de>,
{
    let string_tuples: Vec<(String, String)> = Vec::deserialize(deserializer)?;

    let mut number_tuples: Vec<(f64, f64)> = Vec::with_capacity(string_tuples.len());
    for (s1, s2) in string_tuples {
        let n1 = s1.parse::<f64>().map_err(serde::de::Error::custom)?;
        let n2 = s2.parse::<f64>().map_err(serde::de::Error::custom)?;
        number_tuples.push((n1, n2));
    }

    Ok(number_tuples)
}

pub fn deserialize_as_mix_tuples<'de, D>(deserializer: D) -> Result<Vec<(f64, f64)>, D::Error>
    where
        D: Deserializer<'de>,
{
    struct StringTupleVisitor;

    impl<'de> Visitor<'de> for StringTupleVisitor {
        type Value = Vec<(f64, f64)>;
        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a list of price-amount tuples where the amount can be a string or a number")
        }
        fn visit_seq<S>(self, mut seq: S) -> Result<Vec<(f64, f64)>, S::Error>
            where
                S: SeqAccess<'de>,
        {
            let mut tuples = Vec::new();

            while let Some((price, amount)) = seq.next_element::<(String, serde_json::Value)>()? {
                let price_parsed = price.parse::<f64>().map_err(de::Error::custom)?;
                let amount_parsed = match amount {
                    serde_json::Value::String(s) => s.parse::<f64>().map_err(de::Error::custom)?,
                    serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| de::Error::custom("Invalid number"))?,
                    _ => return Err(de::Error::custom("Invalid type for amount")),
                };
                tuples.push((price_parsed, amount_parsed));
            }

            Ok(tuples)
        }

    }

    deserializer.deserialize_seq(StringTupleVisitor)
}


pub fn deserialize_as_bignumber_string_tuples<'de, D>(deserializer: D) -> Result<Vec<(f64, f64)>, D::Error>
    where D: Deserializer<'de>,
{
    let string_tuples: Vec<(String, String)> = Vec::deserialize(deserializer)?;

    let mut number_tuples: Vec<(f64, f64)> = Vec::with_capacity(string_tuples.len());
    for (s1, s2) in string_tuples {
        let n1 = convert_bignumber_to_f64(&s1).map_err(de::Error::custom)?;
        let n2 = convert_bignumber_to_f64(&s2).map_err(de::Error::custom)?;
        number_tuples.push((n1, n2));
    }

    Ok(number_tuples)
}

pub fn deserialize_to_f64_via_decimal<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let parsed = Decimal::from_str(&s).map_err(serde::de::Error::custom)?;
    let divisor = Decimal::from_str("1000000000000000000").unwrap();
    let decimal_value = parsed / divisor;
    Ok(decimal_value.to_f64().unwrap())
}


pub fn round_to_precision(value: f64, precision: i32) -> f64 {
    let scale = 10f64.powi(precision);
    (value * scale).round() / scale
}
// Define a custom error type
#[derive(Error, Debug)]
enum ConversionError {
    #[error("failed to parse big decimal")]
    BigDecimalParseError,

    #[error("failed to convert to f64")]
    F64ConversionError,
}
fn convert_bignumber_to_f64(bignumber: &str) -> Result<f64, ConversionError> {
    let bd = BigDecimal::from_str(bignumber)
        .map_err(|_| ConversionError::BigDecimalParseError)?;

    let scaled = bd / BigDecimal::from_str("1000000000000000000")
        .map_err(|_| ConversionError::BigDecimalParseError)?;

    scaled.to_f64().ok_or(ConversionError::F64ConversionError)
}
