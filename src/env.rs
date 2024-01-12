use dotenv::dotenv;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt};

// env variable struct
pub struct EnvVars {
    pub bluefin_on_boarding_url: String,
    pub bluefin_websocket_url: String,
    pub bluefin_endpoint: String,
    pub bluefin_wallet_key: String,
    pub bluefin_leverage: u128,
    pub kucoin_on_boarding_url: String,
    pub kucoin_websocket_url: String,
    pub kucoin_endpoint: String,
    pub kucoin_api_key: String,
    pub kucoin_api_secret: String,
    pub kucoin_api_phrase: String,
    pub kucoin_leverage: u128,
    pub kucoin_depth_topic: String,
    pub kucoin_ticker_v2_socket_topic: String,
    pub binance_websocket_url: String,
    pub dry_run: bool,
    pub market_making_trigger_bps: f64,
    pub market_making_time_throttle_period: u64,
    pub log_level: String,
}

/**
 * Method to parse environment variables
 */
#[allow(dead_code)]
pub fn env_variables() -> EnvVars {
    dotenv().ok();

    // bluefin vars
    let bluefin_on_boarding_url =
        std::env::var("BLUEFIN_ON_BOARDING_URL").expect("BLUEFIN_ON_BOARDING_URL must be set.");
    let bluefin_endpoint =
        std::env::var("BLUEFIN_ENDPOINT").expect("BLUEFIN_ENDPOINT must be set.");
    let bluefin_wallet_key =
        std::env::var("BLUEFIN_WALLET_KEY").expect("BLUEFIN_WALLET_KEY must be set.");
    let bluefin_websocket_url =
        std::env::var("BLUEFIN_WEB_SOCKET_URL").expect("BLUEFIN_WEB_SOCKET_URL must be set.");
    let bluefin_leverage = std::env::var("BLUEFIN_LEVERAGE")
        .expect("BLUEFIN_LEVERAGE must be set.")
        .parse::<u128>()
        .unwrap();
    // kucoin vars
    let kucoin_on_boarding_url =
        std::env::var("KUCOIN_ON_BOARDING_URL").expect("KUCOIN_ON_BOARDING_URL must be set.");
    let kucoin_websocket_url =
        std::env::var("KUCOIN_WEB_SOCKET_URL").expect("KUCOIN_WEB_SOCKET_URL must be set.");
    let kucoin_endpoint = std::env::var("KUCOIN_ENDPOINT").expect("KUCOIN_ENDPOINT must be set.");

    let kucoin_api_key = std::env::var("KUCOIN_API_KEY").expect("KUCOIN_API_KEY must be set.");
    let kucoin_api_secret =
        std::env::var("KUCOIN_API_SECRET").expect("KUCOIN_API_SECRET must be set.");

    let kucoin_api_phrase =
        std::env::var("KUCOIN_API_PASSPHRASE").expect("KUCOIN_API_PASSPHRASE must be set.");

    let kucoin_leverage = std::env::var("KUCOIN_LEVERAGE")
        .expect("KUCOIN_LEVERAGE must be set.")
        .parse::<u128>()
        .unwrap();

    let kucoin_depth_topic =
        std::env::var("KUCOIN_DEPTH_SOCKET_TOPIC").expect("KUCOIN_DEPTH_SOCKET_TOPIC must be set.");

    let kucoin_ticker_v2_socket_topic = std::env::var("KUCOIN_TICKER_V2_SOCKET_TOPIC")
        .expect("KUCOIN_TICKER_V2_SOCKET_TOPIC must be set.");

    let binance_websocket_url =
        std::env::var("BINANCE_WEB_SOCKET_URL").expect("BINANCE_WEB_SOCKET_URL must be set.");

    let dry_run = std::env::var("DRY_RUN")
        .expect("DRY_RUN must be set.")
        .parse::<bool>()
        .unwrap();

    let market_making_trigger_bps = std::env::var("MARKET_MAKING_TRIGGER_BPS")
        .expect("MARKET_MAKING_TRIGGER_BPS be set.")
        .parse::<f64>()
        .unwrap();

    let market_making_time_throttle_period = std::env::var("MARKET_MAKING_TIME_THROTTLE_PERIOD")
        .expect("MARKET_MAKING_TIME_THROTTLE_PERIOD be set.")
        .parse::<u64>()
        .unwrap();

    // misc
    let log_level = std::env::var("LOG_LEVEL").expect("LOG_LEVEL must be set.");

    return EnvVars {
        bluefin_on_boarding_url,
        bluefin_endpoint,
        bluefin_wallet_key,
        bluefin_websocket_url,
        bluefin_leverage,
        kucoin_on_boarding_url,
        kucoin_endpoint,
        kucoin_websocket_url,
        kucoin_api_key,
        kucoin_api_secret,
        kucoin_api_phrase,
        kucoin_leverage,
        kucoin_depth_topic,
        kucoin_ticker_v2_socket_topic,
        binance_websocket_url,
        dry_run,
        market_making_trigger_bps,
        market_making_time_throttle_period,
        log_level,
    };
}

/**
 * Initializes logger with provided log level
 */
#[allow(dead_code)]
pub fn init_logger(log_level: String) -> WorkerGuard {
    let filter: tracing::Level = match log_level.as_str() {
        "Error" => tracing::Level::ERROR,
        "Warn" => tracing::Level::WARN,
        "Debug" => tracing::Level::DEBUG,
        "Trace" => tracing::Level::TRACE,
        "Info" | _ => tracing::Level::INFO,
    };

    // TODO: move it to rolling file
    let file_appender = RollingFileAppender::new(Rotation::NEVER, "logs", "chita-bot.log");

    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    tracing::subscriber::set_global_default(
        fmt::Subscriber::builder()
            // subscriber configuration
            .with_max_level(filter)
            .finish()
            // add additional writers
            .with(
                fmt::Layer::default()
                    .json()
                    .flatten_event(true)
                    .with_writer(file_writer),
            ),
    )
    .expect("Unable to set global tracing subscriber");

    tracing::info!("Tracer initialized");

    return guard;
}
