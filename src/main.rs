use dotenv::dotenv;
use env_logger::Env;
use log::{error, info};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use sqlx::postgres::PgRow;
use sqlx::Row;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use starknet::core::types::{BlockId, EventFilter, Felt};
use starknet::core::utils::get_selector_from_name;
use starknet::providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider};
use std::env;
use std::time::Duration;
use url::Url;

#[derive(Debug)]
struct BetClaimedEvent {
    event_address: String,
    user_address: String,
    amount_claimed: BigUint,
    event_outcome: u8,
    timestamp: u64,
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let rpc_endpoint = env::var("RPC_ENDPOINT").expect("RPC_ENDPOINT must be set");
    let rpc_url = Url::parse(&rpc_endpoint).expect("Invalid RPC URL");

    let transport = HttpTransport::new(rpc_url);
    let provider = JsonRpcClient::new(transport);

    let pool = setup_database().await;

    
    loop {
        let contract_addresses = fetch_contract_addresses(&pool).await;
        process_new_events(&provider, &contract_addresses, &pool).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn setup_database() -> Pool<Postgres> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to create database pool");

    setup_block_state_goten(&pool).await;

    pool
}

async fn setup_block_state_goten(pool: &Pool<Postgres>) {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS block_state_goten (
            id INTEGER PRIMARY KEY,
            last_processed_block BIGINT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .expect("Failed to create block_state_goten table");

    sqlx::query(
        "INSERT INTO block_state_goten (id, last_processed_block)
         VALUES (1, 0)
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await
    .expect("Failed to initialize block_state_goten");
}

async fn fetch_contract_addresses(pool: &Pool<Postgres>) -> Vec<Felt> {
    let contract_addresses: Vec<Felt> =
        sqlx::query("SELECT address FROM events WHERE is_active = false")
            .map(|row: PgRow| {
                let address: String = row.get("address");
                Felt::from_hex(&address).expect("Invalid Felt")
            })
            .fetch_all(pool)
            .await
            .expect("Failed to fetch contract addresses");

    contract_addresses
}

async fn process_new_events(
    provider: &JsonRpcClient<HttpTransport>,
    contract_addresses: &[Felt],
    pool: &Pool<Postgres>,
) {
    let last_processed_block = get_last_processed_block(pool).await;
    let latest_block = provider
        .block_number()
        .await
        .expect("Failed to get latest block number");

    info!("Last processed block: {}", last_processed_block);
    info!("Latest block: {}", latest_block);

    if latest_block > last_processed_block {
        info!(
            "🔀 Processing blocks from {} to {}",
            last_processed_block + 1,
            latest_block
        );
        for block_number in (last_processed_block + 1)..=latest_block {
            for &contract_address in contract_addresses {
                process_block(provider, block_number, contract_address, pool).await;
            }
            update_last_processed_block(pool, block_number).await;
        }
    } else {
        info!("📡 No new blocks to process.");
    }
}

async fn get_last_processed_block(pool: &Pool<Postgres>) -> u64 {
    let row: (i64,) =
        sqlx::query_as("SELECT last_processed_block FROM block_state_goten WHERE id = 1")
            .fetch_one(pool)
            .await
            .expect("Failed to fetch last_processed_block");

    row.0 as u64
}

async fn update_last_processed_block(pool: &Pool<Postgres>, block_number: u64) {
    if let Err(e) =
        sqlx::query("UPDATE block_state_goten SET last_processed_block = $1 WHERE id = 1")
            .bind(block_number as i64)
            .execute(pool)
            .await
    {
        error!("Failed to update last_processed_block: {}", e);
    }
}

async fn process_block(
    provider: &JsonRpcClient<HttpTransport>,
    block_number: u64,
    contract_address: Felt,
    pool: &Pool<Postgres>,
) {
    info!(
        "Fetching BetClaimed events for block {} on contract {}",
        block_number, contract_address
    );

    let filter = EventFilter {
        from_block: Some(BlockId::Number(block_number)),
        to_block: Some(BlockId::Number(block_number)),
        address: Some(contract_address),
        keys: Some(vec![vec![bet_claimed_event_key()]]),
    };

    let chunk_size = 100;
    let events_page = match provider.get_events(filter, None, chunk_size).await {
        Ok(page) => page,
        Err(err) => {
            error!("Error fetching events: {}", err);
            return;
        }
    };

    info!(
        "Number of BetClaimed events fetched: {}",
        events_page.events.len()
    );

    if events_page.events.is_empty() {
        info!(
            "No BetClaimed events found for block {} on contract {}",
            block_number, contract_address
        );
    }

    for event in events_page.events {
        let data = event.data.clone();

        if let Some(bet_claimed_event) = parse_bet_claimed_event(&data) {
            info!("✨ New BetClaimed event: {:?}", bet_claimed_event);
            update_database_for_bet_claimed(bet_claimed_event, pool).await;
        } else {
            error!("❌ Failed to parse BetClaimed event with data: {:?}", data);
        }
    }
}

fn bet_claimed_event_key() -> Felt {
    get_selector_from_name("Claim").expect("Failed to compute event selector")
}

fn parse_bet_claimed_event(data: &[Felt]) -> Option<BetClaimedEvent> {
    if data.len() >= 5 {
        let event_address = format_address(&data[0].to_fixed_hex_string());
        let user_address = format_address(&data[1].to_fixed_hex_string());
        let amount_claimed = data[2].to_biguint();
        let event_outcome = data[3].to_u8().unwrap_or(0);
        let timestamp = data[4].to_u64().unwrap_or(0);

        Some(BetClaimedEvent {
            event_address,
            user_address,
            amount_claimed,
            event_outcome,
            timestamp,
        })
    } else {
        None
    }
}

async fn update_database_for_bet_claimed(event: BetClaimedEvent, pool: &Pool<Postgres>) {
    let result = sqlx::query(
        "UPDATE bets SET has_claimed = TRUE, is_claimable = FALSE
        WHERE \"event_address\" = $1 AND user_address = $2",
    )
    .bind(&event.event_address)
    .bind(&event.user_address)
    .execute(pool)
    .await;

    if let Err(e) = result {
        error!("Failed to update bets table: {}", e);
    } else {
        info!(
            "Updated bets table for event_address: {} and user_address: {}",
            event.event_address, event.user_address
        );
    }
}

fn format_address(address: &str) -> String {
    let hex_str = if address.starts_with("0x") {
        &address[2..]
    } else {
        address
    };
    format!("0x{:0>64}", hex_str)
}
