
# Goten 🌀

<div align="center">
  <img src="Goten.png" alt="Goten Logo" width="300"/>
</div>

**Goten** is a Rust-based blockchain event listener and processor specifically designed to interact with the **StarkNet blockchain**. It listens for `BetClaimed` events emitted from smart contracts, parses them, and updates a **PostgreSQL** database accordingly. Goten helps developers monitor and react to smart contract events on StarkNet, providing an efficient way to build real-time data pipelines for decentralized applications.

---

## Features

- **Event Listening**: Monitors `BetClaimed` events on specified StarkNet smart contracts.  
- **Database Updates**:
  - Updates the `bets` table by setting `has_claimed` to `TRUE` and `is_claimable` to `FALSE` for bets matching the `event_address` and `user_address`.
- **Block Synchronization**: Automatically syncs from the last processed block to ensure no events are missed.
- **Efficient Processing**: Handles large volumes of events with robust error handling.
- **Configurable**: Easily adjust RPC endpoints and contract addresses.

---

## Requirements

Before starting, ensure you have the following installed:

- **Rust** (latest stable version)  
- **PostgreSQL** (version 12 or higher)  
- **Docker** (optional, for database setup)  

---

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/AkiraaCorp/goten.git
cd goten
```

### 2. Install Dependencies

Make sure you have Rust installed. If not, install it using `rustup`:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### 3. Set Up Environment Variables

Create a `.env` file in the root directory and add the following:

```env
DATABASE_URL=postgres://username:password@localhost:5432/your_database
RPC_ENDPOINT=https://your-starknet-rpc-endpoint
```

Replace `username`, `password`, and `your_database` with your PostgreSQL credentials.  
Replace `https://your-starknet-rpc-endpoint` with your StarkNet RPC endpoint.

### 4. Set Up the Database

Ensure PostgreSQL is running, and the database specified in `DATABASE_URL` exists. You can create the database using:

```bash
createdb your_database
```

---

## Configuration

- **RPC Endpoint**: Modify the `RPC_ENDPOINT` in your `.env` file to point to the desired StarkNet RPC endpoint.
- **Contract Addresses**: Goten fetches contract addresses from the `events` table in your database where `is_active = true`. Ensure this table is populated with the contracts you want to monitor.

---

## Running the Program

### 1. Build and Run Goten

Use Cargo to build and run the program:

```bash
cargo run
```

### 2. Program Workflow

When you run Goten, it will:

1. **Connect to the Specified RPC Endpoint**: Ensure your RPC provider is accessible.  
2. **Set Up the Database**:
   - Creates necessary tables if they don’t exist.
   - Initializes the `block_state_gotenk` table to track the last processed block.
3. **Fetch Contract Addresses**: Retrieves active contract addresses from the `events` table.
4. **Start Listening for Events**:
   - Processes new blocks starting from the last processed block.
   - Listens for `BetClaimed` events in each block.
5. **Update the Database**:
   - Updates the `bets` table by setting `has_claimed` to `TRUE` and `is_claimable` to `FALSE` for bets matching the `event_address` and `user_address`.
6. **Logging**: Outputs informative logs to the console for monitoring.

---

## Manual Setup for Specific Use Cases

### Changing Contracts

To monitor different contracts, update the `events` table in your PostgreSQL database:

```sql
-- Insert a new contract address
INSERT INTO events (address, is_active)
VALUES ('0xYourContractAddress', TRUE);
```

Replace `'0xYourContractAddress'` with the desired contract address.

### Starting from a Specific Block

To avoid processing from the genesis block, set the `last_processed_block` in the `block_state_gotenk` table:

```sql
UPDATE block_state_gotenk
SET last_processed_block = YOUR_DESIRED_BLOCK_NUMBER
WHERE id = 1;
```

Replace `YOUR_DESIRED_BLOCK_NUMBER` with the block number you want to start from.

---

## Troubleshooting

### Database Connection Error

- Ensure your PostgreSQL instance is running and accessible at the specified `DATABASE_URL`.  
- Check for firewall or connection issues if using a remote PostgreSQL instance.  
- Verify that the database user has the necessary permissions.

### Event Parsing Issues

If event parsing fails, ensure that the event structure in your smart contract matches the parsing logic in Goten.

To enable more verbose logging for debugging, adjust the log level in `main.rs`:

```rust
env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();
```

### RPC Endpoint Errors

- Ensure the RPC endpoint is correct and accessible.  
- Check for network connectivity issues.

---

## License

This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.

---

## Contributing

Feel free to submit issues or pull requests if you have any improvements or suggestions!

---

## Author

**Goten** was developed by [AkiraaCorp].  
For inquiries, contact: [contact@sightbet.com](mailto:contact@sightbet.com)