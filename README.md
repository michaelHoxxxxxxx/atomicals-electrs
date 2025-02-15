# Atomical-ElectrumX Server in Rust

An efficient re-implementation of Atomicals ElectrumX Server in Rust, providing high-performance indexing and querying capabilities for Atomicals on the Bitcoin network. This project is a fork of the original [electrs](https://github.com/romanz/electrs) project, enhanced with Atomicals protocol support.

## Features

- **Atomicals Protocol Support**
  - Full support for NFT and FT (Fungible Token) Atomicals
  - Comprehensive indexing of Atomicals operations and states
  - Real-time tracking of Atomicals ownership and transfers
  - Support for sealed and unsealed Atomicals

- **High Performance**
  - Efficient RocksDB-based storage with optimized column families
  - Fast synchronization and indexing of Atomicals data
  - Low memory footprint and CPU usage
  - Concurrent processing of Atomicals operations

- **Rich API Support**
  - Full Electrum protocol compatibility
  - Extended RPC interface for Atomicals operations
  - Real-time WebSocket notifications
  - Comprehensive query capabilities for Atomicals data

- **Advanced Features**
  - Mempool tracking for unconfirmed Atomicals operations
  - Support for complex Atomicals queries and filters
  - Metadata indexing and search capabilities
  - Chain reorganization handling

## Architecture

The server is built on a modular architecture with the following key components:

1. **Core Components**
   - Chain synchronization and block processing
   - Transaction parsing and validation
   - State management and persistence
   - Memory pool handling

2. **Atomicals-Specific Components**
   - Atomicals operation parser
   - State and ownership tracking
   - Index management
   - RPC interface

3. **Storage Layer**
   - Optimized RocksDB schema
   - Efficient index structures
   - Cache management
   - Data consistency guarantees

## Getting Started

### Prerequisites

- Rust 1.70 or later
- Bitcoin Core 24.0 or later
- RocksDB 6.20.3 or later

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/atomical-electrs.git
   cd atomical-electrs
   ```

2. Build the project:
   ```bash
   cargo build --release
   ```

3. Configure the server:
   ```bash
   cp config.example.toml config.toml
   # Edit config.toml with your settings
   ```

4. Run the server:
   ```bash
   ./target/release/electrs --conf config.toml
   ```

## Configuration

The server can be configured through both command-line arguments and a configuration file. Key configuration options include:

- Bitcoin RPC connection settings
- Network selection (mainnet, testnet, regtest)
- Database location and cache sizes
- Logging and monitoring options
- RPC and WebSocket interface settings

## Documentation

Detailed documentation is available in the `docs` directory:

- [Installation Guide](doc/install.md)
- [Configuration Guide](doc/config.md)
- [API Reference](doc/api.md)
- [Architecture Overview](doc/architecture.md)

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) before submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Original [electrs](https://github.com/romanz/electrs) project
- [Atomicals Protocol](https://atomicals.xyz) specification
- Bitcoin Core team
- Rust and RocksDB communities

## Status

This project is under active development. Please check the [Issues](https://github.com/yourusername/atomical-electrs/issues) page for known issues and planned features.
