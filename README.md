# NARaz v1.3

NARaz v1.3 is a direct continuation of v1.2. The existing registration, login, NARaz Network, wallet, transfers, market feed and branded interface remain in place.

## v1.3 exchange layer

- Binance-style professional trading terminal, branded as NARaz.
- Real-time public market stream: Binance WebSocket is the primary continuous source for ticker, depth and 1h kline updates; REST is only bootstrap/fallback.
- BTC/USDT, ETH/USDT, BNB/USDT and SOL/USDT trading pairs.
- Live 1h market candles when Binance public klines are reachable.
- Real-time Binance order book stream when the NARaz Network server has internet access.
- Market orders on the NARaz exchange testnet use the current live external market price without artificial price impact.
- Limit orders and a NARaz testnet matching engine between authenticated users.
- Open-order list and cancellation.
- Available vs locked exchange balances.
- 0.10% testnet trading fee.
- Exchange state is intentionally separated from the NARaz on-chain ledger until a production custody/settlement design is approved.

## Network

The backend exposes the HTTP API on `NARAZ_PORT` (default 8080) and the NARaz real-time WebSocket on `NARAZ_WS_PORT` (default HTTP port + 1). The Android client receives the WebSocket port from `/api/network` and subscribes automatically. The server must have outbound internet access to reach Binance's public WebSocket.

## Important

v1.3 is a functional exchange **testnet foundation**, not a production Binance replacement. It does not claim real external liquidity, real-money settlement, custody, KYC/AML, or production-grade matching-engine guarantees. Those require dedicated infrastructure, security review, legal work, and audited blockchain/wallet components.

The design goal is to build a NARaz platform that can eventually exceed the user experience and transparency of large exchanges without copying their branding.
