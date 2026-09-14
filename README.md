# NARaz v1.4

NARaz v1.4 is the next functional testnet build, not a screenshot/mockup.

## Included

- NARaz Network HTTP API.
- Separate real-time WebSocket endpoint.
- Persistent SQLite accounts/sessions.
- PBKDF2-HMAC-SHA256 password hashing.
- Automatic account provisioning.
- Native NARaz testnet balance.
- Custom hash-linked NARaz blockchain with genesis block and chain validation.
- Faucet/testnet issuance.
- Wallet/holdings API.
- Live public market data from Binance WebSocket with REST fallback.
- Live order-book endpoint.
- Demo exchange:
  - Market orders against current public market price.
  - Limit orders.
  - User-to-user matching engine.
  - Open orders and cancellation.
  - 0.10% demo fee.
- Responsive NARaz mobile-first interface.
- Network health, chain explorer, wallet, authentication and trading UI.

## Run

```bash
cd backend
python -m venv .venv
# activate the environment
pip install -r requirements.txt
python server.py
```

Frontend can be opened from `frontend/index.html`.

For a phone on the same Wi-Fi, set the Network endpoint in the app to the computer/server LAN address, for example:

`http://192.168.1.10:8080`

## Demo account

Username: `demo`
Password: `demo12345`

New registrations also receive 10,000 USDT demo trading balance and 10,000 NARaz testnet units.

## Important boundary

This build is a functional testnet/demo exchange and development network. It is not yet a legally deployable public financial exchange or production public blockchain. Before real-money/public operation, the project still requires audited cryptography/custody, HTTPS, key management, rate limiting, abuse controls, KYC/AML where applicable, independent security review, operational monitoring, backups, consensus/P2P hardening and legal/regulatory approval.

The code intentionally keeps demo exchange balances separate from the NARaz chain ledger.
