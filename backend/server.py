import hashlib, json, os, secrets, sqlite3, time, math, threading, asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen

try:
    import websocket
except ImportError:
    websocket = None
try:
    import websockets
except ImportError:
    websockets = None

DB_PATH = os.environ.get('NARAZ_DB', os.path.join(os.path.dirname(__file__), 'naraz.db'))
HOST = os.environ.get('NARAZ_HOST', '0.0.0.0')
PORT = int(os.environ.get('NARAZ_PORT', '8080'))
WS_PORT = int(os.environ.get('NARAZ_WS_PORT', str(PORT + 1)))
TOKEN = 'NARaz'
MAX_SUPPLY = 1_000_000_000
TESTNET_GRANT = 1_000.0
DEMO_GRANT = 10_000.0
MARKET_CACHE = {'at': 0.0, 'data': None}
LIVE_MARKET = {'items': {}, 'books': {}, 'updated_at': 0.0, 'connected': False, 'source': 'Binance WebSocket'}
MARKET_LOCK = threading.RLock()
WS_CLIENTS = set()
WS_LOOP = None

# v1.3 exchange-testnet defaults. These are deliberately isolated from the NARaz on-chain ledger.
QUOTE_ASSET = 'USDT'
EXCHANGE_FEE = 0.001
SUPPORTED_PAIRS = ('BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT')


def db():
    c = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA busy_timeout=10000')
    return c


def now(): return time.time()


def hash_password(password, salt=None):
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 180_000)
    return salt.hex() + '$' + digest.hex()


def verify_password(password, stored):
    try:
        salt_hex, digest_hex = stored.split('$', 1)
        digest = hashlib.pbkdf2_hmac('sha256', password.encode(), bytes.fromhex(salt_hex), 180_000)
        return secrets.compare_digest(digest.hex(), digest_hex)
    except Exception:
        return False


def wallet_address(): return 'NAR1' + secrets.token_hex(20)


def init_db():
    c = db()
    c.executescript('''
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE COLLATE NOCASE,
      email TEXT NOT NULL UNIQUE COLLATE NOCASE,
      password_hash TEXT NOT NULL,
      wallet TEXT NOT NULL UNIQUE,
      created_at REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      user_id INTEGER NOT NULL,
      created_at REAL NOT NULL,
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS blocks (
      idx INTEGER PRIMARY KEY,
      timestamp REAL NOT NULL,
      transactions TEXT NOT NULL,
      previous_hash TEXT NOT NULL,
      nonce INTEGER NOT NULL,
      hash TEXT NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS pending (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sender TEXT NOT NULL,
      recipient TEXT NOT NULL,
      amount REAL NOT NULL,
      timestamp REAL NOT NULL,
      tx_id TEXT NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS demo_holdings (
      user_id INTEGER NOT NULL,
      asset TEXT NOT NULL,
      quantity REAL NOT NULL DEFAULT 0,
      avg_price_usd REAL NOT NULL DEFAULT 0,
      PRIMARY KEY(user_id, asset),
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS demo_trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      asset TEXT NOT NULL,
      side TEXT NOT NULL,
      quantity REAL NOT NULL,
      price_usd REAL NOT NULL,
      value_usd REAL NOT NULL,
      timestamp REAL NOT NULL,
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS exchange_orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      symbol TEXT NOT NULL,
      side TEXT NOT NULL,
      order_type TEXT NOT NULL,
      price REAL,
      quantity REAL NOT NULL,
      remaining REAL NOT NULL,
      status TEXT NOT NULL,
      avg_fill_price REAL NOT NULL DEFAULT 0,
      filled REAL NOT NULL DEFAULT 0,
      fee REAL NOT NULL DEFAULT 0,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL,
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS exchange_reservations (
      user_id INTEGER NOT NULL,
      asset TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      PRIMARY KEY(user_id, asset),
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS exchange_trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT NOT NULL,
      price REAL NOT NULL,
      quantity REAL NOT NULL,
      taker_side TEXT NOT NULL,
      buy_order_id INTEGER,
      sell_order_id INTEGER,
      buyer_user_id INTEGER,
      seller_user_id INTEGER,
      fee REAL NOT NULL DEFAULT 0,
      timestamp REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_exchange_orders_book ON exchange_orders(symbol, side, status, price, created_at);
    CREATE INDEX IF NOT EXISTS idx_exchange_orders_user ON exchange_orders(user_id, status, created_at);
    CREATE INDEX IF NOT EXISTS idx_exchange_trades_symbol ON exchange_trades(symbol, timestamp);
    ''')
    if c.execute('SELECT COUNT(*) n FROM blocks').fetchone()['n'] == 0:
        genesis = {
            'index': 0,
            'timestamp': now(),
            'transactions': [{'id':'GENESIS','sender':'SYSTEM','recipient':'TESTNET_FAUCET','amount':MAX_SUPPLY}],
            'previous_hash': '0', 'nonce': 0,
        }
        genesis['hash'] = block_hash(genesis)
        c.execute('INSERT INTO blocks VALUES (?,?,?,?,?,?)', (0, genesis['timestamp'], json.dumps(genesis['transactions']), '0', 0, genesis['hash']))
    seed_demo(c, 'demo_alice', 'demo-alice@naraz.local', 'demo12345')
    seed_demo(c, 'demo_bob', 'demo-bob@naraz.local', 'demo12345')
    c.commit(); c.close()


def seed_demo(c, username, email, password):
    row = c.execute('SELECT id FROM users WHERE username=? COLLATE NOCASE', (username,)).fetchone()
    if row:
        # v1.3 gives existing demo users an exchange quote balance once.
        if not c.execute("SELECT 1 FROM demo_holdings WHERE user_id=? AND asset='USDT'", (row['id'],)).fetchone():
            c.execute("INSERT INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)", (row['id'], 'USDT', 10000.0, 1.0))
        return
    wallet = wallet_address()
    c.execute('INSERT INTO users(username,email,password_hash,wallet,created_at) VALUES(?,?,?,?,?)',
              (username, email, hash_password(password), wallet, now()))
    user_id = c.execute('SELECT id FROM users WHERE username=? COLLATE NOCASE', (username,)).fetchone()['id']
    c.execute("INSERT OR REPLACE INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)", (user_id, 'USD', 10000.0, 1.0))
    c.execute("INSERT OR REPLACE INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)", (user_id, 'USDT', 10000.0, 1.0))
    tx = make_tx('TESTNET_FAUCET', wallet, DEMO_GRANT)
    c.execute('INSERT INTO pending(sender,recipient,amount,timestamp,tx_id) VALUES(?,?,?,?,?)',
              (tx['sender'], tx['recipient'], tx['amount'], tx['timestamp'], tx['id']))
    mine_pending(c)


def block_hash(b):
    raw = json.dumps({k:b[k] for k in ['index','timestamp','transactions','previous_hash','nonce']}, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(raw).hexdigest()


def chain_rows(c): return c.execute('SELECT * FROM blocks ORDER BY idx').fetchall()


def balance(c, address):
    total = 0.0
    for r in chain_rows(c):
        for tx in json.loads(r['transactions']):
            if tx['sender'] == address: total -= float(tx['amount'])
            if tx['recipient'] == address: total += float(tx['amount'])
    for r in c.execute('SELECT sender,recipient,amount FROM pending'):
        if r['sender'] == address: total -= float(r['amount'])
        if r['recipient'] == address: total += float(r['amount'])
    return round(total, 8)


def supply(c):
    total = 0.0
    for r in chain_rows(c):
        for tx in json.loads(r['transactions']):
            if tx['sender'] == 'SYSTEM': total += float(tx['amount'])
    return round(total, 8)


def make_tx(sender, recipient, amount):
    return {'id': secrets.token_hex(16), 'sender':sender, 'recipient':recipient, 'amount':float(amount), 'timestamp':now()}


def mine_pending(c):
    rows = c.execute('SELECT * FROM pending ORDER BY id').fetchall()
    if not rows: return None
    txs = [dict(id=r['tx_id'], sender=r['sender'], recipient=r['recipient'], amount=r['amount'], timestamp=r['timestamp']) for r in rows]
    idx = c.execute('SELECT MAX(idx) m FROM blocks').fetchone()['m'] + 1
    prev = c.execute('SELECT hash FROM blocks WHERE idx=?', (idx-1,)).fetchone()['hash']
    b = {'index':idx,'timestamp':now(),'transactions':txs,'previous_hash':prev,'nonce':0}
    b['hash'] = block_hash(b)
    c.execute('INSERT INTO blocks VALUES (?,?,?,?,?,?)', (idx,b['timestamp'],json.dumps(txs),prev,0,b['hash']))
    c.execute('DELETE FROM pending'); c.commit(); return b


def auth_user(c, headers):
    token = headers.get('Authorization','')
    if not token.startswith('Bearer '): return None
    return c.execute('SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token=?', (token[7:].strip(),)).fetchone()


def body(h):
    n = int(h.headers.get('Content-Length','0'))
    try: return json.loads(h.rfile.read(n) or b'{}')
    except json.JSONDecodeError: raise ValueError('Invalid JSON')


def http_json(url, timeout=7):
    req = Request(url, headers={'User-Agent':'NARaz/1.3'})
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


BINANCE_STREAM_SYMBOLS = {'BTCUSDT':('BTC','Bitcoin'),'ETHUSDT':('ETH','Ethereum'),'BNBUSDT':('BNB','BNB'),'SOLUSDT':('SOL','Solana')}


def _broadcast_payload(payload):
    if not websockets or not WS_CLIENTS or WS_LOOP is None:
        return
    msg = json.dumps(payload, ensure_ascii=False)
    async def send_all():
        dead=[]
        for ws in list(WS_CLIENTS):
            try:
                await ws.send(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            WS_CLIENTS.discard(ws)
    try:
        asyncio.run_coroutine_threadsafe(send_all(), WS_LOOP)
    except Exception:
        pass


def _binance_stream_worker():
    if websocket is None:
        return
    streams=[]
    for sym in BINANCE_STREAM_SYMBOLS:
        s=sym.lower()
        streams += [f'{s}@ticker', f'{s}@depth20@100ms', f'{s}@kline_1h']
    url='wss://stream.binance.com:9443/stream?streams=' + '/'.join(streams)
    while True:
        try:
            ws=websocket.create_connection(url, timeout=10, enable_multithread=True)
            with MARKET_LOCK:
                LIVE_MARKET['connected']=True
            while True:
                raw=ws.recv()
                if not raw: break
                msg=json.loads(raw)
                data=msg.get('data',{})
                stream=msg.get('stream','')
                if data.get('e') == '24hrTicker':
                    symbol=data.get('s')
                    if symbol in BINANCE_STREAM_SYMBOLS:
                        sym,name=BINANCE_STREAM_SYMBOLS[symbol]
                        item=LIVE_MARKET['items'].get(sym, {'symbol':sym,'name':name,'change_24h':0,'source':'Binance WebSocket'})
                        item['price_usd']=float(data['c']); item['change_24h']=float(data.get('P') or 0); item['updated_at']=now(); item['source']='Binance WebSocket'
                        with MARKET_LOCK:
                            LIVE_MARKET['items'][sym]=item; LIVE_MARKET['updated_at']=now()
                        _broadcast_payload({'type':'ticker','item':item})
                elif data.get('e') == 'depthUpdate':
                    symbol=data.get('s')
                    if symbol in BINANCE_STREAM_SYMBOLS:
                        sym,_=BINANCE_STREAM_SYMBOLS[symbol]
                        book={'symbol':sym,'bids':[[float(p),float(q)] for p,q in data.get('b',[])], 'asks':[[float(p),float(q)] for p,q in data.get('a',[])], 'updated_at':now(), 'source':'Binance WebSocket'}
                        with MARKET_LOCK:
                            LIVE_MARKET['books'][sym]=book; LIVE_MARKET['updated_at']=now()
                        _broadcast_payload({'type':'depth','symbol':sym,'book':book})
                elif data.get('e') == 'kline':
                    symbol=data.get('s')
                    if symbol in BINANCE_STREAM_SYMBOLS:
                        sym,_=BINANCE_STREAM_SYMBOLS[symbol]
                        k=data.get('k',{})
                        candle={'time':int(k.get('t',0)),'open':float(k.get('o',0)),'high':float(k.get('h',0)),'low':float(k.get('l',0)),'close':float(k.get('c',0)),'volume':float(k.get('v',0)),'closed':bool(k.get('x'))}
                        _broadcast_payload({'type':'candle','symbol':sym,'interval':k.get('i'),'candle':candle})
        except Exception:
            with MARKET_LOCK:
                LIVE_MARKET['connected']=False
            time.sleep(3)


def _start_market_stream():
    if websocket is None:
        return
    t=threading.Thread(target=_binance_stream_worker, name='naraz-binance-ws', daemon=True)
    t.start()


async def _ws_handler(websocket_conn):
    WS_CLIENTS.add(websocket_conn)
    try:
        with MARKET_LOCK:
            snapshot={'type':'snapshot','connected':LIVE_MARKET['connected'],'items':list(LIVE_MARKET['items'].values()),'books':LIVE_MARKET['books'],'updated_at':LIVE_MARKET['updated_at']}
        await websocket_conn.send(json.dumps(snapshot, ensure_ascii=False))
        async for _ in websocket_conn:
            pass
    finally:
        WS_CLIENTS.discard(websocket_conn)


def _start_naraz_ws_server():
    if websockets is None:
        return
    async def runner():
        async with websockets.serve(_ws_handler, '0.0.0.0', WS_PORT):
            await asyncio.Future()
    def thread_main():
        global WS_LOOP
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        WS_LOOP = loop
        loop.run_until_complete(runner())
        loop.run_forever()
    threading.Thread(target=thread_main, name='naraz-network-ws', daemon=True).start()


def market_data():
    with MARKET_LOCK:
        items = [dict(v) for v in LIVE_MARKET['items'].values()]
        stream_connected = LIVE_MARKET['connected']
    # Bootstrap from Binance REST if the WebSocket has not populated prices yet.
    if not items:
        try:
            data = http_json('https://api.binance.com/api/v3/ticker/24hr', 6)
            for row in data:
                if row.get('symbol') in BINANCE_STREAM_SYMBOLS:
                    sym, name = BINANCE_STREAM_SYMBOLS[row['symbol']]
                    items.append({'symbol':sym,'name':name,'price_usd':float(row['lastPrice']),'change_24h':float(row['priceChangePercent']),'source':'Binance REST bootstrap','updated_at':now()})
            with MARKET_LOCK:
                for x in items: LIVE_MARKET['items'][x['symbol']] = x
        except Exception:
            pass
    if not items:
        try:
            cg = http_json('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,tether,binancecoin,solana&vs_currencies=usd&include_24hr_change=true')
            for asset_id, symbol, name in [('bitcoin','BTC','Bitcoin'),('ethereum','ETH','Ethereum'),('tether','USDT','Tether'),('binancecoin','BNB','BNB'),('solana','SOL','Solana')]:
                d = cg.get(asset_id, {})
                if 'usd' in d: items.append({'symbol':symbol,'name':name,'price_usd':d['usd'],'change_24h':d.get('usd_24h_change') or 0,'source':'CoinGecko fallback','updated_at':now()})
        except Exception:
            pass
    # USDT remains a stable reference asset rather than a Binance trade stream symbol.
    if not any(x.get('symbol') == 'USDT' for x in items):
        items.append({'symbol':'USDT','name':'Tether','price_usd':1.0,'change_24h':0.0,'source':'Binance market reference','updated_at':now()})
    try:
        gold = http_json('https://api.gold-api.com/price/XAU', 6)
        price = gold.get('price') or gold.get('price_usd')
        if price is not None: items.insert(0, {'symbol':'XAU','name':'Gold','price_usd':float(price),'change_24h':float(gold.get('change_percent') or 0),'source':'Gold API','updated_at':now()})
    except Exception:
        pass
    azn = None
    try:
        fx = http_json('https://open.er-api.com/v6/latest/USD', 6)
        azn = float(fx.get('rates',{}).get('AZN'))
    except Exception:
        pass
    if azn:
        items.append({'symbol':'USD/AZN','name':'US Dollar / Manat','price_usd':azn,'price_azn':azn,'change_24h':0,'source':'ExchangeRate-API','updated_at':now()})
        for x in items:
            if x['symbol'] != 'USD/AZN': x['price_azn'] = x['price_usd'] * azn
    result = {'items':items,'usd_azn':azn,'updated_at':now(),'live':bool(items),'realtime':stream_connected,'source':'Binance WebSocket real-time market stream' if stream_connected else 'Binance REST bootstrap / fallback'}
    MARKET_CACHE.update({'at':now(),'data':result})
    return result

def live_price(symbol):
    m = market_data()
    if symbol.endswith('/USDT'):
        base = symbol.split('/')[0]
        x = next((i for i in m['items'] if i['symbol']==base), None)
        if x: return float(x['price_usd'])
    return None


def exchange_wallet(c, user_id, asset):
    row = c.execute('SELECT quantity FROM demo_holdings WHERE user_id=? AND asset=?', (user_id, asset)).fetchone()
    return float(row['quantity']) if row else 0.0

def reserved(c, user_id, asset):
    row=c.execute('SELECT amount FROM exchange_reservations WHERE user_id=? AND asset=?',(user_id,asset)).fetchone()
    return float(row['amount']) if row else 0.0

def available(c, user_id, asset):
    return max(0.0, exchange_wallet(c,user_id,asset)-reserved(c,user_id,asset))

def reserve(c,user_id,asset,amount):
    current=reserved(c,user_id,asset)
    if amount < 0 or available(c,user_id,asset) + 1e-12 < amount: raise ValueError(f'Insufficient available {asset} balance')
    c.execute('INSERT OR REPLACE INTO exchange_reservations(user_id,asset,amount) VALUES(?,?,?)',(user_id,asset,current+amount))

def release(c,user_id,asset,amount):
    current=reserved(c,user_id,asset)
    c.execute('INSERT OR REPLACE INTO exchange_reservations(user_id,asset,amount) VALUES(?,?,?)',(user_id,asset,max(0,current-amount)))


def set_exchange_wallet(c, user_id, asset, qty, avg=0.0):
    c.execute('INSERT OR REPLACE INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)', (user_id,asset,max(0.0,qty),avg))


def pair_assets(symbol):
    base, quote = symbol.split('/')
    return base, quote


def fee_for(value): return round(value * EXCHANGE_FEE, 10)


def execute_virtual_market(c, u, symbol, side, quantity):
    price = live_price(symbol)
    if price is None: raise ValueError('Live market is unavailable')
    base, quote = pair_assets(symbol)
    # Fill at the current real external market price. The settlement remains inside NARaz Exchange Testnet.
    fill_price = price
    value = fill_price * quantity
    fee = fee_for(value)
    qbal = exchange_wallet(c, u['id'], quote)
    bbal = exchange_wallet(c, u['id'], base)
    if side == 'BUY':
        required = value + fee
        if qbal < required: raise ValueError(f'Insufficient {quote} balance')
        set_exchange_wallet(c, u['id'], quote, qbal-required, 1.0)
        set_exchange_wallet(c, u['id'], base, bbal+quantity, fill_price)
    else:
        if bbal < quantity: raise ValueError(f'Insufficient {base} balance')
        set_exchange_wallet(c, u['id'], base, bbal-quantity, fill_price)
        set_exchange_wallet(c, u['id'], quote, qbal+value-fee, 1.0)
    oid = c.execute('INSERT INTO exchange_orders(user_id,symbol,side,order_type,price,quantity,remaining,status,avg_fill_price,filled,fee,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (u['id'],symbol,side,'MARKET',fill_price,quantity,0,'FILLED',fill_price,quantity,fee,now(),now())).lastrowid
    c.execute('INSERT INTO exchange_trades(symbol,price,quantity,taker_side,buy_order_id,sell_order_id,buyer_user_id,seller_user_id,fee,timestamp) VALUES(?,?,?,?,?,?,?,?,?,?)',
              (symbol,fill_price,quantity,side,oid if side=='BUY' else None,oid if side=='SELL' else None,u['id'] if side=='BUY' else None,u['id'] if side=='SELL' else None,fee,now()))
    return {'order_id':oid,'symbol':symbol,'side':side,'type':'MARKET','status':'FILLED','price':fill_price,'quantity':quantity,'filled':quantity,'fee':fee,'value':value}


def place_limit(c, u, symbol, side, price, quantity):
    if symbol not in SUPPORTED_PAIRS: raise ValueError('Unsupported trading pair')
    if price <= 0 or quantity <= 0: raise ValueError('Price and quantity must be positive')
    base, quote = pair_assets(symbol)
    if side == 'BUY': reserve(c,u['id'],quote,price*quantity*(1+EXCHANGE_FEE))
    else: reserve(c,u['id'],base,quantity)
    oid=c.execute('INSERT INTO exchange_orders(user_id,symbol,side,order_type,price,quantity,remaining,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                  (u['id'],symbol,side,'LIMIT',price,quantity,quantity,'OPEN',now(),now())).lastrowid
    match_limit_orders(c,symbol)
    row=c.execute('SELECT * FROM exchange_orders WHERE id=?',(oid,)).fetchone()
    return dict(row)

def match_limit_orders(c, symbol):
    # Price-time matching between real user limit orders on this testnet.
    while True:
        buy=c.execute("SELECT * FROM exchange_orders WHERE symbol=? AND side='BUY' AND status='OPEN' ORDER BY price DESC, created_at ASC, id ASC LIMIT 1",(symbol,)).fetchone()
        sell=c.execute("SELECT * FROM exchange_orders WHERE symbol=? AND side='SELL' AND status='OPEN' ORDER BY price ASC, created_at ASC, id ASC LIMIT 1",(symbol,)).fetchone()
        if not buy or not sell or float(buy['price']) + 1e-12 < float(sell['price']): return
        qty=min(float(buy['remaining']),float(sell['remaining']))
        price=float(sell['price'])
        base,quote=pair_assets(symbol)
        buyer=buy['user_id']; seller=sell['user_id']
        gross=price*qty; fee=fee_for(gross)
        # Buyer had reserved at its limit price; seller had reserved base quantity.
        release(c,buyer,quote,min(reserved(c,buyer,quote), float(buy['price'])*qty*(1+EXCHANGE_FEE)))
        release(c,seller,base,qty)
        bbase=exchange_wallet(c,buyer,base); bquote=exchange_wallet(c,buyer,quote)
        sbase=exchange_wallet(c,seller,base); squote=exchange_wallet(c,seller,quote)
        # Refund buyer's price improvement after charging actual execution + fee.
        buyer_reserved_for_fill=float(buy['price'])*qty*(1+EXCHANGE_FEE)
        buyer_actual=gross+fee
        refund=max(0,buyer_reserved_for_fill-buyer_actual)
        set_exchange_wallet(c,buyer,base,bbase+qty,price)
        set_exchange_wallet(c,buyer,quote,bquote-refund,1.0)
        set_exchange_wallet(c,seller,base,sbase-qty,price)
        set_exchange_wallet(c,seller,quote,squote+gross-fee,1.0)
        br=max(0,float(buy['remaining'])-qty); sr=max(0,float(sell['remaining'])-qty)
        bfilled=float(buy['filled'])+qty; sfilled=float(sell['filled'])+qty
        c.execute("UPDATE exchange_orders SET remaining=?,filled=?,avg_fill_price=?,status=?,fee=fee+?,updated_at=? WHERE id=?",(br,bfilled,price,'FILLED' if br<=1e-12 else 'PARTIAL',fee,now(),buy['id']))
        c.execute("UPDATE exchange_orders SET remaining=?,filled=?,avg_fill_price=?,status=?,fee=fee+?,updated_at=? WHERE id=?",(sr,sfilled,price,'FILLED' if sr<=1e-12 else 'PARTIAL',fee,now(),sell['id']))
        c.execute('INSERT INTO exchange_trades(symbol,price,quantity,taker_side,buy_order_id,sell_order_id,buyer_user_id,seller_user_id,fee,timestamp) VALUES(?,?,?,?,?,?,?,?,?,?)',(symbol,price,qty,'LIMIT',buy['id'],sell['id'],buyer,seller,fee,now()))

def orderbook(c, symbol):
    if symbol not in SUPPORTED_PAIRS: raise ValueError('Unsupported trading pair')
    base, _ = pair_assets(symbol)
    with MARKET_LOCK:
        b = LIVE_MARKET['books'].get(base)
        live_connected = LIVE_MARKET['connected']
        last = LIVE_MARKET['items'].get(base, {}).get('price_usd', 0)
    bids=[]; asks=[]
    if b:
        bids=[{'price':p,'quantity':q,'orders':1,'virtual':False,'source':'Binance WebSocket'} for p,q in b['bids'][:12]]
        asks=[{'price':p,'quantity':q,'orders':1,'virtual':False,'source':'Binance WebSocket'} for p,q in b['asks'][:12]]
    rows=c.execute("SELECT side,price,remaining,COUNT(*) n FROM exchange_orders WHERE symbol=? AND status='OPEN' GROUP BY side,price ORDER BY price",(symbol,)).fetchall()
    for r in rows:
        row={'price':float(r['price']),'quantity':float(r['remaining']),'orders':int(r['n']),'virtual':False,'source':'NARaz'}
        (bids if r['side']=='BUY' else asks).append(row)
    bids=sorted(bids,key=lambda x:x['price'],reverse=True)[:12]
    asks=sorted(asks,key=lambda x:x['price'])[:12]
    return {'symbol':symbol,'last_price':last,'bids':bids,'asks':asks,'updated_at':now(),'realtime':bool(b and live_connected),'simulated_liquidity':False}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): return
    def out(self, data, status=200):
        raw=json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status); self.send_header('Content-Type','application/json; charset=utf-8'); self.send_header('Access-Control-Allow-Origin','*'); self.send_header('Access-Control-Allow-Headers','Content-Type, Authorization'); self.send_header('Access-Control-Allow-Methods','GET,POST,OPTIONS'); self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
    def do_OPTIONS(self): self.out({},204)
    def do_GET(self):
        c=db(); parsed=urlparse(self.path); p=parsed.path; qs=parse_qs(parsed.query)
        try:
            if p == '/api/network':
                return self.out({'network':'NARaz Network','token':TOKEN,'max_supply':MAX_SUPPLY,'supply':supply(c),'chain_length':c.execute('SELECT COUNT(*) n FROM blocks').fetchone()['n'],'online':True,'version':'1.3.0','websocket_port':WS_PORT,'market_realtime':LIVE_MARKET['connected']})
            if p == '/api/market': return self.out(market_data())
            if p == '/api/exchange/orderbook': return self.out(orderbook(c, qs.get('symbol',['BTC/USDT'])[0].upper()))
            if p == '/api/exchange/candles':
                symbol=qs.get('symbol',['BTCUSDT'])[0].upper().replace('/','')
                interval=qs.get('interval',['1h'])[0]
                limit=min(100,max(10,int(qs.get('limit',['48'])[0])))
                try:
                    candles=http_json(f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}', 7)
                    return self.out({'symbol':symbol,'interval':interval,'live':True,'candles':[{'time':int(x[0]),'open':float(x[1]),'high':float(x[2]),'low':float(x[3]),'close':float(x[4]),'volume':float(x[5])} for x in candles]})
                except Exception:
                    return self.out({'symbol':symbol,'interval':interval,'live':False,'candles':[]})
            if p == '/api/exchange/trades':
                symbol=qs.get('symbol',['BTC/USDT'])[0].upper()
                rows=c.execute('SELECT price,quantity,taker_side,timestamp FROM exchange_trades WHERE symbol=? ORDER BY id DESC LIMIT 50',(symbol,)).fetchall()
                return self.out({'symbol':symbol,'trades':[dict(r) for r in rows]})
            if p == '/api/exchange/orders':
                u=auth_user(c,self.headers)
                if not u:return self.out({'error':'Unauthorized'},401)
                rows=c.execute('SELECT id,symbol,side,order_type,price,quantity,remaining,status,avg_fill_price,filled,fee,created_at,updated_at FROM exchange_orders WHERE user_id=? ORDER BY id DESC LIMIT 100',(u['id'],)).fetchall()
                return self.out({'orders':[dict(r) for r in rows]})
            if p == '/api/exchange/wallet':
                u=auth_user(c,self.headers)
                if not u:return self.out({'error':'Unauthorized'},401)
                rows=c.execute('SELECT asset,quantity,avg_price_usd FROM demo_holdings WHERE user_id=? AND quantity>0 ORDER BY asset',(u['id'],)).fetchall()
                balances=[]
                for r in rows:
                    d=dict(r); d['reserved']=reserved(c,u['id'],r['asset']); d['available']=available(c,u['id'],r['asset']); balances.append(d)
                return self.out({'balances':balances,'fee_rate':EXCHANGE_FEE})
            if p == '/api/demo/accounts':
                rows=c.execute("SELECT username,email,wallet FROM users WHERE username IN ('demo_alice','demo_bob') ORDER BY username").fetchall()
                return self.out({'accounts':[dict(r) | {'balance':balance(c,r['wallet'])} for r in rows], 'password':'demo12345'})
            if p == '/api/chain':
                blocks=[{'index':r['idx'],'timestamp':r['timestamp'],'transactions':json.loads(r['transactions']),'previous_hash':r['previous_hash'],'nonce':r['nonce'],'hash':r['hash']} for r in chain_rows(c)]
                return self.out({'chain':blocks})
            if p == '/api/me':
                u=auth_user(c,self.headers)
                if not u:return self.out({'error':'Unauthorized'},401)
                return self.out({'username':u['username'],'email':u['email'],'wallet':u['wallet'],'balance':balance(c,u['wallet']),'token':TOKEN})
            if p.startswith('/api/balance/'):
                a=p.split('/api/balance/',1)[1]; return self.out({'address':a,'balance':balance(c,a),'token':TOKEN})
            if p == '/api/demo/portfolio':
                u=auth_user(c,self.headers)
                if not u:return self.out({'error':'Unauthorized'},401)
                cashrow=c.execute("SELECT quantity FROM demo_holdings WHERE user_id=? AND asset='USD'",(u['id'],)).fetchone()
                cash=float(cashrow['quantity']) if cashrow else 10000.0
                holdings=[dict(r) for r in c.execute('SELECT asset,quantity,avg_price_usd FROM demo_holdings WHERE user_id=? AND quantity>0',(u['id'],)).fetchall()]
                trades=[dict(r) for r in c.execute('SELECT asset,side,quantity,price_usd,value_usd,timestamp FROM demo_trades WHERE user_id=? ORDER BY id DESC LIMIT 100',(u['id'],)).fetchall()]
                return self.out({'cash_usd':cash,'holdings':holdings,'trades':trades})
            if p == '/api/transactions':
                u=auth_user(c,self.headers)
                if not u:return self.out({'error':'Unauthorized'},401)
                wallet=u['wallet']; out=[]
                for r in chain_rows(c):
                    for tx in json.loads(r['transactions']):
                        if tx['sender']==wallet or tx['recipient']==wallet: out.append(tx)
                return self.out({'transactions':out[-100:][::-1]})
            return self.out({'error':'Not found'},404)
        except Exception as e:
            return self.out({'error':str(e)},400)
        finally: c.close()

    def do_POST(self):
        c=db(); p=urlparse(self.path).path
        try:
            data=body(self)
            if p == '/api/register':
                username=str(data.get('username','')).strip(); email=str(data.get('email','')).strip().lower(); password=str(data.get('password',''))
                if len(username)<3 or len(username)>32:return self.out({'error':'Username must be 3-32 characters'},400)
                if len(password)<8:return self.out({'error':'Password must be at least 8 characters'},400)
                if '@' not in email:return self.out({'error':'Valid email required'},400)
                if c.execute('SELECT 1 FROM users WHERE username=? COLLATE NOCASE',(username,)).fetchone(): return self.out({'error':'Username already exists'},409)
                if c.execute('SELECT 1 FROM users WHERE email=? COLLATE NOCASE',(email,)).fetchone(): return self.out({'error':'Email already exists'},409)
                wallet=wallet_address(); c.execute('INSERT INTO users(username,email,password_hash,wallet,created_at) VALUES(?,?,?,?,?)',(username,email,hash_password(password),wallet,now()))
                user_id=c.execute('SELECT last_insert_rowid() id').fetchone()['id']
                tx=make_tx('TESTNET_FAUCET',wallet,TESTNET_GRANT); c.execute('INSERT INTO pending(sender,recipient,amount,timestamp,tx_id) VALUES(?,?,?,?,?)',(tx['sender'],tx['recipient'],tx['amount'],tx['timestamp'],tx['id']))
                mine_pending(c)
                token=secrets.token_urlsafe(32); c.execute('INSERT INTO sessions VALUES(?,?,?)',(token,user_id,now())); c.commit()
                return self.out({'message':'Account created','token':token,'user':{'username':username,'email':email,'wallet':wallet,'balance':balance(c,wallet),'token_name':TOKEN}},201)
            if p == '/api/login':
                email=str(data.get('email','')).strip().lower(); password=str(data.get('password',''))
                u=c.execute('SELECT * FROM users WHERE email=? COLLATE NOCASE',(email,)).fetchone()
                if not u or not verify_password(password,u['password_hash']): return self.out({'error':'Invalid email or password'},401)
                token=secrets.token_urlsafe(32); c.execute('INSERT INTO sessions VALUES(?,?,?)',(token,u['id'],now())); c.commit(); return self.out({'token':token,'user':{'username':u['username'],'email':u['email'],'wallet':u['wallet'],'balance':balance(c,u['wallet']),'token_name':TOKEN}})
            u=auth_user(c,self.headers)
            if not u:return self.out({'error':'Unauthorized'},401)
            if p == '/api/transfer':
                recipient=str(data.get('recipient','')).strip(); amount=float(data.get('amount',0))
                if amount<=0:return self.out({'error':'Amount must be positive'},400)
                dest=c.execute('SELECT wallet,username FROM users WHERE wallet=? OR username=? COLLATE NOCASE',(recipient,recipient)).fetchone()
                if not dest:return self.out({'error':'Recipient not found'},404)
                if dest['wallet']==u['wallet']:return self.out({'error':'Cannot send to yourself'},400)
                if amount>balance(c,u['wallet']):return self.out({'error':'Insufficient balance'},400)
                tx=make_tx(u['wallet'],dest['wallet'],amount); c.execute('INSERT INTO pending(sender,recipient,amount,timestamp,tx_id) VALUES(?,?,?,?,?)',(tx['sender'],tx['recipient'],tx['amount'],tx['timestamp'],tx['id']))
                block=mine_pending(c)
                return self.out({'message':'Transfer confirmed','transaction':tx,'block':block,'sender_balance':balance(c,u['wallet']),'recipient_balance':balance(c,dest['wallet'])},201)
            if p == '/api/demo/reset':
                c.execute("DELETE FROM demo_holdings WHERE user_id=?", (u['id'],)); c.execute("DELETE FROM demo_trades WHERE user_id=?", (u['id'],))
                c.execute("INSERT OR REPLACE INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)", (u['id'],'USD',10000.0,1.0)); c.execute("INSERT OR REPLACE INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)", (u['id'],'USDT',10000.0,1.0))
                c.execute("UPDATE exchange_orders SET status='CANCELLED', remaining=0, updated_at=? WHERE user_id=? AND status='OPEN'",(now(),u['id']))
                c.execute("DELETE FROM exchange_reservations WHERE user_id=?",(u['id'],))
                c.commit(); return self.out({'message':'Demo account reset','cash_usd':10000.0})
            if p in ('/api/demo/buy','/api/demo/sell'):
                asset=str(data.get('asset','')).upper(); quantity=float(data.get('quantity',0))
                if quantity<=0:return self.out({'error':'Quantity must be positive'},400)
                market=market_data(); item=next((x for x in market['items'] if x['symbol']==asset),None)
                if not item:return self.out({'error':'Asset is not available in live market'},404)
                price=float(item['price_usd']); value=price*quantity
                cashrow=c.execute("SELECT quantity FROM demo_holdings WHERE user_id=? AND asset='USD'",(u['id'],)).fetchone(); cash=float(cashrow['quantity']) if cashrow else 10000.0
                h=c.execute('SELECT quantity,avg_price_usd FROM demo_holdings WHERE user_id=? AND asset=?',(u['id'],asset)).fetchone(); old_qty=float(h['quantity']) if h else 0.0; old_avg=float(h['avg_price_usd']) if h else 0.0
                if p.endswith('/buy'):
                    if value>cash:return self.out({'error':'Недостаточно демо USD'},400)
                    new_qty=old_qty+quantity; new_avg=((old_qty*old_avg)+(quantity*price))/new_qty if new_qty else 0
                    c.execute("INSERT OR REPLACE INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)",(u['id'],asset,new_qty,new_avg)); c.execute("INSERT OR REPLACE INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)",(u['id'],'USD',cash-value,1.0)); side='BUY'
                else:
                    if quantity>old_qty:return self.out({'error':'Недостаточно актива в демо-портфеле'},400)
                    new_qty=old_qty-quantity; c.execute("INSERT OR REPLACE INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)",(u['id'],asset,new_qty,old_avg)); c.execute("INSERT OR REPLACE INTO demo_holdings(user_id,asset,quantity,avg_price_usd) VALUES(?,?,?,?)",(u['id'],'USD',cash+value,1.0)); side='SELL'
                c.execute('INSERT INTO demo_trades(user_id,asset,side,quantity,price_usd,value_usd,timestamp) VALUES(?,?,?,?,?,?,?)',(u['id'],asset,side,quantity,price,value,now())); c.commit(); return self.out({'message':side,'asset':asset,'quantity':quantity,'price_usd':price,'value_usd':value})
            if p == '/api/exchange/order':
                symbol=str(data.get('symbol','BTC/USDT')).upper(); side=str(data.get('side','BUY')).upper(); order_type=str(data.get('type','MARKET')).upper(); qty=float(data.get('quantity',0)); price=float(data.get('price',0) or 0)
                if symbol not in SUPPORTED_PAIRS:return self.out({'error':'Unsupported trading pair'},400)
                if side not in ('BUY','SELL') or qty<=0:return self.out({'error':'Invalid order'},400)
                if order_type=='MARKET': result=execute_virtual_market(c,u,symbol,side,qty); c.commit(); return self.out(result,201)
                if order_type=='LIMIT': result=place_limit(c,u,symbol,side,price,qty); c.commit(); return self.out(result,201)
                return self.out({'error':'Supported types: MARKET, LIMIT'},400)
            if p == '/api/exchange/order/cancel':
                oid=int(data.get('order_id',0)); row=c.execute("SELECT * FROM exchange_orders WHERE id=? AND user_id=?",(oid,u['id'])).fetchone()
                if not row:return self.out({'error':'Order not found'},404)
                if row['status']!='OPEN':return self.out({'error':'Order is not open'},400)
                base,quote=pair_assets(row['symbol']); release(c,u['id'],quote,float(row['price'])*float(row['remaining'])*(1+EXCHANGE_FEE)) if row['side']=='BUY' else release(c,u['id'],base,float(row['remaining'])); c.execute("UPDATE exchange_orders SET status='CANCELLED',remaining=0,updated_at=? WHERE id=?",(now(),oid)); c.commit(); return self.out({'order_id':oid,'status':'CANCELLED'})
            return self.out({'error':'Not found'},404)
        except Exception as e:
            c.rollback()
            return self.out({'error':str(e)},400)
        finally: c.close()


if __name__=='__main__':
    init_db(); _start_market_stream(); _start_naraz_ws_server(); print(f'NARaz Network API listening on http://{HOST}:{PORT}, WebSocket on ws://{HOST}:{WS_PORT}'); ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
