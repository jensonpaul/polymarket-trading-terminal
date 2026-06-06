...continue from README.md

First order is still auto canceled even after initial INIT_PING order status check (issue is something else) - client initiation/heartbeats?


Could you simply write the market feed data into a file. separate files for each slugs and up/down token sides. it should be non blocking. keep the prices in memory for each market side token. on stop market feed or market close or stop, write them. or if possible when the market timestamp is over. 
Essentially, the feed is responsible for updating the in-memory state, and the file is only touched once, when the market closes or the window ends. each market is a 5 minute window. so calculate timestamp and 5 minute window accordingly when starting the market data store and write accordintly. ask me if you need any clarifications.

Do you want full tick history or just last price per side?
current design: full (ts, price) series
alternative: only last_up, last_down
What file format do you actually want?
JSON (easy, slow-ish, human-readable)
CSV (faster, smaller)
Parquet (best for analytics, more setup)
Do you want overwrite or append if restart happens mid-window?
1. full tick history
2. parquet as well as csv
3. append for restarts (only for same market)

In fact, let this be a console command from shell access so that the writing layer is abstracted away from the UI and worker. However, the market data feed consumer for both cli and the app should be the same. can we acheive this.

the goal is to run the cli command or binary separate from the app runtime so that there wouldn't be separate websocket connection for both console as well as app. so, app and console might be running independently and if app has started while command is running, app will consume the already running market feed and vice versa. 

Low Latency
UDS + broadcast + incremental updates





https://github.com/Juswanth-T/Low-Latency-Engine
A low-latency C++ engine that aggregates cryptocurrency prices from multiple exchanges (Binance, Kraken, Coinbase) with Prometheus metrics, graphana dashboards and Kubernetes deployment.
https://github.com/brucify/orderly
A Rust WebSocket client that connects to multiple crypto exchanges and publishes a merged live order book through gRPC stream 
https://github.com/minicheddar/crypto-stream
High performance market data handlers for cryptocurrency exchanges 
https://github.com/arakinyemi/crypto-cli
This is a command line application that lists prices of cryptocurrency across five different crypto exchanges (Binance, Coinbase, Kraken, KuCoin, and OKX) 