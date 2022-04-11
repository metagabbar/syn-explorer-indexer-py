# syn-explorer-indexer

Indexes synapse bridge transactions across all chains. Pulled from [BlazeWasHere/SYN-Explorer-API](https://github.com/BlazeWasHere/SYN-Explorer-API)

### Setup

* Run redis
  * `docker run -d -p 6379:6379 redis`
* Run mongo
  * `docker run -d -p 27017:27017 mongo`
* `pip install -r requirements.txt`. Ensure `python3-dev` tools and `gcc` is installed
* Setup the `.env` file with RPCs and connection URLs
* `python main.py`