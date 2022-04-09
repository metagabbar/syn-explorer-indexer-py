# syn-explorer-indexer

Indexes synapse bridge transactions across all chains. [BlazeWasHere/SYN-Explorer-API](https://github.com/BlazeWasHere/SYN-Explorer-API)

### Setup

* Run redis
  * `docker run -d -p 6379:6379 redis`
* `pip install -r requirements.txt`
* Setup the `.env` file with RPCs and connection URLs
* `python main.py`