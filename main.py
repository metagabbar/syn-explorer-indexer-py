import gevent
from indexer.helpers import dispatch_get_logs
from indexer.rpc import bridge_callback
from indexer import poll

if __name__ == '__main__':
    gevent.joinall([
        # Gets new events
        gevent.spawn(poll.start, bridge_callback),
        # Backfill events
        gevent.spawn(dispatch_get_logs, bridge_callback)
    ])
