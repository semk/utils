#!/usr/bin/env python
#
# A Log handler that stores log messages on Riak
#
# @author: Sreejith K
# Created On 20th Feb 2012

import riak
from riak import key_filter
import logging
import time


class RiakLogger(logging.Handler):
    """Log messages to Riak
    """
    def __init__(self, 
                 level=logging.NOTSET, 
                 application='root', 
                 logname='default', 
                 host='localhost', 
                 port=8091):
        self._application = application
        self._logname = logname
        self._host = host
        self._port = port
        logging.Handler.__init__(self, level)
        # Riak connection
        self._client = riak.RiakClient(host, port)
        self._bucket = self._client.bucket('%s_%s_log' % 
                                           (self._application, self._logname))
        
    def emit(self, record):
        """Flush the log.
        """
        logdata = self._bucket.new('%s-%s' % (record.created, record.levelname),
                                   record.__dict__)
        logdata.store()

    def get_log_messages(self, items=10, level='INFO'):
        """Get log messages from Riak.
        """
        client = riak.RiakClient(self._host, self._port)
        query = client.add(self._bucket.get_name())
        filters = (key_filter.tokenize('-', 1) + key_filter.less_than(str(time.time()))) \
                    & (key_filter.tokenize('-', 2) + key_filter.eq(level))
        query.add_key_filters(filters)
        query.map_values_json()\
            .reduce_sort('function(a, b) { return b.created - a.created }')\
            .reduce_limit(items)
        for result in query.run():
            yield result


def setup_logging():
    """Setup logging with Riak
    """
    log = logging.getLogger('')
    riak_logger = RiakLogger(application='testapp', logname='testlog')
    log.addHandler(riak_logger)
    return riak_logger


def test_logging():
    # set DEBUG mode
    logging.basicConfig(level=logging.DEBUG)
    # setup logging using riak
    riak_logger = setup_logging()
    # create a custom logger
    log = logging.getLogger('RiakLogger')
    log.debug('Test Message')
    # log using root logger
    logging.debug('From root logger')
    # get the latest 15 log messages from Riak
    for message in riak_logger.get_log_messages(5, 'DEBUG'):
        print message.get('created'), message.get('msg')


if __name__ == '__main__':
    test_logging()
