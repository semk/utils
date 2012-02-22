#! /usr/bin/env python
#
# A Python Wrapper for Riak which allows you to use 
# mutiple index queries at once.
#
# @author: Sreejith K
# Created On 22nd Feb 2012

import riak


_JS_MAP_FUNCTION = """
function(v) {
var data = JSON.parse(v.values[0].data);
if(%s) {
return [[v.key, data]];
}
return [];
}
"""


class InvalidFilterOperation(Exception):
    def __init__(self, op):
        self._op = op

    def __repr__(self):
        print 'Invalid Operator %s' % self._op


class RiakMultiIndexQuery(object):
    """This class implements a Muti-Query interface for
    Riak Indexes which makes use of Index queries and MapReduce
    """
    def __init__(self, address, port, bucket):
        self._address = address
        self._port = port
        self._bucket = bucket
        self.reset()

    def reset(self):
        self._client = riak.RiakClient(self._address, self._port)
        self._mr_query = riak.RiakMapReduce(self._client)
        self._mr_inputs = set()
        self._filters = []
        self._offset = 0
        self._limit = 0
        self._order = ()

    def filter(self, field, op, value):
        self._filters.append((field, op, value))
        return self

    def offset(self, offset):
        self._offset = offset
        return self

    def limit(self, limit=0):
        self._limit = limit
        return self

    def order(self, sort_key, order='ASC'):
        self._order = (sort_key, order)
        return self

    def _filter_to_index_query(self, field, op, value):
        if isinstance(value, basestring):
            index_type = 'bin'
            max = min = ''
        else:
            index_type = 'int'
            max = 99999999999999999
            min = -max
        field = '%s_%s' % (field, index_type)

        if op == '==':
            return self._client.index(self._bucket, field, value)
        elif op == '>' or op == '>=':
            return self._client.index(self._bucket, field, value, max)
        elif op == '<' or op == '<=':
            return self._client.index(self._bucket, field, min, value)
        else:
            raise InvalidFilterOperation(op)

    def run(self, timeout=9000):
        mr_inputs = set()
        for (field, op, value) in self._filters:
            for res in self._filter_to_index_query(field, op, value).run():
                mr_inputs.add(res.get_key())

        if not mr_inputs:
            self._mr_query = self._client.add(self._bucket)
        for key in mr_inputs:
            self._mr_query.add(self._bucket, key)

        if not self._filters:
            filter_condition = 'true'
        else:
            conditions = []
            for filter in self._filters:
                conditions.append('data.%s %s %r' % filter) 
            filter_condition = ' && '.join(conditions).strip()

        map_function = _JS_MAP_FUNCTION % filter_condition
        self._mr_query.map(map_function)

        if self._order:
            if self._order[1] == 'DESC':
                reduce_func = 'function(a, b) { return b.%s - a.%s }'\
                    % (self._order[0], self._order[0])
            else:
                reduce_func = 'function(a, b) { return a.%s - b.%s }' \
                    % (self._order[0], self._order[0])
            self._mr_query.reduce(reduce_func)

        if self._limit:
            start = self._offset
            end = self._offset + self._limit
            self._mr_query.reduce('Riak.reduceSlice', {'arg': [start, end]})

        for result in self._mr_query.run(timeout):
            yield result


def test_multi_index_query():
    client = riak.RiakClient('localhost', 8091)
    bucket = client.bucket('test_multi_index')

    bucket.new('sree', {'name': 'Sreejith', 'age': '25'}).\
        add_index('name_bin', 'Sreejith').\
        add_index('age_int', 25).store()
    bucket.new('vishnu', {'name': 'Vishnu', 'age': '31'}).\
        add_index('name_bin', 'Vishnu').\
        add_index('age_int', 31).store()

    query = RiakMultiIndexQuery('localhost', 8091, 'test_multi_index')
    for res in query.filter('name', '==', 'Sreejith').run():
        print res

    query.reset()
    for res in query.filter('age', '<', 50).filter('name', '==', 'Vishnu').run():
        print res

    query.reset()
    for res in query.limit(1).run():
        print res

    query.reset()
    for res in query.order('age', 'ASC').offset(1).limit(1).run():
        print res


if __name__ == '__main__':
    test_multi_index_query()
