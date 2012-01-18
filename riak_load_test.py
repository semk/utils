#! /usr/bin/env python
#
# Sample data for Riak for testing purposes.
#   Sample contains the Social Media responses to the 2010 NBA Finals
#   http://simplymeasured.com/blog/2010/06/lakers-vs-celtics-social-media-breakdown-nba/
#
#   This script pushes the data into Riak and analyzes the write performance.
#
#   @author: Sreejith K
#   Created on 18th Jan 2012

import csv
import riak
import itertools
import uuid
import time

def AddNBARecords(bucket_name, csvfile):
    """Push the NBA social media responses to Riak
    Args:
        bucket_name: Riak bucket name to push the data to
        csvfile:     Name of the CSV file to read the data from
    """
    client = riak.RiakClient(port=8091)
    bucket = client.bucket(bucket_name)
    start_time = time.time()
    with open(csvfile, 'rb') as f:
        reader = csv.reader(f)
        properties = reader.next()
        record_count = 0
        time_spent = 1
        try:
            for row in reader:
                record = {}
                for prop, val in itertools.izip(properties, row):
                    record[prop] = val
                key = uuid.uuid4().hex
                record_count += 1
                print 'Storing record #%d with key %s' % (record_count, key)
                bucket.new(key, record).store()
                time_spent = time.time() - start_time
                if record_count in xrange(1000, 1000000, 1000):
                    print 'Time taken: %d secs, Avg writes/sec: %f' %(time_spent, record_count/time_spent)
        except KeyboardInterrupt:
            print 'Interrupted by user. Exiting.'
        except Exception, ex:
            print 'Something went wrong: %s' %str(ex)
        print 'Total time taken: %d secs, Avg writes/sec: %f' %(time_spent, record_count/time_spent)


if __name__ == '__main__':
    AddNBARecords('NBAGame_1', 'nba1.csv')
    AddNBARecords('NBAGame_2', 'nba2.csv')
