# coding: utf-8

import mmh3
import redis


BIT_SIZE = 5000000
SEEDS = [50, 51, 52, 53, 54, 55, 56]


def get_redis(host='localhost', port=6379, db=0):
    return redis.Redis(host=host, port=port, db=db)


class BloomFilter(object):

    def __init__(self, key='bloomfilter'):
        self.db = get_redis()
        self.key = key

    def cal_offsets(self, content):
        return [mmh3.hash(content, seed) % BIT_SIZE for seed in SEEDS]

    def is_contains(self, content):
        if not content:
            return False
        locs = self.cal_offsets(content)

        return all(True if self.db.getbit(self.key, loc) else False for loc in locs)

    def insert(self, content):
        locs = self.cal_offsets(content)
        for loc in locs:
            self.db.setbit(self.key, loc, 1)


if __name__ == '__main__':
    bloom_filter = BloomFilter()

    test_url = 'https://douban.com'

    print 'before'
    if bloom_filter.is_contains(test_url):
        print test_url + ' is existed'
    else:
        print test_url + ' is not existed'

    bloom_filter.insert(test_url)

    print 'after'
    if bloom_filter.is_contains(test_url):
        print test_url + ' is existed'
    else:
        print test_url + ' is not existed'
