#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
    template <typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size)
            :globalDepth(0), bucketMaxSize(size), numBuckets(1) {
        bucketTable.push_back(make_shared<Bucket>(0));
    }


/*
 * helper function to calculate the hashing address of input key
 */
    template <typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) const{
        return hash<K>{}(key);
    }

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
    template <typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const {
        lock_guard<mutex> lck(lock);
        return globalDepth;
    }

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
    template <typename K, typename V>
    int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
        lock_guard<mutex> lck(lock);
        if (!bucketTable[bucket_id] || bucketTable[bucket_id]->items.empty()) return -1;
        return bucketTable[bucket_id]->localDepth;
    }

/*
 * helper function to return current number of bucket in hash table
 */
    template <typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const {
        lock_guard<mutex> lck(lock);
        return numBuckets;
    }

/*
 * lookup function to find value associate with input key
 */
    template <typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
        lock_guard<mutex> lck(lock);
        auto index = getBucketIndex(key);
        shared_ptr<Bucket> bucket = bucketTable[index];
        if (bucket != nullptr && bucket->items.find(key) != bucket->items.end()) {
            value = bucket->items[key];
            return true;
        }
        return false;
    }

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
    template <typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {
        lock_guard<mutex> lck(lock);

        auto index = getBucketIndex(key);
        shared_ptr<Bucket> bucket = bucketTable[index];

        if (bucket == nullptr || bucket->items.find(key) == bucket->items.end()) {
            return false;
        }
        bucket->items.erase(key);
        return true;
    }

    template <typename K, typename V>
    int ExtendibleHash<K, V>::getBucketIndex(const K &key) const {
        return HashKey(key) & ((1 << globalDepth) - 1);
    }

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
    template <typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
        lock_guard<mutex> lck(lock);

        auto index = getBucketIndex(key);
        shared_ptr<Bucket> targetBucket = bucketTable[index];

        while (targetBucket->items.size() == bucketMaxSize) {
            if (targetBucket->localDepth == globalDepth) {
                size_t length = bucketTable.size();
                for (size_t i = 0; i < length; i++) {
                    bucketTable.push_back(bucketTable[i]);
                }
                globalDepth++;
            }
            int mask = 1 << targetBucket->localDepth;
            numBuckets++;
            auto zeroBucket = make_shared<Bucket>(targetBucket->localDepth + 1);
            auto oneBucket = make_shared<Bucket>(targetBucket->localDepth + 1);
            for (auto item : targetBucket->items) {
                size_t hashkey = HashKey(item.first);
                if (hashkey & mask) {
                    oneBucket->items.insert(item);
                } else {
                    zeroBucket->items.insert(item);
                }
            }

            for (size_t i = 0; i < bucketTable.size(); i++) {
                if (bucketTable[i] == targetBucket) {
                    if (i & mask) {
                        bucketTable[i] = oneBucket;
                    } else {
                        bucketTable[i] = zeroBucket;
                    }
                }
            }

            index = getBucketIndex(key);
            targetBucket = bucketTable[index];
        }

        targetBucket->items[key] = value;
    }
    template class ExtendibleHash<page_id_t, Page *>;
    template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
    template class ExtendibleHash<int, std::string>;
    template class ExtendibleHash<int, std::list<int>::iterator>;
    template class ExtendibleHash<int, int>;
} // namespace scudb