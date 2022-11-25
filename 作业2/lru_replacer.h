/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include<memory>
#include<unordered_map>
#include<mutex>
#include "buffer/replacer.h"
#include "hash/extendible_hash.h"
using namespace std;
namespace scudb {

    template <typename T> class LRUReplacer : public Replacer<T> {
        struct Node{
            Node(){};
            Node(T val): val(val){};
            T val;
            Node* pre;
            Node* nxt;
        };
    public:
        // do not change public interface
        LRUReplacer();

        ~LRUReplacer();

        void Insert(const T &value);

        bool Victim(T &value);

        bool Erase(const T &value);

        size_t Size();

    private:
        // add your member variables here
        //typedef void (*node_ptr)(Node);
        Node* head;
        Node* tail;
        unordered_map<T,Node*> map;
        mutable mutex lock;
    };

} // namespace scudb