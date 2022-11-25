/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once
#include <list>
#include <mutex>

#include "buffer/lru_replacer.h"
#include "disk/disk_manager.h"
#include "hash/extendible_hash.h"
#include "logging/log_manager.h"
#include "page/page.h"
using namespace std;
namespace scudb {
    class BufferPoolManager {
    public:
        BufferPoolManager(size_t pool_size, DiskManager *disk_manager,
                          LogManager *log_manager = nullptr);

        ~BufferPoolManager();

        Page *FetchPage(page_id_t page_id);

        bool UnpinPage(page_id_t page_id, bool is_dirty);

        bool FlushPage(page_id_t page_id);

        Page *NewPage(page_id_t &page_id);

        bool DeletePage(page_id_t page_id);

    private:
        size_t pool_size_; // number of pages in buffer pool
        Page *pages;      // array of pages
        DiskManager *disk;
        LogManager *log;
        HashTable<page_id_t, Page*> *page_list; // to keep track of pages
        Replacer<Page *> *change;   // to find an unpinned page for replacement
        list<Page *> *free; // to find a free page for replacement
        mutex lock;             // to protect shared data structure
    };
} // namespace scudb