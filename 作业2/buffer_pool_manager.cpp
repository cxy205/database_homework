#include "buffer/buffer_pool_manager.h"

namespace scudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::BufferPoolManager(size_t pool_size,DiskManager *disk_manager,LogManager *log_manager): pool_size_(pool_size), disk(disk_manager),log(log_manager)
    {
        // a consecutive memory space for buffer pool
        pages = new Page[pool_size_];
        page_list = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
        change = new LRUReplacer<Page *>;
        free = new list<Page *>;

        // put all the pages into free list
        for (size_t i = 0; i < pool_size_; ++i) {
            free->push_back(&pages[i]);
        }
    }

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::~BufferPoolManager() {
        delete[] pages;
        delete page_list;
        delete change;
        delete free;
    }

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
    Page *BufferPoolManager::FetchPage(page_id_t page_id) {
        lock_guard<mutex> lck(lock);
        Page *pst;
        bool tem=page_list->Find(page_id,pst);
        if (tem) { //1.1
            pst->pin_count_++;
            change->Erase(pst);
            return pst;
        }else{
            if (free->empty()&&change->Size() == 0) {
                return nullptr;
            }else if(free->empty()){
                change->Victim(pst);
                assert(pst->GetPinCount() == 0);
            }else{
                pst = free->front();
                free->pop_front();
                assert(pst->GetPageId() == INVALID_PAGE_ID);
                assert(pst->GetPinCount() == 0);
            }

            if (pst->is_dirty_) disk->WritePage(pst->GetPageId(),pst->data_);
            page_list->Remove(pst->GetPageId());
            page_list->Insert(page_id,pst);

            disk->ReadPage(page_id,pst->data_);
            pst->pin_count_ = 1;
            pst->is_dirty_ = false;
            pst->page_id_= page_id;

            return pst;
        }
    }

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
    bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
        lock_guard<mutex> lck(lock);
        Page *pst = nullptr;
        page_list->Find(page_id,pst);
        if (pst == nullptr) {
            return false;
        }else{
            pst->is_dirty_ = is_dirty;
            if(pst->GetPinCount() <= 0) return false;
            if(--pst->pin_count_ == 0) change->Insert(pst);
            return true;
        }
    }

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
    bool BufferPoolManager::FlushPage(page_id_t page_id) {
        lock_guard<mutex> lck(lock);
        Page *pst = nullptr;
        page_list->Find(page_id,pst);
        if ((pst != nullptr || pst->page_id_ != INVALID_PAGE_ID)&&!pst->is_dirty_){
            return true;
        } else if (pst->is_dirty_) {
            disk->WritePage(page_id,pst->GetData());
        }
        return true;
    }

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
    bool BufferPoolManager::DeletePage(page_id_t page_id) {
        lock_guard<mutex> lck(lock);
        Page *pst ;
        page_list->Find(page_id,pst);
        if(pst==nullptr){
            disk->DeallocatePage(page_id);
            return true;
        }else if (pst->GetPinCount() > 0) return false;

        change->Erase(pst);
        pst->is_dirty_= false;
        page_list->Remove(page_id);
        pst->ResetMemory();
        free->push_back(pst);
        disk->DeallocatePage(page_id);
        return true;
    }

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
    Page *BufferPoolManager::NewPage(page_id_t &page_id) {
        lock_guard<mutex> lck(lock);
        Page *pst;
        if (free->empty()&&change->Size() == 0) {
            return nullptr;
        }else if(free->empty()){
            change->Victim(pst);
            assert(pst->GetPinCount() == 0);
        }else{
            pst = free->front();
            free->pop_front();
            assert(pst->GetPageId() == INVALID_PAGE_ID);
            assert(pst->GetPinCount() == 0);
        }

        page_id = disk->AllocatePage();
        if (pst->is_dirty_) {
            disk->WritePage(pst->GetPageId(),pst->data_);
        }
        page_list->Remove(pst->GetPageId());
        page_list->Insert(page_id,pst);

        pst->page_id_ = page_id;
        pst->ResetMemory();
        pst->is_dirty_ = false;
        pst->pin_count_ = 1;

        return pst;
    }

} // namespace scudb