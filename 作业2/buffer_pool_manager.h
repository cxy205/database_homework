#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/lru_replacer.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"
using namespace std;

namespace bustub {

class BufferPoolManagerInstance : public BufferPoolManager {
 public:

  BufferPoolManagerInstance(int pool_size, DiskManager *disk_manager, LogManager *log_manager = nullptr);
  BufferPoolManagerInstance(int pool_size, uint32_t num_instances, uint32_t instance_index,
                            DiskManager *disk_manager, LogManager *log_manager = nullptr);
  ~BufferPoolManagerInstance() override;
  size_t GetPoolSize() override { return pool_size_; }
  Page *GetPages() { return pages_; }

 protected:

  Page *FetchPgImp(int page_id) override;
  bool UnpinPgImp(int page_id, bool is_dirty) override;
  bool FlushPgImp(int page_id) override;
  Page *NewPgImp(int *page_id) override;
  bool DeletePgImp(int page_id) override;

  void FlushAllPgsImp() override;

  int AllocatePage();

  void DeallocatePage(int page_id) {
  }

  void ValidatePageId(int page_id) const;

  const int pool_size_;
  const uint32_t num_instances_ = 1;
  const uint32_t instance_index_ = 0;
  atomic<int> next_page_id_ = instance_index_;

  Page *pages_;
  DiskManager *disk_manager_;
  LogManager *log_manager_;
  unordered_map<int, int> page_table_;
  Replacer *replacer_;
  list<int> free_list_;
  mutex latch_;
};
} 
