#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(int pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(int pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);
  for (int i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(int page_id) {
  lock_guard<mutex> lock(latch_);  // 加锁
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  int frame_id = page_table_[page_id];
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  lock_guard<mutex> lock(latch_);  // 加锁
  auto iter = page_table_.begin();
  while (iter != page_table_.end()) {
    int page_id = iter->first;
    int frame_id = page_table_[page_id];
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(page_id, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(int *page_id) {
  int frame_id;
  lock_guard<mutex> lock(latch_);  // 加锁

  if (!free_list_.empty()) {  // 存在空余页
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {  // 需根据LRU算法淘汰一页
    bool res = replacer_->Victim(&frame_id);
    if (!res) {
      return nullptr;  // 淘汰失败
    }
    // 在这里调用disk_manager_->WritePage方法实际上破坏了API的一致性，但FlushPgImp函数需解锁，比较麻烦
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    page_table_.erase(pages_[frame_id].page_id_);  // 在page_table中删除该frame对应的页
  }

  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;

  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory();
  
  disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  return &pages_[frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(int page_id) {
  int frame_id;
  lock_guard<std::mutex> lock(latch_);  // 加锁

  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {  // 原先就在buffer里
    frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->Pin(frame_id);  // 只有pin_count为0才有可能在replacer里
    }
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {  // 存在空余页
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {  // 需根据LRU算法淘汰一页
    bool res = replacer_->Victim(&frame_id);
    if (!res) {
      return nullptr;  // 淘汰失败
    }
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    page_table_.erase(pages_[frame_id].page_id_);  // 在page_table中删除该frame对应的页
  }
  page_table_[page_id] = frame_id;

  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  return &pages_[frame_id];
}

bool BufferPoolManagerInstance::DeletePgImp(int page_id) {
  int frame_id;
  lock_guard<mutex> lock(latch_);  // 加锁
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }
  page_table_.erase(page_id);
  replacer_->Pin(frame_id);  // 从replacer中删除该页
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  free_list_.push_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(int page_id, bool is_dirty) {
  lock_guard<mutex> lock(latch_);  // 加锁
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  int frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;  // 不能直接赋值
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

int BufferPoolManagerInstance::AllocatePage() {
  const int next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const int page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  

