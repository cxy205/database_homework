#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

class LRUReplacer : public Replacer {
 public:

  explicit LRUReplacer(int num_pages);

  ~LRUReplacer() override;

  bool Victim(int *frm_id) override;

  void Pin(int frm_id) override;

  void Unpin(int frm_id) override;

  int Size() override;

 private:
  using ItPair = std::pair<bool, std::list<int>::iterator>;
  std::mutex lock;
  std::list<int> data;
  std::unordered_map<int, ItPair> map;
};

}  // namespace bustub
