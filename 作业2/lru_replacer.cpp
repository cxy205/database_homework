#include "buffer/lru_replacer.h"
using namespace std;

namespace bustub {

LRUReplacer::LRUReplacer(int num_pages) {
  Itpair null_data(false, data.end());
  int max_frame_id = static_cast<int>(num_pages);
  for (int i = 0; i < max_frame_id; i++) {
    map[i] = null_data;
  }
}

LRUReplacer::~LRUReplacer() {}

bool LRUReplacer::Victim(int *frm_id) {
  lock_guard<mutex> lock(lock);
  if (data.empty()) {
    return false;
  }
  *frm_id= data.front();
  map[*frm_id].first = false;
  data.pop_front();
  return true;
}

void LRUReplacer::Pin(int frm_id) {
  lock_guard<mutex> lock(lock);
  if (map[frm_id].first) {
  	map[frm_id].first = false;
    data.erase(map[frm_id].second);
  }
}

void LRUReplacer::Unpin(int frm_id) {
  lock_guard<mutex> lock(lock);
  if (!map[frm_id].first) {
    data.push_back(frm_id);
    map[frm_id].second = --data.end();
    map[frm_id].first = true;
  }
}

int LRUReplacer::Size() {
  lock_guard<mutex> lock(lock);
  return data.size();
}

}  // namespace bustub


