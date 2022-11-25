/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

    template <typename T> LRUReplacer<T>::LRUReplacer() {
        head=new Node();
        tail=new Node();
        head->nxt = tail;
        tail->pre = head;
    }

    template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
    template <typename T> void LRUReplacer<T>::Insert(const T &value) {
        lock_guard<mutex> lck(lock);
        Node* pst=new Node();
        if(map.find(value)==map.end()){
            pst->val=value;
        }else{
            pst=map[value];
            pst->pre->nxt=pst->nxt;
            pst->nxt->pre=pst->pre;
        }
        pst->nxt=head->nxt;
        pst->pre=head;
        pst->nxt->pre=pst;
        head->nxt=pst;
        map[value] = pst;
        return;
    }

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
    template <typename T> bool LRUReplacer<T>::Victim(T &value) {
        lock_guard<mutex> lck(lock);
        if (map.empty()) {
            return false;
        }else {
            value = tail->pre->val;
            tail->pre = tail->pre->pre;
            tail->pre->nxt = tail;
            map.erase(value);
            return true;
        }
        return false;
    }

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
    template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
        lock_guard<mutex> lck(lock);
        if (map.find(value) != map.end()) {
            Node* pst = map[value];
            pst->pre->nxt = pst->nxt;
            pst->nxt->pre = pst->pre;
        }
        return map.erase(value);
    }

    template <typename T> size_t LRUReplacer<T>::Size() {
        lock_guard<mutex> lck(lock);
        return map.size();
    }

    template class LRUReplacer<Page *>;
// test only
    template class LRUReplacer<int>;

} // namespace scudb