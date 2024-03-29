/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "page/b_plus_tree_internal_page.h"

namespace scudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize((PAGE_SIZE- sizeof(BPlusTreeInternalPage))/sizeof(MappingType) - 1); //minus 1 for first invalid key
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  KeyType k=array[index].first;
  return k;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  //LOG_DEBUG("%d",GetSize());
  for (int i = 0; i < GetSize(); i++) {
    if (value == ValueAt(i)) 
      return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  ValueType v=array[index].second;
  return v;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  assert(GetSize() > 1);
  int l =1, r = GetSize() - 1;
  while (l<=r) { 
    int mid = (r+l)/2;
    if (comparator(array[mid].first,key) <= 0) 
      l = mid+1;
    else 
      r = mid-1;
  }
  return array[l-1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  array[1].first = new_key;
  array[1].second = new_value;
  array[0].second = old_value;
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  int index = ValueIndex(old_value) + 1;
  for (int i = GetSize(); i > index; i--) {
    array[i]= array[i - 1];
  }
  array[index].first = new_key;
  array[index].second = new_value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  int total = GetMaxSize() + 1;
  page_id_t newId = recipient->GetPageId();
  for (int i = total/2; i < total; i++) {
    recipient->array[i - total/2] = array[i];
    Page* crp = buffer_pool_manager->FetchPage(array[i].second);
    BPlusTreePage *cp = reinterpret_cast<BPlusTreePage *>(crp->GetData());
    cp->SetParentPageId(newId);
    buffer_pool_manager->UnpinPage(array[i].second,true);
  }
  recipient->SetSize(total - total/2);
  SetSize(total/2);
  
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index + 1; i < GetSize(); i++) {
    array[i - 1] = array[i];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType r = ValueAt(0);
  SetSize(0);
  return r;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
  Page *newp = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *pp = reinterpret_cast<BPlusTreeInternalPage *>(newp->GetData());
  SetKeyAt(0, pp->KeyAt(index_in_parent));
  buffer_pool_manager->UnpinPage(pp->GetPageId(), false);
  
  int a = recipient->GetSize();
  for (int i = 0; i < GetSize(); ++i) {
    recipient->array[a + i] = array[i];
    Page* crp = buffer_pool_manager->FetchPage(array[i].second);
    BPlusTreePage *cp = reinterpret_cast<BPlusTreePage *>(crp->GetData());
    cp->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(array[i].second,true);
  }
  recipient->SetSize(a + GetSize());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  Page *p = buffer_pool_manager->FetchPage(array[0].second);
  BPlusTreePage *cp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  cp->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(cp->GetPageId(), true);
  
  IncreaseSize(-1);
  for(int i=0;i<GetSize();i++){
    array[i]=array[i+1];
  }
  
  Page *p2 = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *pp = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(p2->GetData());
  pp->SetKeyAt(pp->ValueIndex(GetPageId()), array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array[GetSize()] = pair;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(array[GetSize()-1], parent_index, buffer_pool_manager);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  for(int i=GetSize();i>0;i--){
    array[i]=array[i-1];
  }  
  IncreaseSize(1);
  array[0] = pair;

  Page *p = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage *cp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  cp->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(cp->GetPageId(), true);
  
  Page *p2 = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *pp = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(p2->GetData());
  pp->SetKeyAt(parent_index, array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace scudb
