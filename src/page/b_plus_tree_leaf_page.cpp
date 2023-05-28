#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const { return next_page_id_; }

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int left = 0;
  int right = GetSize();
  while (left < right) {
    int mid = (left + right) / 2;
    GenericKey *midKey = KeyAt(mid);
    int compare = KM.CompareKeys(key, midKey);
    switch (compare) {
      case -1:
        right = mid;
        break;
      case 0:
        return mid;
      case 1:
        left = mid + 1;
        break;
    }
  }
  return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) { return KeyAt(index); }

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
  // replace with your own code
  return make_pair(KeyAt(index), ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int size = GetSize();
  // 空节点直接插入
  if (size == 0) {
    SetKeyAt(0, key);
    SetValueAt(0, value);
    SetSize(1);
    return 1;
  }

  // 获得需要插入的位置
  int index = KeyIndex(key, KM);

  // 将index后的pair整体后移
  PairCopy(PairPtrAt(index + 1), PairPtrAt(index), size - index);
  SetKeyAt(index, key);
  SetValueAt(index, value);
  SetSize(++size);
  return size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  // 调用CopyNFrom()拷贝N个到recipient
  int size = GetSize();
  int N = size / 2;
  recipient->CopyNFrom(PairPtrAt(size - N), N);
  SetSize(GetSize() - N);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  // 获得index后拷贝键值对
  int index = GetSize();
  PairCopy(PairPtrAt(index), src, size);
  SetSize(index + size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  // 二分搜索的逻辑，如果查找失败会返回size，因为此时left==right， right被初始化为GetSize()
  int index = KeyIndex(key, KM);
  if (index >= GetSize()) return false;
  // 判断
  if (KM.CompareKeys(key, KeyAt(index)))
    return false;
  else {
    value = ValueAt(index);
    return true;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  // 先查找已知键，若存在删除移动后返回当前大小，不在直接返回当前大小
  RowId value;
  int size = GetSize();
  if (Lookup(key, value, KM)) {
    int index = KeyIndex(key, KM);
    PairCopy(PairPtrAt(index), PairPtrAt(index + 1), size - index - 1);
    SetSize(--size);
    return size;
  } else
    return size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  // 先全部移动，后修改nextPageId,最后修改当前size
  int size = GetSize();
  recipient->CopyNFrom(PairPtrAt(0), size);
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  // 先拷贝，后整体移动
  int size = GetSize();
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  PairCopy(PairPtrAt(0), PairPtrAt(1), size - 1);
  SetSize(--size);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int size = GetSize();
  SetValueAt(size, value);
  SetKeyAt(size, key);
  SetSize(++size);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  recipient->PairCopy(recipient->PairPtrAt(1), recipient->PairPtrAt(0), recipient->GetSize());
  recipient->PairCopy(recipient->PairPtrAt(0), PairPtrAt(GetSize() - 1),  1);
  SetSize(GetSize() - 1);
  recipient->SetSize(recipient->GetSize() + 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int size = GetSize();
  PairCopy(PairPtrAt(1), PairPtrAt(0), size);
  SetValueAt(0, value);
  SetKeyAt(0, key);
  SetSize(++size);
}