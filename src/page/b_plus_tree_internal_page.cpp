#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_ + INTERNAL_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetKeySize(key_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value) return i;
  }
  return -1;
}
/*
 * 返回 GenericKey 类型指针，可以操作 data_ 数组
 * @param index
 * @return
 */
void *InternalPage::PairPtrAt(int index) { return KeyAt(index); }
/*
 * 键值对拷贝
 * @param dest
 * @param src
 * @param pair_num
 */
void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  // Root 就是 Leaf 的部分情况
  if (GetSize() == 0) return INVALID_PAGE_ID;
  if (GetSize() == 1) return ValueAt(0);

  // 判断第一个键值对
  GenericKey *secKey = KeyAt(1);
  if (KM.CompareKeys(key, secKey) < 0) return ValueAt(0);

  // 从第二个键值对开始二分搜索
  int left = 1;
  int right = GetSize();
  while (left < right) {
    int mid = (left + right) / 2;
    GenericKey *midKey = KeyAt(mid);
    if (KM.CompareKeys(midKey, key) < 0)
      right = mid - 1;
    else
      left = mid + 1;
  }
  return ValueAt(left);
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
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // 新 Root 的生成
  SetSize(2);
  SetValueAt(0, old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // 找到 old_value 所在的 index
  int size = GetSize();
  int old_index = ValueIndex(old_value);

  // 依次移动后半部分
  for (int i = size; i > old_index + 1; --i) {  // old_index+1 是新键值对需要插入的地方
    SetValueAt(i, ValueAt(i - 1));
    SetKeyAt(i, KeyAt(i - 1));
  }

  // 新值插入并修改size
  SetKeyAt(old_index + 1, new_key);
  SetValueAt(old_index, new_value);
  SetSize(size + 1);
  return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 *
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  // 利用PairPtrAt()计算地址, 传给CopyNFrom()
  int size = GetSize();
  recipient->CopyNFrom(PairPtrAt(size - size / 2), size / 2, buffer_pool_manager);
  size = size - size / 2;
  SetSize(size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  // 用两个指针传递数据给CopyLastFrom()
  GenericKey *key;
  page_id_t *pageId;
  char *source = reinterpret_cast<char *>(src);
  SetSize(GetSize() + size);
  for (int i = 0; i < size; i++) {
    memcpy(key, source, GetKeySize());
    source += GetKeySize();
    memcpy(pageId, source, 4);
    source += sizeof(page_id_t);
    CopyLastFrom(key, *pageId, buffer_pool_manager);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  //计算index，依次向前移动
  int size = GetSize();
  SetSize(--size);
  for(int i = index;i<size;i++){
    SetKeyAt(i,KeyAt(i+1));
    SetValueAt(i,ValueAt(i+1));
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  //判断是否是唯一pair，然后返回值
  if(GetSize() != 1) return INVALID_PAGE_ID;
  page_id_t res = ValueAt(0);
  Remove(0);
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  //首先拷贝来自parent的键值对，因为最小
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  recipient->CopyNFrom(PairPtrAt(1), GetSize() - 1, buffer_pool_manager);
  buffer_pool_manager->DeletePage(GetPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // 计算index并且插入新的键值对
  int index = GetSize();
  SetValueAt(index, value);
  SetKeyAt(index, key);
  SetSize(GetSize() + 1);

  // 更新page_id对应的page中的header内容
  Page *page = buffer_pool_manager->FetchPage(value);
  auto *inPage = reinterpret_cast<InternalPage *>(page->GetData());
  inPage->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {}