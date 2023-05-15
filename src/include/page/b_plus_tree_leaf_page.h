#ifndef MINISQL_B_PLUS_TREE_LEAF_PAGE_H
#define MINISQL_B_PLUS_TREE_LEAF_PAGE_H

/**
 * b_plus_tree_leaf_page.h
 *
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.

 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 24 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | ParentPageId (4) |
 *  ---------------------------------------------------------------------
 *  ------------------------------
 * | PageId (4) | NextPageId (4)
 *  ------------------------------
 */
#include <utility>
#include <vector>

#include "index/generic_key.h"
#include "page/b_plus_tree_page.h"

#define LEAF_PAGE_HEADER_SIZE 32
#define LEAF_PAGE_SIZE (((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType)) - 1)

class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int key_size = UNDEFINED_SIZE,
            int max_size = UNDEFINED_SIZE);

  // helper methods
  page_id_t GetNextPageId() const;

  void SetNextPageId(page_id_t next_page_id);

  GenericKey *KeyAt(int index);

  void SetKeyAt(int index, GenericKey *key);

  RowId ValueAt(int index) const;

  void SetValueAt(int index, RowId value);

  int KeyIndex(const GenericKey *key, const KeyManager &comparator);

  void *PairPtrAt(int index);

  void PairCopy(void *dest, void *src, int pair_num = 1);

  std::pair<GenericKey *, RowId> GetItem(int index);

  // insert and delete methods
  int Insert(GenericKey *key, const RowId &value, const KeyManager &comparator);

  bool Lookup(const GenericKey *key, RowId &value, const KeyManager &comparator);

  int RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &comparator);

  // Split and Merge utility methods
  void MoveHalfTo(BPlusTreeLeafPage *recipient);

  void MoveAllTo(BPlusTreeLeafPage *recipient);

  void MoveFirstToEndOf(BPlusTreeLeafPage *recipient);

  void MoveLastToFrontOf(BPlusTreeLeafPage *recipient);

 private:
  void CopyNFrom(void *src, int size);

  void CopyLastFrom(GenericKey *key, const RowId value);

  void CopyFirstFrom(GenericKey *key, const RowId value);

  page_id_t next_page_id_{INVALID_PAGE_ID};

  char data_[PAGE_SIZE - LEAF_PAGE_HEADER_SIZE];
};

using LeafPage = BPlusTreeLeafPage;
#endif  // MINISQL_B_PLUS_TREE_LEAF_PAGE_H
