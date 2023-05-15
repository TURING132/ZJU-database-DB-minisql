#ifndef MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H
#define MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H

#include <string.h>

#include <queue>

#include "index/generic_key.h"
#include "page/b_plus_tree_page.h"

#define INTERNAL_PAGE_HEADER_SIZE 28
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(std::pair<GenericKey *, page_id_t>)) - 1)
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int key_size = UNDEFINED_SIZE,
            int max_size = UNDEFINED_SIZE);

  GenericKey *KeyAt(int index);

  void SetKeyAt(int index, GenericKey *key);

  int ValueIndex(const page_id_t &value) const;

  page_id_t ValueAt(int index) const;

  void SetValueAt(int index, page_id_t value);

  void *PairPtrAt(int index);

  void PairCopy(void *dest, void *src, int pair_num = 1);

  page_id_t Lookup(const GenericKey *key, const KeyManager &KP);

  void PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value);

  int InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value);

  void Remove(int index);

  page_id_t RemoveAndReturnOnlyChild();

  // Split and Merge utility methods
  void MoveAllTo(BPlusTreeInternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager);

  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);

  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, GenericKey *middle_key,
                        BufferPoolManager *buffer_pool_manager);

  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, GenericKey *middle_key,
                         BufferPoolManager *buffer_pool_manager);

 private:
  void CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager);

  void CopyLastFrom(GenericKey *key, page_id_t value, BufferPoolManager *buffer_pool_manager);

  void CopyFirstFrom(page_id_t value, BufferPoolManager *buffer_pool_manager);

  char data_[PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE];
};

using InternalPage = BPlusTreeInternalPage;
#endif  // MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H
