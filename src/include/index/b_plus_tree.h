#ifndef MINISQL_B_PLUS_TREE_H
#define MINISQL_B_PLUS_TREE_H

#include <queue>
#include <string>
#include <vector>

#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_page.h"
#include "transaction/transaction.h"

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage;
  using LeafPage = BPlusTreeLeafPage;

 public:
  explicit BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &comparator,
                     int leaf_max_size = UNDEFINED_SIZE, int internal_max_size = UNDEFINED_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(GenericKey *key, const RowId &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const GenericKey *key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction = nullptr);

  IndexIterator Begin();

  IndexIterator Begin(const GenericKey *key);

  IndexIterator End();

  // expose for test purpose
  Page *FindLeafPage(const GenericKey *key, page_id_t page_id = INVALID_PAGE_ID, bool leftMost = false);

  // used to check whether all pages are unpinned
  bool Check();

  // destroy the b plus tree
  void Destroy(page_id_t current_page_id = INVALID_PAGE_ID);

  void PrintTree(std::ofstream &out) {
    if (IsEmpty()) {
      return;
    }
    out << "digraph G {" << std::endl;
    Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto *node = reinterpret_cast<BPlusTreePage *>(root_page);
    ToGraph(node, buffer_pool_manager_, out);
    out << "}" << std::endl;
  }

 private:
  void StartNewTree(GenericKey *key, const RowId &value);

  bool InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  LeafPage *Split(LeafPage *node, Transaction *transaction);

  InternalPage *Split(InternalPage *node, Transaction *transaction);

  template <typename N>
  bool CoalesceOrRedistribute(N *&node, Transaction *transaction = nullptr);

  bool Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                Transaction *transaction = nullptr);

  bool Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                Transaction *transaction = nullptr);

  void Redistribute(LeafPage *neighbor_node, LeafPage *node, int index);

  void Redistribute(InternalPage *neighbor_node, InternalPage *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  index_id_t index_id_;
  page_id_t root_page_id_{INVALID_PAGE_ID};
  BufferPoolManager *buffer_pool_manager_;
  KeyManager processor_;
  int leaf_max_size_;
  int internal_max_size_;
};

#endif  // MINISQL_B_PLUS_TREE_H
