#include <algorithm>
#include "index/b_plus_tree_index.h"

#include "index/generic_key.h"
#include "utils/tree_file_mgr.h"
BPlusTreeIndex::BPlusTreeIndex(index_id_t index_id, IndexSchema *key_schema, size_t key_size,
                               BufferPoolManager *buffer_pool_manager)
    : Index(index_id, key_schema),
      processor_(key_schema_, key_size),
      container_(index_id, buffer_pool_manager, processor_) {}

dberr_t BPlusTreeIndex::InsertEntry(const Row &key, RowId row_id, Transaction *txn) {
  // ASSERT(row_id.Get() != INVALID_ROWID.Get(), "Invalid row id for index insert.");
  GenericKey *index_key = processor_.InitKey();
  processor_.SerializeFromKey(index_key, key, key_schema_);

  bool status = container_.Insert(index_key, row_id, txn);
  delete index_key;
  //  TreeFileManagers mgr("tree_");
  //  static int i = 0;
  //  if (i % 10 == 0) container_.PrintTree(mgr[i]);
  //  i++;

  if (!status) {
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t BPlusTreeIndex::RemoveEntry(const Row &key, RowId row_id, Transaction *txn) {
  GenericKey *index_key = processor_.InitKey();
  processor_.SerializeFromKey(index_key, key, key_schema_);

  container_.Remove(index_key, txn);
  delete index_key;
  return DB_SUCCESS;
}

dberr_t BPlusTreeIndex::ScanKey(const Row &key, vector<RowId> &result, Transaction *txn, string compare_operator) {
  GenericKey *index_key = processor_.InitKey();
  processor_.SerializeFromKey(index_key, key, key_schema_);
  if (compare_operator == "=") {
    container_.GetValue(index_key, result, txn);
  } else if (compare_operator == ">") {
    auto iter = GetBeginIterator(index_key);
    if (container_.GetValue(index_key, result, txn))
      ++iter;
    for (; iter != GetEndIterator(); ++iter) {
      result.emplace_back((*iter).second);
    }
  } else if (compare_operator == ">=") {
    for (auto iter = GetBeginIterator(index_key); iter != GetEndIterator(); ++iter) {
      result.emplace_back((*iter).second);
    }
  } else if (compare_operator == "<") {
    for (auto iter = GetBeginIterator(); iter != GetBeginIterator(index_key); ++iter) {
      result.emplace_back((*iter).second);
    }
  } else if (compare_operator == "<=") {
    for (auto iter = GetBeginIterator(); iter != GetBeginIterator(index_key); ++iter) {
      result.emplace_back((*iter).second);
    }
    container_.GetValue(index_key, result, txn);
  } else if (compare_operator == "<>") {
    for (auto iter = GetBeginIterator(); iter != GetEndIterator(); ++iter) {
      result.emplace_back((*iter).second);
    }
    vector<RowId> temp;
    if (container_.GetValue(index_key, temp, txn))
      result.erase(find(result.begin(), result.end(), temp[0]));
  }
  delete index_key;
  if (!result.empty())
    return DB_SUCCESS;
  else
    return DB_KEY_NOT_FOUND;
}

dberr_t BPlusTreeIndex::Destroy() {
  container_.Destroy();
  return DB_SUCCESS;
}

IndexIterator BPlusTreeIndex::GetBeginIterator() {
  return container_.Begin();
}

IndexIterator BPlusTreeIndex::GetBeginIterator(GenericKey *key) {
  return container_.Begin(key);
}

IndexIterator BPlusTreeIndex::GetEndIterator() {
  return container_.End();
}