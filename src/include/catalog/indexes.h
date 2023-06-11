#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "common/macros.h"
#include "common/rowid.h"
#include "index/b_plus_tree_index.h"
#include "index/generic_key.h"
#include "record/schema.h"

class IndexMetadata {
  friend class IndexInfo;

 public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                               const std::vector<uint32_t> &key_map);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

 private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                         const std::vector<uint32_t> &key_map);

 private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_; /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
 public:
  static IndexInfo *Create() { return new IndexInfo(); }

  ~IndexInfo() {
    delete meta_data_;
    delete index_;
    delete key_schema_;
  }

  /**
 * TODO: Student Implement
   */
  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    // Step2: mapping index key to key schema
    // Step3: call CreateIndex to create the index
    //ASSERT(false, "Not Implemented yet.");
    // Step1: init index metadata and table info
    this->meta_data_ = meta_data;
    this->table_info_ = table_info;
    // Step2: mapping index key to key schema
    // 实例化对应变量
    key_schema_ = Schema::ShallowCopySchema(table_info->GetSchema(), meta_data->key_map_);
    // Step3: call CreateIndex to create the index
    index_ = CreateIndex(buffer_pool_manager,"bptree");
  }

  inline Index *GetIndex() { return index_; }

  std::string GetIndexName() { return meta_data_->GetIndexName(); }

  IndexSchema *GetIndexKeySchema() { return key_schema_; }

 private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, key_schema_{nullptr} {}
//  Index *CreateIndex(BufferPoolManager *buffer_pool_manager)
//  {
//    return ALLOC_P(heap_, BPTIndex<64>)(meta_data_->index_id_, key_schema_, buffer_pool_manager);
//  }

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager, const string &index_type){
    size_t max_size = 0;
    for (auto col : key_schema_->GetColumns()) {
      max_size += col->GetLength();
    }

    if (index_type == "bptree") {
      if (max_size <= 8)
        max_size = 16;
      else if (max_size <= 24)
        max_size = 32;
      else if (max_size <= 56)
        max_size = 64;
      else if (max_size <= 120)
        max_size = 128;
      else if (max_size <= 248)
        max_size = 256;
      else {
        LOG(ERROR) << "GenericKey size is too large";
        return nullptr;
      }
    } else {
      return nullptr;
    }
    return new BPlusTreeIndex(meta_data_->index_id_, key_schema_, max_size, buffer_pool_manager);
  }


 private:
  IndexMetadata *meta_data_;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
};

#endif  // MINISQL_INDEXES_H
