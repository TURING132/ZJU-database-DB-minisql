#ifndef MINISQL_GENERIC_KEY_H
#define MINISQL_GENERIC_KEY_H

#include <cstring>

#include "record/field.h"
#include "record/row.h"

class GenericKey {
  friend class KeyManager;
  char data[0];
};

class KeyManager {
 public: /**/
  [[nodiscard]] inline GenericKey *InitKey() const {
    return (GenericKey *)malloc(key_size_);  // remember delete
  }

  inline void SerializeFromKey(GenericKey *key_buf, const Row &key, Schema *schema) const {
    // initialize to 0
    [[maybe_unused]] uint32_t size = key.GetSerializedSize(schema);
    ASSERT(key.GetFieldCount() == schema->GetColumnCount(), "field nums not match.");
    ASSERT(size <= (uint32_t)key_size_, "Index key size exceed max key size.");
    memset(key_buf->data, 0, key_size_);
    key.SerializeTo(key_buf->data, schema);
  }

  inline void DeserializeToKey(const GenericKey *key_buf, Row &key, Schema *schema) const {
    [[maybe_unused]] uint32_t ofs = key.DeserializeFrom(const_cast<char *>(key_buf->data), schema);
    ASSERT(ofs <= (uint32_t)key_size_, "Index key size exceed max key size.");
  }

  // compare
  [[nodiscard]] inline int CompareKeys(const GenericKey *lhs, const GenericKey *rhs) const {
    //    ASSERT(malloc_usable_size((void *)&lhs) == malloc_usable_size((void *)&rhs), "key size not match.");
    uint32_t column_count = key_schema_->GetColumnCount();
    Row lhs_key(INVALID_ROWID);
    Row rhs_key(INVALID_ROWID);
    DeserializeToKey(lhs, lhs_key, key_schema_);
    DeserializeToKey(rhs, rhs_key, key_schema_);

    for (uint32_t i = 0; i < column_count; i++) {
      Field *lhs_value = lhs_key.GetField(i);
      Field *rhs_value = rhs_key.GetField(i);

      if (lhs_value->CompareLessThan(*rhs_value) == CmpBool::kTrue) {
        if (key_schema_->GetColumn(i)->GetType() == kTypeChar) {
          delete lhs_value->GetData();
          delete rhs_value->GetData();
        }
        return -1;
      }

      if (lhs_value->CompareGreaterThan(*rhs_value) == CmpBool::kTrue) {
        if (key_schema_->GetColumn(i)->GetType() == kTypeChar) {
          delete lhs_value->GetData();
          delete rhs_value->GetData();
        }
        return 1;
      }
    }
    // equals
    return 0;
  }

  inline int GetKeySize() const { return key_size_; }

  KeyManager(const KeyManager &other) {
    this->key_schema_ = other.key_schema_;
    this->key_size_ = other.key_size_;
  }

  // constructor
  KeyManager(Schema *key_schema, size_t key_size) : key_size_(key_size), key_schema_(key_schema) {}

 private:
  int key_size_;
  Schema *key_schema_;
};

#endif  // MINISQL_GENERIC_KEY_H
