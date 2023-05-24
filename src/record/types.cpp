#include "record/types.h"
#include <stdexcept>

#include "common/macros.h"
#include "record/field.h"

inline int CompareStrings(const char *str1, int len1, const char *str2, int len2) {
  assert(str1 != nullptr);
  assert(len1 >= 0);
  assert(str2 != nullptr);
  assert(len2 >= 0);
  int ret = memcmp(str1, str2, static_cast<size_t>(std::min(len1, len2)));
  if (ret == 0 && len1 != len2) {
    ret = len1 - len2;
  }
  return ret;
}

// ==============================Type=============================

Type *Type::type_singletons_[] = {new Type(TypeId::kTypeInvalid), new TypeInt(), new TypeFloat(), new TypeChar()};//初始化单例静态初始化

uint32_t Type::SerializeTo(const Field &field, char *buf) const {
  switch (field.GetTypeId()) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->SerializeTo(field,buf);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->SerializeTo(field,buf);
    }
    case kTypeChar:{
      return type_singletons_[kTypeChar]->SerializeTo(field,buf);
    }
    case kTypeInvalid:{
      throw std::runtime_error("Serialize fail for valid type");
    }
  }
  throw std::runtime_error("Serialize fail in types.cpp");
}

uint32_t Type::DeserializeFrom(char *storage, Field **field, bool is_null) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->DeserializeFrom(storage,field,is_null);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->DeserializeFrom(storage,field,is_null);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->DeserializeFrom(storage,field,is_null);
    }
    case kTypeInvalid:{
      throw std::runtime_error("Serialize fail for invalid type");
    }
  }
  throw std::runtime_error("Serialize fail in types.cpp");
}

uint32_t Type::GetSerializedSize(const Field &field, bool is_null) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->GetSerializedSize(field,is_null);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->GetSerializedSize(field,is_null);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->GetSerializedSize(field,is_null);
    }
    case kTypeInvalid:{
      throw std::runtime_error("GetSerializedSize fail for invalid type");
    }
  }
  throw std::runtime_error("GetSerializedSize fail in types.cpp");
}

const char *Type::GetData(const Field &val) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->GetData(val);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->GetData(val);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->GetData(val);
    }
    case kTypeInvalid:{
      throw std::runtime_error("GetData fail for invalid type");
    }
  }
  throw std::runtime_error("GetData fail in types.cpp");
}

uint32_t Type::GetLength(const Field &val) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->GetLength(val);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->GetLength(val);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->GetLength(val);
    }
    case kTypeInvalid:{
      throw std::runtime_error("GetLength fail for invalid type");
    }
  }
  throw std::runtime_error("GetLength fail in types.cpp");
}

CmpBool Type::CompareEquals(const Field &left, const Field &right) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->CompareEquals(left,right);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->CompareEquals(left,right);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->CompareEquals(left,right);
    }
    case kTypeInvalid:{
      throw std::runtime_error("CompareEquals fail for invalid type");
    }
  }
  throw std::runtime_error("CompareEquals fail in types.cpp");
}

CmpBool Type::CompareNotEquals(const Field &left, const Field &right) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->CompareNotEquals(left,right);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->CompareNotEquals(left,right);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->CompareNotEquals(left,right);
    }
    case kTypeInvalid:{
      throw std::runtime_error("CompareNotEquals fail for invalid type");
    }
  }
  throw std::runtime_error("CompareNotEquals fail in types.cpp");
}

CmpBool Type::CompareLessThan(const Field &left, const Field &right) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->CompareLessThan(left,right);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->CompareLessThan(left,right);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->CompareLessThan(left,right);
    }
    case kTypeInvalid:{
      throw std::runtime_error("CompareLessThan fail for invalid type");
    }
  }
  throw std::runtime_error("CompareLessThan fail in types.cpp");
}

CmpBool Type::CompareLessThanEquals(const Field &left, const Field &right) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->CompareLessThanEquals(left,right);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->CompareLessThanEquals(left,right);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->CompareLessThanEquals(left,right);
    }
    case kTypeInvalid:{
      throw std::runtime_error("CompareLessThanEquals fail for invalid type");
    }
  }
  throw std::runtime_error("CompareLessThanEquals fail in types.cpp");
}

CmpBool Type::CompareGreaterThan(const Field &left, const Field &right) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->CompareGreaterThan(left,right);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->CompareGreaterThan(left,right);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->CompareGreaterThan(left,right);
    }
    case kTypeInvalid:{
      throw std::runtime_error("CompareGreaterThan fail for invalid type");
    }
  }
  throw std::runtime_error("CompareGreaterThan fail in types.cpp");
}

CmpBool Type::CompareGreaterThanEquals(const Field &left, const Field &right) const {
  switch (type_id_) {
    case kTypeInt:{
      return type_singletons_[kTypeInt]->CompareGreaterThanEquals(left,right);
    }
    case kTypeFloat:{
      return type_singletons_[kTypeFloat]->CompareGreaterThanEquals(left,right);
    }
    case kTypeChar:{//怎么获得字符串的长度？
      return type_singletons_[kTypeChar]->CompareGreaterThanEquals(left,right);
    }
    case kTypeInvalid:{
      throw std::runtime_error("CompareGreaterThanEquals fail for invalid type");
    }
  }
  throw std::runtime_error("CompareGreaterThanEquals fail in types.cpp");
}

// ==============================TypeInt=================================

uint32_t TypeInt::SerializeTo(const Field &field, char *buf) const {
  if (!field.IsNull()) {
    MACH_WRITE_TO(int32_t, buf, field.value_.integer_);
    return GetTypeSize(type_id_);
  }
  return 0;
}

uint32_t TypeInt::DeserializeFrom(char *storage, Field **field, bool is_null) const {
  if (is_null) {
    *field = new Field(TypeId::kTypeInt);
    return 0;
  }
  int32_t val = MACH_READ_FROM(int32_t, storage);
  *field = new Field(TypeId::kTypeInt, val);
  return GetTypeSize(type_id_);
}

uint32_t TypeInt::GetSerializedSize(const Field &field, bool is_null) const {
  if (is_null) {
    return 0;
  }
  return GetTypeSize(type_id_);
}

CmpBool TypeInt::CompareEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ == right.value_.integer_);
}

CmpBool TypeInt::CompareNotEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ != right.value_.integer_);
}

CmpBool TypeInt::CompareLessThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ < right.value_.integer_);
}

CmpBool TypeInt::CompareLessThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ <= right.value_.integer_);
}

CmpBool TypeInt::CompareGreaterThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ > right.value_.integer_);
}

CmpBool TypeInt::CompareGreaterThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ >= right.value_.integer_);
}

// ==============================TypeFloat=============================

uint32_t TypeFloat::SerializeTo(const Field &field, char *buf) const {
  if (!field.IsNull()) {
    MACH_WRITE_TO(float_t, buf, field.value_.float_);
    return GetTypeSize(type_id_);
  }
  return 0;
}

uint32_t TypeFloat::DeserializeFrom(char *storage, Field **field, bool is_null) const {
  if (is_null) {
    *field = new Field(TypeId::kTypeFloat);
    return 0;
  }
  float_t val = MACH_READ_FROM(float_t, storage);
  *field = new Field(TypeId::kTypeFloat, val);
  return GetTypeSize(type_id_);
}

uint32_t TypeFloat::GetSerializedSize(const Field &field, bool is_null) const {
  if (is_null) {
    return 0;
  }
  return GetTypeSize(type_id_);
}

CmpBool TypeFloat::CompareEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ == right.value_.float_);
}

CmpBool TypeFloat::CompareNotEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ != right.value_.float_);
}

CmpBool TypeFloat::CompareLessThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ < right.value_.float_);
}

CmpBool TypeFloat::CompareLessThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ <= right.value_.float_);
}

CmpBool TypeFloat::CompareGreaterThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ > right.value_.float_);
}

CmpBool TypeFloat::CompareGreaterThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ >= right.value_.float_);
}

// ==============================TypeChar=============================
uint32_t TypeChar::SerializeTo(const Field &field, char *buf) const {
  if (!field.IsNull()) {
    uint32_t len = GetLength(field);
    memcpy(buf, &len, sizeof(uint32_t));
    memcpy(buf + sizeof(uint32_t), field.value_.chars_, len);
    return len + sizeof(uint32_t);
  }
  return 0;
}

uint32_t TypeChar::DeserializeFrom(char *storage, Field **field, bool is_null) const {
  if (is_null) {
    *field = new Field(TypeId::kTypeChar);
    return 0;
  }
  uint32_t len = MACH_READ_UINT32(storage);
  *field = new Field(TypeId::kTypeChar, storage + sizeof(uint32_t), len, true);
  return len + sizeof(uint32_t);
}

uint32_t TypeChar::GetSerializedSize(const Field &field, bool is_null) const {
  if (is_null) {
    return 0;
  }
  uint32_t len = GetLength(field);
  return len + sizeof(uint32_t);
}

const char *TypeChar::GetData(const Field &val) const {
  return val.value_.chars_;
}

uint32_t TypeChar::GetLength(const Field &val) const {
  return val.len_;
}

CmpBool TypeChar::CompareEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) == 0);
}

CmpBool TypeChar::CompareNotEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) != 0);
}

CmpBool TypeChar::CompareLessThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) < 0);
}

CmpBool TypeChar::CompareLessThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) <= 0);
}

CmpBool TypeChar::CompareGreaterThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) > 0);
}

CmpBool TypeChar::CompareGreaterThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) >= 0);
}
