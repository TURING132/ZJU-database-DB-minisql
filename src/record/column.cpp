#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* DONE: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // magic_num + name_len + name_ + type + len + table_ind_ + nullable + unique
  int cur = 0;
  memcpy(buf+cur,&COLUMN_MAGIC_NUM,sizeof(COLUMN_MAGIC_NUM));cur+=sizeof(COLUMN_MAGIC_NUM);
  size_t len = name_.length();
  memcpy(buf+cur, &len, sizeof(len));cur+=sizeof(len);
  memcpy(buf+cur, name_.c_str(), name_.length());cur+=name_.length();
  memcpy(buf+cur, &type_, sizeof(type_));cur+=sizeof(type_);
  memcpy(buf+cur, &len_, sizeof(len_));cur+=sizeof(len_);
  memcpy(buf+cur, &table_ind_, sizeof(table_ind_));cur+=sizeof(table_ind_);
  memcpy(buf+cur, &nullable_, sizeof(nullable_));cur+=sizeof(nullable_);
  memcpy(buf+cur, &unique_, sizeof(unique_));cur+=sizeof(unique_);
  return cur;
}

/**
 * DONE: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return sizeof(COLUMN_MAGIC_NUM)+sizeof(size_t)+name_.length()+sizeof(type_)+sizeof(len_)+sizeof(table_ind_)+sizeof(nullable_)+sizeof(unique_);
}

/**
 * DONE: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  int cur = 0;
  uint32_t  magic = MACH_READ_INT32(buf+cur);cur+=sizeof(uint32_t);
  ASSERT(magic==COLUMN_MAGIC_NUM,"column deserialize wrong, not a colum buffer");

  size_t name_length = MACH_READ_FROM(size_t,buf+cur);cur+=sizeof (size_t);
  char name_buffer[name_length]; memcpy(name_buffer,buf+cur,name_length);
  std::string name(name_buffer,name_length/(sizeof(char)));cur+=name_length;
  TypeId type;memcpy(&type,buf+cur,sizeof(type));cur+=sizeof(type);
  uint32_t len = MACH_READ_INT32(buf+cur);cur+=sizeof(uint32_t);
  uint32_t index = MACH_READ_INT32(buf+cur);cur+=sizeof(uint32_t);
  bool nullable = MACH_READ_FROM(bool,buf+cur);cur+=sizeof(bool);
  bool unique = MACH_READ_FROM(bool,buf+cur);cur+= sizeof(bool);
  if(type==kTypeChar){
    column = new Column(name,type,len,index,nullable,unique);
  }else{
    column = new Column(name,type,index,nullable,unique);
  }


  return cur;
}
