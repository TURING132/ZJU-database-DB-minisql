#include "record/schema.h"

/**
 * DONE: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  //form: magic + col_length + &column
  uint32_t cur=0;
  memcpy(buf+cur,&SCHEMA_MAGIC_NUM,sizeof(SCHEMA_MAGIC_NUM));cur+=sizeof(SCHEMA_MAGIC_NUM);
  uint32_t columns_count = static_cast<uint32_t>(columns_.size());
  memcpy(buf+cur,&columns_count,sizeof(columns_count));cur+=sizeof (columns_count);
  for(const auto * col : columns_){
    cur+=col->SerializeTo(buf+cur);
  }
  return cur;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;
  size += sizeof (SCHEMA_MAGIC_NUM);
  size += sizeof (uint32_t);
  for(const auto * col: columns_){
    size+=col->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t cur = 0;
  uint32_t magic;
  memcpy(&magic,buf+cur,sizeof(uint32_t));cur+=sizeof(uint32_t);
  ASSERT(magic==SCHEMA_MAGIC_NUM,"DeserializeFrom for invalid schema buf");

  uint32_t col_count;
  memcpy(&col_count,buf+cur,sizeof(uint32_t));cur+=sizeof(uint32_t);

  std::vector<Column *>columns;
  columns.reserve(col_count);
  for(uint32_t i=0;i<col_count;i++){
    Column * col;
    cur += Column::DeserializeFrom(buf+cur,col);
    columns.push_back(col);
  }
  schema = new Schema(columns, true);//默认is_manage都是true，是否manage不应该在这里判断
  return cur;
}