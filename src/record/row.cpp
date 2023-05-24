#include "record/row.h"

/**
 * TODO: Student Implement
 */
//field_num+null_bitmap+field1+field2 ...
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t cur = 0;
  uint32_t field_num = fields_.size();
  memcpy(buf+cur,&field_num,sizeof(uint32_t));cur+=sizeof(uint32_t);

  uint32_t bitmap_len = (field_num +7)/8;//取上整个字节
  char* bitmap = buf+cur;
  memset(bitmap,0,bitmap_len);//初始化为0
  cur+=bitmap_len;

  for(uint32_t i=0;i<field_num;i++){
    Field *field = fields_[i];
    if(field->IsNull()){
      bitmap[i/8] |= (1<<(i%8));
    }else{
      cur+=field->SerializeTo(buf+cur);
    }
  }

  return cur;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");//只有空的row可以用这个方法解析
  uint32_t cur=0;
  uint32_t field_num;
  memcpy(&field_num,buf+cur,sizeof(uint32_t));cur+=sizeof(uint32_t);

  char* bitmap = buf+cur;
  uint32_t bitmap_len = (field_num+7)/8;
  memset(bitmap,0,bitmap_len);cur+=bitmap_len;

  fields_.clear();fields_.resize(field_num);
  for(uint32_t i=0;i<field_num;i++){
    if(bitmap[i/8]&(1<<(i%8))){
      cur+=Field::DeserializeFrom(buf+cur,schema->GetColumns()[i]->GetType(),&fields_[i], true);
    }else{
      cur+=Field::DeserializeFrom(buf+cur,schema->GetColumns()[i]->GetType(),&fields_[i],false);
    }
  }
  return cur;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
  size+=sizeof(uint32_t);
  size+=(fields_.size()+7)/8;
  for(uint32_t i=0;i<fields_.size();i++){
    if(!fields_[i]->IsNull()){
      size+=fields_[i]->GetSerializedSize();
    }
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
