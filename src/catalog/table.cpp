#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  /*content: MAGIC_NUM | table_id_ | table_name_.length() | table_name_ | root_table_id_ |schema*/
  char* buffer = buf;
  //magic num
  MACH_WRITE_TO(uint32_t, buffer, TABLE_METADATA_MAGIC_NUM);
  buffer+=sizeof(uint32_t);
  //table_id
  MACH_WRITE_TO(table_id_t, buffer,table_id_);
  buffer+=sizeof(uint32_t);
  //table name
  uint32_t len = table_name_.size();
  memcpy(buffer, &len, sizeof(uint32_t));
  memcpy(buffer + sizeof(uint32_t), table_name_.c_str(), len);
  buffer = buffer + len + sizeof(uint32_t);
  //root table
  MACH_WRITE_TO(int32_t, buffer,root_page_id_);
  buffer+=sizeof(int32_t);

  buffer+=schema_->SerializeTo(buffer);

  return buffer-buf;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(table_id_)+sizeof(TABLE_METADATA_MAGIC_NUM)
         +MACH_STR_SERIALIZED_SIZE(table_name_)+sizeof(root_page_id_);
}


uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta) {
  if (table_meta != nullptr) {
    LOG(WARNING) << "Pointer object table info is not null in table info deserialize." << std::endl;
  }
  char *p = buf;
  // magic num
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == TABLE_METADATA_MAGIC_NUM, "Failed to deserialize table info.");
  // table id
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
  buf += 4;
  // table name
  uint32_t len = MACH_READ_UINT32(buf);
  buf += 4;
  std::string table_name(buf, len);
  buf += len;
  // table heap root page id
  page_id_t root_page_id = MACH_READ_FROM(page_id_t, buf);
  buf += 4;
  // table schema
  TableSchema *schema = nullptr;
  buf += TableSchema::DeserializeFrom(buf, schema);
  // allocate space for table metadata
  table_meta = new TableMetadata(table_id, table_name, root_page_id, schema);
  return buf - p;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema) {
  return new TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
