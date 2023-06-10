#include "catalog/indexes.h"

IndexMetadata::IndexMetadata(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                             const std::vector<uint32_t> &key_map)
    : index_id_(index_id), index_name_(index_name), table_id_(table_id), key_map_(key_map) {}

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name, const table_id_t table_id,
                                     const vector<uint32_t> &key_map) {
  return new IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  /*content: MAGIC_NUM | index_id_ | index_name_ | table_id_ | key_map_ */
  uint32_t offset=0;
  //magic num
  MACH_WRITE_TO(uint32_t,buf,INDEX_METADATA_MAGIC_NUM);
  offset=sizeof(INDEX_METADATA_MAGIC_NUM);
  //index_id
  MACH_WRITE_TO(uint32_t,buf+offset,index_id_);
  offset+=sizeof(index_id_);
  //index_name_size
  MACH_WRITE_TO(uint32_t,buf+offset,index_name_.size());
  offset+=sizeof(uint32_t);
  //index_name
  MACH_WRITE_STRING(buf+offset,index_name_);
  offset+=index_name_.size();
  //table_id
  MACH_WRITE_TO(uint32_t,buf+offset,table_id_);
  offset+=sizeof(table_id_);
  //key_map_size
  MACH_WRITE_TO(uint32_t,buf+offset,key_map_.size());
  offset+=sizeof(uint32_t);

  //serialize key_map_
  for(uint32_t i=0;i<key_map_.size();++i){
    MACH_WRITE_TO(uint32_t,buf+offset,key_map_[i]);
    offset+=sizeof(uint32_t);
  }

  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return index_name_.size()+4*key_map_.size()+4*5;
}


uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta) {
  if (index_meta != nullptr) {
    LOG(WARNING) << "Pointer object index info is not null in table info deserialize." << std::endl;
  }
  char *p = buf;
  // magic num
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == INDEX_METADATA_MAGIC_NUM, "Failed to deserialize index info.");
  // index id
  index_id_t index_id = MACH_READ_FROM(index_id_t, buf);
  buf += 4;
  // index name
  uint32_t len = MACH_READ_UINT32(buf);
  buf += 4;
  std::string index_name(buf, len);
  buf += len;
  // table id
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
  buf += 4;
  // index key count
  uint32_t index_key_count = MACH_READ_UINT32(buf);
  buf += 4;
  // key mapping in table
  std::vector<uint32_t> key_map;
  for (uint32_t i = 0; i < index_key_count; i++) {
    uint32_t key_index = MACH_READ_UINT32(buf);
    buf += 4;
    key_map.push_back(key_index);
  }
  // allocate space for index meta data
  index_meta = new IndexMetadata(index_id, index_name, table_id, key_map);
  return buf - p;
}
