#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
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

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  //some param used for create indexinfo
  /*content: MAGIC_NUM | index_id_ | index_name_ | table_id_ | key_map_ */
  index_id_t index_id;
  string index_name;
  table_id_t table_id;
  vector<uint32_t> key_map;
  uint32_t offset=sizeof(uint32_t);
  //index_id
  index_id=MACH_READ_FROM(uint32_t,buf+offset);
  offset+=sizeof(uint32_t);
  //indexname
  char tmp[50000];
  uint32_t len_of_indexname=MACH_READ_FROM(uint32_t,buf+offset);
  offset+=sizeof(uint32_t);
  memcpy(tmp,buf+offset,len_of_indexname);
  tmp[len_of_indexname]='\0';
  offset+=len_of_indexname;
  //table_id
  table_id=MACH_READ_FROM(uint32_t,buf+offset);
  offset+=sizeof(uint32_t);
  //key_map
  uint32_t key_map_size=MACH_READ_FROM(uint32_t,buf+offset);
  for(int i=0;i<(int)key_map_size;++i){
    uint32_t tmp2=MACH_READ_FROM(uint32_t,buf+offset);
    offset+=sizeof(uint32_t);
    key_map.push_back(tmp2);
  }
  index_meta = IndexMetadata::Create(index_id, tmp, table_id, key_map, heap);
  return offset;
}