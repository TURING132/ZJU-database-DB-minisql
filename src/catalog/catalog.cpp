#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  /*| MAGIC_NUM | table_meta_pages_.size() | table_meta_pages_ | index_meta_pages_.size() | index_meta_pages_ |*/
  char* tmp=buf;
  MACH_WRITE_TO(uint32_t,tmp,CATALOG_METADATA_MAGIC_NUM);
  tmp+=sizeof(uint32_t);
  uint32_t length=table_meta_pages_.size();
  //去掉invalid
  for(auto it:table_meta_pages_){
    if(it.second == INVALID_PAGE_ID) length--;
  }
  MACH_WRITE_TO(uint32_t,tmp,length);
  tmp+=sizeof(uint32_t);
  //serialize table_meta_pages_
  for(auto itera = table_meta_pages_.begin();itera!=table_meta_pages_.end();++itera){
    if(itera->second<0)continue;
    MACH_WRITE_TO(uint32_t,tmp,itera->first);
    tmp+=sizeof(uint32_t);
    MACH_WRITE_TO(int32_t,tmp,itera->second);
    tmp+=sizeof(int32_t);
  }
  //serialize index_meta_pages_
  length=index_meta_pages_.size();
  for(auto it:table_meta_pages_){
    if(it.second == INVALID_PAGE_ID) length--;
  }
  MACH_WRITE_TO(uint32_t,tmp,length);
  tmp+=sizeof(uint32_t);
  for(auto itera=index_meta_pages_.begin();itera!=index_meta_pages_.end();++itera){
    if(itera->second<0) continue;
    MACH_WRITE_TO(uint32_t,tmp,itera->first);
    tmp+=sizeof(uint32_t);
    MACH_WRITE_TO(int32_t,tmp,itera->second);
    tmp+=sizeof(int32_t);
  }

}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  /*| MAGIC_NUM | table_meta_pages_.size() | table_meta_pages_ | index_meta_pages_.size() | index_meta_pages_ |*/
  // CatalogMeta* res = new(heap->Allocate(sizeof(CatalogMeta)))CatalogMeta;
  CatalogMeta* res=CatalogMeta::NewInstance(heap);
  if(res==nullptr)
    return nullptr;
  uint32_t mag_num=MACH_READ_FROM(uint32_t,buf);
  if(mag_num!=CATALOG_METADATA_MAGIC_NUM) return nullptr;
  uint32_t offset=sizeof(uint32_t);

  //table_meta
  uint32_t size_of_table_meta_pages=MACH_READ_FROM(uint32_t,buf+offset);//because of magic num
  // std::cout<<size_of_table_meta_pages<<std::endl;
  offset+=sizeof(uint32_t);
  for(int i=0;i<(int)size_of_table_meta_pages;++i){
    uint32_t table_id=MACH_READ_FROM(uint32_t,buf+offset);
    offset+=sizeof(uint32_t);
    int32_t page_id=MACH_READ_FROM(int32_t,buf+offset);
    offset+=sizeof(int32_t);
    res->table_meta_pages_[table_id] = page_id;
  }
  res->GetTableMetaPages()->emplace(size_of_table_meta_pages, INVALID_PAGE_ID);
  //index_meta
  uint32_t size_of_index_meta_pages=MACH_READ_FROM(uint32_t,buf+offset);
  // std::cout<<size_of_index_meta_pages<<std::endl;
  offset+=sizeof(uint32_t);
  for(int i=0;i<(int)size_of_index_meta_pages;++i){
    uint32_t index_id=MACH_READ_FROM(uint32_t,buf+offset);
    offset+=sizeof(uint32_t);
    int32_t page_id=MACH_READ_FROM(int32_t,buf+offset);
    offset+=sizeof(int32_t);
    res->index_meta_pages_[index_id] = page_id;
    // std::cout<<"**********"<<std::endl;
  }
  res->GetIndexMetaPages()->emplace(size_of_index_meta_pages, INVALID_PAGE_ID);
  return res;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  /*Consider MAGIC_NUM, size of table_meta_pages_ and size of index_meta_pages = 3*/
  return 3*sizeof(uint32_t)+table_meta_pages_.size()*2*sizeof(uint32_t)+index_meta_pages_.size()*2*sizeof(uint32_t);
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
      log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
  if(init == true){//初次创建时（init = true）初始化元数据
    catalog_meta_=CatalogMeta::NewInstance(heap_);
  }
  //并在后续重新打开数据库实例时，从数据库文件中加载所有的表和索引信息，
  //构建TableInfo和IndexInfo信息置于内存中
  else
  {
    Page* p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    char* tmp = p->GetData();
    TableInfo* tmpinfo;
    catalog_meta_ = CatalogMeta::DeserializeFrom(tmp,heap_);

    //tableinfo
    next_table_id_ = catalog_meta_->GetNextTableId();
    for(auto it:catalog_meta_->table_meta_pages_){
      if(it.second<0) continue;//if invalidpage(-1) continue
      p = buffer_pool_manager->FetchPage(it.second);
      tmp = p->GetData();
      TableMetadata* meta;
      meta->DeserializeFrom(tmp,meta, heap_);
      tmpinfo = TableInfo::Create(heap_);
      TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,meta->GetSchema(), nullptr, log_manager_, lock_manager_, heap_);
      tmpinfo->Init(meta, table_heap);//initialize
      table_names_[meta->GetTableName()] = meta->GetTableId();
      tables_[meta->GetTableId()] = tmpinfo;
      index_names_.insert({meta->GetTableName(), std::unordered_map<std::string, index_id_t>()});
    }
    //indexinfo
    next_index_id_ = catalog_meta_->GetNextIndexId();
    for(auto it:catalog_meta_->index_meta_pages_){
      if(it.second<0) continue;
      p = buffer_pool_manager->FetchPage(it.second);
      tmp = p->GetData();
      IndexMetadata* meta;
      IndexMetadata::DeserializeFrom(tmp, meta, heap_);
      tmpinfo = tables_[meta->GetTableId()];
      IndexInfo* index_info = IndexInfo::Create(heap_);
      index_info->Init(meta,tmpinfo,buffer_pool_manager_);//init
      index_names_[tmpinfo->GetTableName()][meta->GetIndexName()] = meta->GetIndexId();
      indexes_[meta->GetIndexId()] = index_info;
    }

  }
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if(table_names_.count(table_name) != 0)
    return DB_TABLE_ALREADY_EXIST;

  table_id_t table_id = next_table_id_++;
  page_id_t page_id=0;//分配tableMetaData的数据页
  Page* new_table_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->table_meta_pages_[next_table_id_] = -1;
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, page_id, schema, heap_);
  //create table_heap
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,schema, txn, log_manager_, lock_manager_, heap_);
  //init table_info
  table_info = TableInfo::Create(heap_);
  table_info->Init(table_meta, table_heap);
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;
  index_names_.insert({table_name, std::unordered_map<std::string, index_id_t>()});

  //serialize tablemeta to new tablepage
  table_meta->SerializeTo(new_table_page->GetData());
  buffer_pool_manager_->UnpinPage(page_id,true);
  //serialize tablemeta to page0
  catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(0)->GetData());
  if(table_meta!=nullptr && table_heap!=nullptr)
    return DB_SUCCESS;
  return DB_FAILED;

}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.count(table_name) <= 0)
    return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  if(tables_.size()==0)
    return DB_FAILED;
  tables.resize(tables_.size());//allocate memory
  uint32_t i=0;
  for(auto itera=tables_.begin();itera!=tables_.end();++itera,++i)
    tables[i]=itera->second;

  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  //judge if the table exists
  if(index_names_.count(table_name)<=0) return DB_TABLE_NOT_EXIST;
  //judge if the index already exists
  auto it = index_names_[table_name];
  if(it.count(index_name)>0) return DB_INDEX_ALREADY_EXIST;

  //get table information
  index_id_t index_id = next_index_id_++;
  std::vector<uint32_t> key_map;
  TableInfo* now_table;
  dberr_t error = GetTable(table_name,now_table);
  if(error) return error;
  for(int j=0;j<int(index_keys.size());j++){
    uint32_t keys_id;
    dberr_t err = now_table->GetSchema()->GetColumnIndex(index_keys[j], keys_id);
    if(err) return err;
    key_map.push_back(keys_id);
  }

  //create index_info and init
  IndexMetadata *indexmeta = IndexMetadata::Create(index_id, index_name, table_names_[table_name],key_map,heap_ );
  index_info = IndexInfo::Create(heap_);
  index_info->Init(indexmeta,now_table,buffer_pool_manager_);
  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = index_info;

  //将index的信息写入catalog_meta
  page_id_t page_id;//分配indexMetaData的数据页
  Page* new_index_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->index_meta_pages_[index_id] = page_id;
  catalog_meta_->index_meta_pages_[next_index_id_] = -1;

  //serialize
  indexmeta->SerializeTo(new_index_page->GetData());
  catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(0)->GetData());

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  //find table
  if(index_names_.find(table_name)==index_names_.end())
    return DB_TABLE_NOT_EXIST;

  //find index
  auto indname_id=index_names_.find(table_name)->second;
  if(indname_id.find(index_name)==indname_id.end())
    return DB_INDEX_NOT_FOUND;

  //have found and return index_info
  index_id_t index_id=indname_id[index_name];
  index_info=indexes_.find(index_id)->second;

  return DB_SUCCESS;
}
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  //find table index
  auto table_indexes = index_names_.find(table_name);
  if(table_indexes == index_names_.end())
    return DB_TABLE_NOT_EXIST;
  //update indexes
  auto indexes_map = table_indexes->second;
  for(auto it:indexes_map){
    indexes.push_back(indexes_.find(it.second)->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  //not found
  if(table_names_.find(table_name)==table_names_.end())
    return DB_TABLE_NOT_EXIST;
  //get table through hash map
  table_id_t table_id=table_names_[table_name];
  TableInfo* droptable=tables_[table_id];
  //drop
  if(droptable==nullptr)
    return DB_FAILED;
  tables_.erase(table_id);
  table_names_.erase(table_name);
  droptable->~TableInfo();
  return DB_SUCCESS;


}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto get=index_names_[table_name].find(index_name);
  //not found
  if(get==index_names_[table_name].end()){
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t drop_index_id=get->second;
  IndexInfo*  getindex=indexes_[drop_index_id];
  //first
  getindex->GetIndex()->Destroy();
  //second
  buffer_pool_manager_->DeletePage(catalog_meta_->GetIndexMetaPages()->find(drop_index_id)->second);
  catalog_meta_->GetIndexMetaPages()->erase(drop_index_id);

  Page* catalog_meta_page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  heap_->Free(getindex);

  indexes_.erase(drop_index_id);
  index_names_[table_name].erase(index_name);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  if(buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) return DB_SUCCESS;
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto it = tables_.find(table_id);
  if(it==tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = it->second;
  return DB_SUCCESS;
}