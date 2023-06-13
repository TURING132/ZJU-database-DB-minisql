#include "storage/table_heap.h"

/**
 * TODO: USE Transaction and lock
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  uint32_t tuple_size = row.GetSerializedSize(schema_);
  if(tuple_size>PAGE_SIZE){
    return false;
  }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if(page== nullptr)return false;
  page->WLatch();
  bool insertResult = page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  while(!insertResult){//break if success
    page_id_t next_id = page->GetNextPageId();
    if(next_id!=INVALID_PAGE_ID){
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_id));
      page->WLatch();
      insertResult = page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }else{
      //attach a page
      page_id_t new_page_id;
      buffer_pool_manager_->NewPage(new_page_id);
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(new_page_id));
      new_page->WLatch();
      new_page->Init(new_page_id,page->GetPageId(),log_manager_,txn);
      new_page->SetNextPageId(INVALID_PAGE_ID);
      page->WLatch();
      page->SetNextPageId(new_page_id);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      insertResult = new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
      new_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
      break ;
    }
  }
  if(insertResult)return true;
  else return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  auto old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  old_page->WLatch();
  Row* old_row = new Row(rid);
  old_page->GetTuple(old_row,schema_,txn,lock_manager_);
  bool update_result = old_page->UpdateTuple(row,old_row,schema_,txn,lock_manager_,log_manager_);
  old_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(old_page->GetPageId(), true);
  return update_result;
}

/**
 * TODO: USE Transaction and lock
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  ASSERT(page!= nullptr,"page not found when delete");
  // Otherwise, apply delete
  page->WLatch();
  page->ApplyDelete(rid,txn,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  page->RLatch();
  bool get_result = page->GetTuple(row,schema_,txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  return get_result;
}

/*define by liliyang*/
RowId TableHeap::GetNextRowId(Row *row, Transaction *txn) {
  RowId r_id = row->GetRowId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(r_id.GetPageId()));
  page->RLatch();
  RowId n_id;
  bool get_result = page->GetNextTupleRid(r_id,&n_id);
  if(get_result){//如果在当前页有下一条记录
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return n_id;
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  //看下一页
  auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  if(next_page==NULL){
      return INVALID_ROWID;//最后一条记录了，返回
  }

  while(true){
      next_page->RLatch();
      get_result = next_page->GetFirstTupleRid(&r_id);
      if(get_result){//一旦获取成功就返回
        buffer_pool_manager_->UnpinPage(next_page->GetPageId(), false);
        next_page->RUnlatch();
        return r_id;
      }
      //否则继续寻找下一页
      buffer_pool_manager_->UnpinPage(next_page->GetPageId(), false);
      next_page->RUnlatch();
      next_page =  reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page->GetNextPageId()));
      if(next_page==NULL){
        return INVALID_ROWID;//最后一条记录了，返回
      }
  }
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */


TableIterator TableHeap::Begin(Transaction *txn) {
  auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId rid;temp_table_page->GetFirstTupleRid(&rid);
  return TableIterator(this,rid);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(this,INVALID_ROWID);
}
