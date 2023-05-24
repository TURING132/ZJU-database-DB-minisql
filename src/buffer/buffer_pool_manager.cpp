#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);//新建page列表，全在free_list_中
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

// 1.     Search the page table for the requested page (P).
// 1.1    If P exists, pin it and return it immediately.
// 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
//        Note that pages are always found from the free list first.
// 2.     If R is dirty, write it back to the disk.
// 3.     Delete R from the page table and insert P.
// 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  frame_id_t tmp;
  
  //If P exists, pin it and return it immediately.
  if(page_table_.count(page_id)>0){
    tmp = page_table_[page_id];
    replacer_->Pin(tmp);
    pages_[tmp].pin_count_++;
    return &pages_[tmp];
  }
  //p dos not exist
  if(free_list_.size()>0){//from freelist
    tmp = free_list_.front();//pick from the head of the free list
    page_table_[page_id] = tmp;//update page_table_
    free_list_.pop_front();////delete the frame id from free list

    disk_manager_->ReadPage(page_id, pages_[tmp].data_);
    pages_[tmp].pin_count_ = 1;
    pages_[tmp].page_id_ = page_id;
    
    return &pages_[tmp];
  }
  else{//from replacer
    bool flag = replacer_->Victim(&tmp);
    if(flag==false) return nullptr;
    if(pages_[tmp].IsDirty()){//write back to the disk
      disk_manager_->WritePage(pages_[tmp].GetPageId(),pages_[tmp].GetData());
    }
    //update the pages_
    pages_[tmp].page_id_ = page_id;
    pages_[tmp].pin_count_ = 1;
    page_table_[page_id] = tmp;
    //readpage from disk
    disk_manager_->ReadPage(page_id,pages_[tmp].data_);
    return &pages_[tmp];
  }
  
  return nullptr;
}

// 0.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 1.   If all the pages in the buffer pool are pinned, return nullptr.
// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 3.   Update P's metadata, zero out memory and add P to the page table.
// 4.   Set the page ID output parameter. Return a pointer to P.
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  page_id = 0;
  frame_id_t tmp;
  if(free_list_.size()>0){//freelist first
    page_id = AllocatePage();
    tmp = free_list_.front();//pick from the head of the free list
    free_list_.pop_front();//delete the frame id from free list
  }
  else{//replacer
    bool flag = replacer_->Victim(&tmp);
    if(flag==false) return nullptr;//如果replacer里也没有
    page_id = AllocatePage();//new page_id
    if(pages_[tmp].IsDirty()){
      disk_manager_->WritePage(pages_[tmp].GetPageId(),pages_[tmp].GetData());
    }
    page_table_.erase(pages_[tmp].page_id_);//由于替换了page_id，要删除相应old值
  }
  //Update P's metadata
  pages_[tmp].ResetMemory();
  pages_[tmp].page_id_ = page_id;
  pages_[tmp].pin_count_ = 1;
  page_table_[page_id] = tmp;
  return &pages_[tmp];
}
// 0.   Make sure you call DeallocatePage!
// 1.   Search the page table for the requested page (P).
// 1.   If P does not exist, return true.
// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  if(page_table_.count(page_id)==0)
    return true;
  //Search the page table for the requested page
  frame_id_t tmp = page_table_[page_id];
  //If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if(pages_[tmp].pin_count_>0)
    return false;
  //delete
  page_table_.erase(page_id);
  pages_[tmp].ResetMemory();
  pages_[tmp].page_id_=INVALID_PAGE_ID;
  pages_[tmp].is_dirty_=false;
  free_list_.push_back(tmp);
  disk_manager_->DeAllocatePage(page_id);//call DeallocatePage
  return true;
 
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_table_.count(page_id)==0) 
    return false;
  frame_id_t tmp = page_table_[page_id];
  if(pages_[tmp].pin_count_==0) 
    return true;
  pages_[tmp].pin_count_--;
  replacer_->Unpin(tmp);
  if(is_dirty) pages_[tmp].is_dirty_ = true;
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  latch_.lock();
  frame_id_t tmp = page_table_[page_id];
  if(page_table_.count(page_id)>0){
    disk_manager_->WritePage(page_id, pages_[tmp].data_);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}