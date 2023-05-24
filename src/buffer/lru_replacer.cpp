#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/*
替换（即删除）与所有被跟踪的页相比最近最少被访问的页，
将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数frame_id中
输出并返回true，如果当前没有可以替换的元素则返回false
*/
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list_.size()==0){
    return false; 
  }else{
    *frame_id=lru_list_.back();//least recently used
    lru_hash.erase(*frame_id);//erase it from lru_hash
    lru_list_.pop_back();
    return true;
  }
}
/*
将数据页固定使之不能被Replacer替换，即从lru_list_中移除该数据页对应的页帧。
Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用；
*/
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(!lru_hash.count(frame_id)){
    return;
  }
  else{//exist
    auto tmp=lru_hash[frame_id];
    lru_hash.erase(frame_id);//remove from the lrulist
    lru_list_.erase(tmp);
  }
}
/*
将数据页解除固定，放入lru_list_中，使之可以在必要时被Replacer替换掉。
Unpin函数应当在一个数据页的引用计数变为0时被Buffer Pool Manager调用，
使页帧对应的数据页能够在必要时被替换；
*/
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(lru_hash.count(frame_id)){
    return;
  }else if(lru_list_.size()==capacity){//list is full
    return;
  }else{
    lru_list_.push_front(frame_id);//put into the head
    auto p=lru_list_.begin();
    lru_hash.emplace(frame_id,p);//insert into the lru hash
  }
}
/*
此方法返回当前LRUReplacer中能够被替换的数据页的数量
*/
size_t LRUReplacer::Size() {
  return lru_list_.size();
}