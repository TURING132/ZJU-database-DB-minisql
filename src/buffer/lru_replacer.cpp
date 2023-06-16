#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : cache(num_pages, victims_.end()) {
  maxVol = num_pages;
}

LRUReplacer::~LRUReplacer() = default;
/*
替换（即删除）与所有被跟踪的页相比最近最少被访问的页，
将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数frame_id中
输出并返回true，如果当前没有可以替换的元素则返回false
*/
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (victims_.empty()!=0) {
    return false;
  }
  else {
    *frame_id = victims_.back();
    cache[*frame_id] = victims_.end();
    victims_.pop_back();
    return true;
  }
}
/*
将数据页固定使之不能被Replacer替换，即从lru_list_中移除该数据页对应的页帧。
Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用；
*/
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto temp = cache[frame_id];
  if (temp != victims_.end()) {
    // 存在对应元素
    victims_.erase(temp);
    cache[frame_id] = victims_.end();
  }
}
/*
将数据页解除固定，放入lru_list_中，使之可以在必要时被Replacer替换掉。
Unpin函数应当在一个数据页的引用计数变为0时被Buffer Pool Manager调用，
使页帧对应的数据页能够在必要时被替换；
*/
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (victims_.size() >= maxVol ) {
    return;
  }
  else if (cache[frame_id] != victims_.end()){
    return ;
  }
  else{
    victims_.push_front(frame_id);
    cache[frame_id] = victims_.begin();
  }

}

size_t LRUReplacer::Size() { return victims_.size(); }