#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : cache(num_pages, victims_.end()) { maxVol = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (!victims_.empty()) {
    *frame_id = victims_.back();
    cache[*frame_id] = victims_.end();
    victims_.pop_back();
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  auto temp = cache[frame_id];
  if (temp != victims_.end()) {
    // 存在对应元素
    victims_.erase(temp);
    cache[frame_id] = victims_.end();
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (victims_.size() >= maxVol || cache[frame_id] != victims_.end()) {
    return;
  }
  victims_.push_front(frame_id);
  cache[frame_id] = victims_.begin();
}

size_t LRUReplacer::Size() { return victims_.size(); }