#include "page/bitmap_page.h"

#include "glog/logging.h"


template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_>=MAX_CHARS*8){
    return false;//full or invalid offset
  }
  uint32_t n_byte_index = next_free_page_ / 8;
  uint8_t n_bit_index = next_free_page_ % 8;
  bytes[n_byte_index] |= (1 << n_bit_index);
  page_offset = next_free_page_;
  for(uint32_t i=0;i<MAX_CHARS*8;i++){
    uint32_t byte_index = i / 8;
    uint8_t bit_index = i % 8;
    if(IsPageFreeLow(byte_index,bit_index)){
      next_free_page_ = i;
      break ;
    }
  }
  page_allocated_+=1;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(page_offset>=MAX_CHARS*8)return false;
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  if((IsPageFreeLow(byte_index,bit_index))){
    return false;//the page has not been allocated
  }
  bytes[byte_index] &= ~(1 << bit_index);
  page_allocated_-=1;
  next_free_page_ = page_offset;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset>=MAX_CHARS*8)return false;
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  if(IsPageFreeLow(byte_index,bit_index)){
    return true;
  }else{
    return false;
  }
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return !(bytes[byte_index] & (1<<bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;