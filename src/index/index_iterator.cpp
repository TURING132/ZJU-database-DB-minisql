#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  //每遍历到一个页面pin住，unpin在重载的++运算符中
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id));
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return  page->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {
  if(item_index != page->GetSize() - 1) item_index++;
  else{
    int next_id = page->GetNextPageId();
    //unpin上一个page
    buffer_pool_manager->UnpinPage(page->GetPageId(), false);
    page = reinterpret_cast<IndexIterator::LeafPage *>(buffer_pool_manager->FetchPage(next_id));
    item_index = 0;
    current_page_id = next_id;
  }
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}