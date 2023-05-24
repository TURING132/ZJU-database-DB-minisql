#include "page/b_plus_tree_page.h"

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
/**
 * TODO: Student Implement
 */
bool BPlusTreePage::IsLeafPage() const { return page_type_ == IndexPageType::LEAF_PAGE; }

/**
 * TODO: Student Implement
 */
 //root只依赖此判断，root在没有孩子的时候是leaf而不是internal
bool BPlusTreePage::IsRootPage() const { return GetParentPageId() == INVALID_PAGE_ID; }

/**
 * TODO: Student Implement
 */
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type = page_type_; }

int BPlusTreePage::GetKeySize() const { return key_size_; }

void BPlusTreePage::SetKeySize(int size) { key_size_ = size; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const { return size_; }

void BPlusTreePage::SetSize(int size) { size_ = size; }

void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
/**
 * TODO: Student Implement
 */
int BPlusTreePage::GetMaxSize() const { return max_size_; }

/**
 * TODO: Student Implement
 */
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
/**
 * TODO: Student Implement
 */
int BPlusTreePage::GetMinSize() const {
  if (IsRootPage() && IsLeafPage())
    return 0;
  else if (IsRootPage())
    return 2;
  else
    return (max_size_ + 1) / 2;//Leaf和非Root的节点的min_size都是max_size/2取上整
}

/*
 * Helper methods to get/set parent page id
 */
/**
 * TODO: Student Implement
 */
page_id_t BPlusTreePage::GetParentPageId() const { return parent_page_id_; }

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const { return page_id_; }

void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }