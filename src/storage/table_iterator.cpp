#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * Don't use iterator! not done!
 *
 */
/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap* tableHeap,RowId currentRowID) {
  tableHeap_ = tableHeap;
  currentRowID_ = currentRowID;
}

TableIterator::TableIterator(const TableIterator &other) {
  tableHeap_ = other.tableHeap_;
  currentRowID_ = other.currentRowID_;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return currentRowID_ == itr.currentRowID_ && itr.tableHeap_ == tableHeap_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(currentRowID_ == itr.currentRowID_ && itr.tableHeap_ == tableHeap_);
}

const Row &TableIterator::operator*() {
  ASSERT(tableHeap_, "TableHeap is nullptr.");
  Row* row = new Row(currentRowID_);
  tableHeap_->GetTuple(row, nullptr);
  ASSERT(row, "Invalid row.");
  return *row;
}

Row *TableIterator::operator->() {
  ASSERT(tableHeap_, "TableHeap is nullptr.");
  Row* row = new Row(currentRowID_);
  tableHeap_->GetTuple(row, nullptr);
  ASSERT(row, "Invalid row.");
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  tableHeap_ = itr.tableHeap_;
  currentRowID_ = itr.currentRowID_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
//  ASSERT(tableHeap_, "TableHeap is nullptr.");
//  Row cur_row;
//  cur_row.SetRowId(currentRowID_);
//  tableHeap_->GetTuple(&cur_row, nullptr);
//  currentRowID_ = tableHeap_->GetNextRowId(&cur_row, nullptr);
  return *this;
}

// iter++
TableIterator &TableIterator::operator++(int) {
//  ASSERT(tableHeap_, "TableHeap is nullptr.");
//  Row cur_row;
//  cur_row.SetRowId(currentRowID_);
//  tableHeap_->GetTuple(&cur_row, nullptr);
//  currentRowID_ = tableHeap_->GetNextRowId(&cur_row, nullptr);
  return *this;
}
