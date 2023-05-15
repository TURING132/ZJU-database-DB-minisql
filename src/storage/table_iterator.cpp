#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator() {

}

TableIterator::TableIterator(const TableIterator &other) {

}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return false;
}

const Row &TableIterator::operator*() {
  ASSERT(false, "Not implemented yet.");
}

Row *TableIterator::operator->() {
  return nullptr;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  ASSERT(false, "Not implemented yet.");
}

// ++iter
TableIterator &TableIterator::operator++() {
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  return TableIterator();
}
