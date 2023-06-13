#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
 explicit TableIterator(){}

  explicit TableIterator(TableHeap* tableHeap,RowId currentRowID);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator &operator++(int);

private:
  // add your own private member variables here
 TableHeap* tableHeap_; // 指向TableHeap对象的指针
 RowId currentRowID_;   // 当前行的RowID
};

#endif  // MINISQL_TABLE_ITERATOR_H
