//
// Created by njz on 2023/1/18.
//

#ifndef MINISQL_COLUMN_VALUE_EXPRESSION_H
#define MINISQL_COLUMN_VALUE_EXPRESSION_H

#include <memory>
#include <vector>

#include "abstract_expression.h"
#include "record/schema.h"

/**
 * ColumnValueExpression maintains the row index and column index relative to a particular schema or join.
 */
class ColumnValueExpression : public AbstractExpression {
 public:
  /**
   * ColumnValueExpression is an abstraction around "Table.member" in terms of indexes.
   * @param row_idx {row index 0 = left side of join, row index 1 = right side of join}
   * @param col_idx the index of the column in the schema
   * @param ret_type the return type of the expression
   */
  ColumnValueExpression(uint32_t row_idx, uint32_t col_idx, TypeId ret_type)
      : AbstractExpression({}, ret_type, ExpressionType::ColumnExpression), row_idx_{row_idx}, col_idx_{col_idx} {}

  Field Evaluate(const Row *row) const override { return Field(*row->GetField(col_idx_)); }

  Field EvaluateJoin(const Row *left_row, const Row *right_row) const override {
    return row_idx_ == 0 ? Field(*left_row->GetField(col_idx_)) : Field(*right_row->GetField(col_idx_));
  }

  uint32_t GetRowIdx() const { return row_idx_; }
  uint32_t GetColIdx() const { return col_idx_; }

 private:
  /** Row index 0 = left side of join, row index 1 = right side of join */
  uint32_t row_idx_;
  /** Column index refers to the index within the schema of the row, e.g. schema {A,B,C} has indexes {0,1,2} */
  uint32_t col_idx_;
};

#endif  // MINISQL_COLUMN_VALUE_EXPRESSION_H
