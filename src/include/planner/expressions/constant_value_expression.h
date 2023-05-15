//
// Created by njz on 2023/1/18.
//

#ifndef MINISQL_CONSTANT_VALUE_EXPRESSION_H
#define MINISQL_CONSTANT_VALUE_EXPRESSION_H

#include "abstract_expression.h"

/**
 * ConstantValueExpression represents constants.
 */
class ConstantValueExpression : public AbstractExpression {
 public:
  /** Creates a new constant value expression wrapping the given value. */
  explicit ConstantValueExpression(const Field &val)
      : AbstractExpression({}, val.GetTypeId(), ExpressionType::ConstantExpression), val_(val) {}

  Field Evaluate(const Row *row) const override { return Field(val_); }

  Field EvaluateJoin(const Row *left_row, const Row *right_row) const override { return Field(val_); }

  const Field val_;
};

#endif  // MINISQL_CONSTANT_VALUE_EXPRESSION_H
