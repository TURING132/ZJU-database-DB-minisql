//
// Created by njz on 2023/1/18.
//

#ifndef MINISQL_COMPARISON_EXPRESSION_H
#define MINISQL_COMPARISON_EXPRESSION_H

#include <utility>

#include "abstract_expression.h"
#include "record/schema.h"

/**
 * ComparisonExpression represents two expressions being compared.
 */
class ComparisonExpression : public AbstractExpression {
 public:
  /** Creates a new comparison expression representing (left comp_type right). */
  ComparisonExpression(AbstractExpressionRef left, AbstractExpressionRef right, string comp_type)
      : AbstractExpression({std::move(left), std::move(right)}, TypeId::kTypeInt, ExpressionType::ComparisonExpression),
        comp_type_{std::move(comp_type)} {}

  /** e.g. evaluate the result of id = 1 */
  Field Evaluate(const Row *row) const override {
    Field lhs = GetChildAt(0)->Evaluate(row);
    Field rhs = GetChildAt(1)->Evaluate(row);
    return Field(kTypeInt, PerformComparison(lhs, rhs));
  }

  Field EvaluateJoin(const Row *left_row, const Row *right_row) const override {
    Field lhs = GetChildAt(0)->EvaluateJoin(left_row, right_row);
    Field rhs = GetChildAt(1)->EvaluateJoin(left_row, right_row);
    return Field(kTypeInt, PerformComparison(lhs, rhs));
  }

  std::string GetComparisonType() { return comp_type_; }

 private:
  CmpBool PerformComparison(const Field &lhs, const Field &rhs) const {
    if (comp_type_ == "=")
      return lhs.CompareEquals(rhs);
    else if (comp_type_ == "<>")
      return lhs.CompareNotEquals(rhs);
    else if (comp_type_ == "<")
      return lhs.CompareLessThan(rhs);
    else if (comp_type_ == "<=")
      return lhs.CompareLessThanEquals(rhs);
    else if (comp_type_ == ">")
      return lhs.CompareGreaterThan(rhs);
    else if (comp_type_ == ">=")
      return lhs.CompareGreaterThanEquals(rhs);
    else if (comp_type_ == "is")
      return GetCmpBool(lhs.IsNull());
    else if (comp_type_ == "not")
      return GetCmpBool(!lhs.IsNull());
    else
      throw std::logic_error("Unsupported comparison type");
  }

  std::string comp_type_;
};

#endif  // MINISQL_COMPARISON_EXPRESSION_H
