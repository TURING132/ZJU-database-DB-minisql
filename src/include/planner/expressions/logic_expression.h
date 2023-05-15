//
// Created by njz on 2023/2/8.
//

#ifndef MINISQL_LOGIC_EXPRESSION_H
#define MINISQL_LOGIC_EXPRESSION_H

#include "abstract_expression.h"

/** ArithmeticType represents the type of logic operation that we want to perform. */
enum class LogicType { And, Or };

/**
 * LogicExpression represents two expressions being computed.
 */
class LogicExpression : public AbstractExpression {
 public:
  /** Creates a new comparison expression representing (left comp_type right). */
  LogicExpression(AbstractExpressionRef left, AbstractExpressionRef right, LogicType logic_type)
      : AbstractExpression({std::move(left), std::move(right)}, TypeId::kTypeInt, ExpressionType::LogicExpression),
        logic_type_{logic_type} {
    if (GetChildAt(0)->GetReturnType() != TypeId::kTypeInt || GetChildAt(1)->GetReturnType() != TypeId::kTypeInt) {
      throw std::logic_error("expect boolean from either side");
    }
  }

  /** e.g. evaluate the result of id = 1 and name = "str"*/
  Field Evaluate(const Row *row) const override {
    Field lhs = GetChildAt(0)->Evaluate(row);
    Field rhs = GetChildAt(1)->Evaluate(row);
    return Field(kTypeInt, PerformComputation(lhs, rhs));
  }

  Field EvaluateJoin(const Row *left_row, const Row *right_row) const override {
    Field lhs = GetChildAt(0)->EvaluateJoin(left_row, right_row);
    Field rhs = GetChildAt(1)->EvaluateJoin(left_row, right_row);
    return Field(kTypeInt, PerformComputation(lhs, rhs));
  }

  static LogicType Char2Type(char *val) {
    if (!strcmp(val, "and"))
      return LogicType::And;
    else if (!strcmp(val, "or"))
      return LogicType::Or;
    else
      throw std::logic_error("Unsupported logic type.");
  }

  LogicType logic_type_;

 private:
  CmpBool GetFieldAsCmpBool(const Field &val) const {
    if (val.IsNull()) {
      return CmpBool::kNull;
    }
    if (val.CompareEquals(Field(kTypeInt, 1))) {
      return CmpBool::kTrue;
    }
    return CmpBool::kFalse;
  }

  CmpBool PerformComputation(const Field &lhs, const Field &rhs) const {
    auto l = GetFieldAsCmpBool(lhs);
    auto r = GetFieldAsCmpBool(rhs);
    switch (logic_type_) {
      case LogicType::And:
        if (l == CmpBool::kFalse || r == CmpBool::kFalse) {
          return CmpBool::kFalse;
        }
        if (l == CmpBool::kTrue && r == CmpBool::kTrue) {
          return CmpBool::kTrue;
        }
        return CmpBool::kNull;
      case LogicType::Or:
        if (l == CmpBool::kFalse && r == CmpBool::kFalse) {
          return CmpBool::kFalse;
        }
        if (l == CmpBool::kTrue || r == CmpBool::kTrue) {
          return CmpBool::kTrue;
        }
        return CmpBool::kNull;
      default:
        throw std::logic_error("Unsupported logic type.");
    }
  }
};

#endif  // MINISQL_LOGIC_EXPRESSION_H
