//
// Created by njz on 2023/1/17.
//

#ifndef MINISQL_ABSTRACT_EXPRESSION_H
#define MINISQL_ABSTRACT_EXPRESSION_H

#include <utility>
#include <vector>

#include "record/row.h"
#include "record/schema.h"

class AbstractExpression;
using AbstractExpressionRef = std::shared_ptr<AbstractExpression>;

enum class ExpressionType { LogicExpression = 0, ComparisonExpression, ColumnExpression, ConstantExpression };

/**
 * AbstractExpression is the base class of all the expressions in the system.
 * Expressions are modeled as trees, i.e. every expression may have a variable number of children.
 */
class AbstractExpression {
 public:
  /**
   * Create a new AbstractExpression with the given children and return type.
   * @param children the children of this abstract expression
   * @param ret_type the return type of this abstract expression when it is evaluated
   */
  AbstractExpression(std::vector<AbstractExpressionRef> children, TypeId ret_type, ExpressionType type)
      : ret_type_(ret_type), type_(type), children_{std::move(children)} {}

  /** Virtual destructor. */
  virtual ~AbstractExpression() = default;

  /** @return The field obtained by evaluating the row */
  virtual Field Evaluate(const Row *row) const = 0;

  /**
   * Returns the field obtained by evaluating a JOIN.
   * @param left_row The left row
   * @param right_row The right row
   * @return The field obtained by evaluating a JOIN on the left and right
   */
  virtual Field EvaluateJoin(const Row *left_row, const Row *right_row) const = 0;

  /** @return the child_idx'th child of this expression */
  const AbstractExpressionRef &GetChildAt(uint32_t child_idx) const { return children_[child_idx]; }

  /** @return the children of this expression, ordering may matter */
  const std::vector<AbstractExpressionRef> &GetChildren() const { return children_; }

  /** @return the type of this expression if it were to be evaluated */
  virtual TypeId GetReturnType() { return ret_type_; }

  /** @return the type of this expression */
  virtual ExpressionType GetType() { return type_; }

 private:
  /** The return type of this expression. */
  TypeId ret_type_;

  /** The type of this expression. */
  ExpressionType type_;

  /** The children of this expression. Note that the order of appearance of children may matter. */
  std::vector<AbstractExpressionRef> children_;
};

#endif  // MINISQL_ABSTRACT_EXPRESSION_H
