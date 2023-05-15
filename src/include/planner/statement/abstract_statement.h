//
// Created by njz on 2023/2/3.
//

#ifndef MINISQL_ABSTRACT_STATEMENT_H
#define MINISQL_ABSTRACT_STATEMENT_H

#include <string>

#include "planner/expressions/abstract_expression.h"
#include "planner/expressions/column_value_expression.h"
#include "planner/expressions/comparison_expression.h"
#include "planner/expressions/constant_value_expression.h"
#include "planner/expressions/logic_expression.h"

extern "C" {
#include "parser/parser.h"
};

/**
 * AbstractStatement is the base class of any type of bound SQL statement.
 */
class AbstractStatement {
 public:
  explicit AbstractStatement(pSyntaxNode ast, ExecuteContext *context) : ast_(ast), context_(context){};
  virtual ~AbstractStatement() = default;

  pSyntaxNode ast_;

  ExecuteContext *context_;

 public:
  /** Render this statement as a string. */
  virtual std::string ToString() const {
    throw std::logic_error("ToString not supported for this type of SQLStatement");
  }
  /**
   * Make a column value expression.
   * @param table_name The name of the table
   * @param col The ptr to the SyntaxNode of the column
   * @return A owning pointer to the ColumnValueExpression
   */
  AbstractExpressionRef MakeColumnValueExpression(const std::string &table_name, pSyntaxNode col) {
    TableInfo *info = nullptr;
    context_->GetCatalog()->GetTable(table_name, info);
    auto schema = info->GetSchema();
    uint32_t index;
    if (schema->GetColumnIndex(col->val_, index) != DB_SUCCESS) {
      throw std::logic_error("the column does not exist in table");
    }
    auto col_type = schema->GetColumn(index)->GetType();
    return std::make_shared<ColumnValueExpression>(0, index, col_type);
  }

  /**
   * Allocate a constant value expression and return it to the caller.
   * @param col_type The type of the constant value
   * @param value The ptr to the SyntaxNode of the constant value
   * @return An owning pointer to the ConstantValueExpression
   */
  AbstractExpressionRef MakeConstantValueExpression(TypeId col_type, pSyntaxNode value) {
    Field *f = nullptr;
    if (value->type_ == kNodeNull) {
      f = new Field(col_type);
    } else {
      switch (col_type) {
        case kTypeInt: {
          if (value->type_ != kNodeNumber)
            throw std::logic_error("The value of the predicate does not match the type of column");
          f = new Field(kTypeInt, stoi(value->val_));
          break;
        }
        case kTypeFloat: {
          if (value->type_ != kNodeNumber)
            throw std::logic_error("The value of the predicate does not match the type of column");
          f = new Field(kTypeFloat, stof(value->val_));
          break;
        }
        case kTypeChar: {
          if (value->type_ != kNodeString)
            throw std::logic_error("The value of the predicate does not match the type of column");
          f = new Field(kTypeChar, value->val_, strlen(value->val_), true);
          break;
        }
        default:
          throw std::logic_error("The type of the column is kTypeInvalid");
      }
    }
    auto const_expr = std::make_shared<ConstantValueExpression>(*f);
    delete f;
    return const_expr;
  }

  /**
   * Allocate a comparison value expression or a logic value expression and return it to the caller.
   * @param table_name The name of the table
   * @param ast The ptr to the child node of kNodeConditions
   * @return An owning pointer to the ConstantValueExpression
   */
  AbstractExpressionRef MakePredicate(pSyntaxNode ast, std::string table_name,
                                      vector<uint32_t> *column_in_condition = nullptr, bool *has_or = nullptr) {
    switch (ast->type_) {
      case kNodeConnector: {
        auto left = MakePredicate(ast->child_, table_name);
        auto right = MakePredicate(ast->child_->next_, table_name);
        if (has_or && !strcmp(ast->val_, "or")) {
          *has_or = true;
        }
        return MakeLogicExpression(left, right, LogicExpression::Char2Type(ast->val_));
      }
      case kNodeCompareOperator: {
        pSyntaxNode col = ast->child_;
        pSyntaxNode value = ast->child_->next_;
        auto col_expr = MakeColumnValueExpression(table_name, col);
        auto const_expr = MakeConstantValueExpression(col_expr->GetReturnType(), value);
        if (column_in_condition) {
          uint32_t index = dynamic_pointer_cast<ColumnValueExpression>(col_expr)->GetColIdx();
          column_in_condition->emplace_back(index);
        }
        return MakeComparisonExpression(col_expr, const_expr, ast->val_);
      }
      default:
        throw std::logic_error("The node kNodeConditions has a child node of the wrong type");
    }
  }

  /**
   * Allocate a comparison expression and return it to the caller.
   * @param lhs The abstract expression for the left-hand side of the comparison
   * @param rhs The abstract expression for the right-hand side of the comparison
   * @param comp_type The type of the comparison operation
   * @return An owning pointer to the ComparisonExpression
   */
  AbstractExpressionRef MakeComparisonExpression(const AbstractExpressionRef &lhs, const AbstractExpressionRef &rhs,
                                                 string comp_type) {
    return std::make_shared<ComparisonExpression>(lhs, rhs, comp_type);
  }

  /**
   * Allocate a logic expression and return it to the caller.
   * @param lhs The abstract expression for the left-hand side of the logic computation
   * @param rhs The abstract expression for the right-hand side of the logic computation
   * @param logic_type The type of the logic computation operation
   * @return An owning pointer to the LogicExpression
   */
  AbstractExpressionRef MakeLogicExpression(const AbstractExpressionRef &lhs, const AbstractExpressionRef &rhs,
                                            LogicType logic_type) {
    return std::make_shared<LogicExpression>(lhs, rhs, logic_type);
  }
};

#endif  // MINISQL_ABSTRACT_STATEMENT_H
