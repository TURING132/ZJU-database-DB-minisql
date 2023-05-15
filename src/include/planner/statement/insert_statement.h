//
// Created by njz on 2023/2/3.
//

#ifndef MINISQL_INSERT_STATEMENT_H
#define MINISQL_INSERT_STATEMENT_H

#include "abstract_statement.h"

class SelectStatement;
class InsertStatement : public AbstractStatement {
 public:
  explicit InsertStatement(pSyntaxNode ast, ExecuteContext *context) : AbstractStatement(ast, context) {}

  /** Transfer syntax tree to statement. */
  void SyntaxTree2Statement(pSyntaxNode ast) {
    if (!ast)
      return;
    switch (ast->type_) {
      case kNodeIdentifier: {
        TableInfo *info = nullptr;
        if (context_->GetCatalog()->GetTable(ast->val_, info) != DB_SUCCESS) {
          std::stringstream error_info;
          error_info << "the table " << ast->val_ << " is not exist.";
          throw std::logic_error(error_info.str());
        }
        table_name_ = ast->val_;
        break;
      }
      case kNodeColumnValues: {
        MakeInsertValues(ast->child_);
        break;
      }
      default:
        throw std::logic_error("the ast_type is not supported in planner yet");
    }
    SyntaxTree2Statement(ast->next_);
  };

  void MakeInsertValues(pSyntaxNode ast) {
    std::vector<AbstractExpressionRef> value;
    TableInfo *info = nullptr;
    context_->GetCatalog()->GetTable(table_name_, info);
    auto columns = info->GetSchema()->GetColumns();

    for (auto column : columns) {
      if (!ast)
        throw std::logic_error("The inserted value does not match the schema");
      if (ast_->type_ == kNodeNull) {
        auto f = new Field(column->GetType());
        value.emplace_back(std::make_shared<ConstantValueExpression>(*f));
        delete f;
      } else {
        value.emplace_back(MakeConstantValueExpression(column->GetType(), ast));
      }
      ast = ast->next_;
    }
    if (ast)
      throw std::logic_error("The inserted value does not match the schema");
    raw_values_.emplace_back(value);
  }

  /** Bound FROM clause. */
  std::string table_name_;

  /** Bound Select statement, used in "insert into t1 select..." */
  SelectStatement *select_ = nullptr;

  /** If raw insert, bound raw values. */
  std::vector<std::vector<AbstractExpressionRef>> raw_values_;

  std::string ToString() const override {
    std::stringstream sstream;
    sstream << "Insert {{\\n  table={" << table_name_ << "}\\n }}";
    return sstream.str();
  };
};

#endif  // MINISQL_INSERT_STATEMENT_H
