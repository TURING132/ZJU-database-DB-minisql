//
// Created by njz on 2023/2/3.
//

#ifndef MINISQL_SELECT_STATEMENT_H
#define MINISQL_SELECT_STATEMENT_H

#include "abstract_statement.h"

class SelectStatement : public AbstractStatement {
 public:
  explicit SelectStatement(pSyntaxNode ast, ExecuteContext *context) : AbstractStatement(ast, context) {}

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
      case kNodeAllColumns:
      case kNodeColumnList: {
        SyntaxTree2Statement(ast->next_);
        MakeColumnList(ast->child_);
        return;
      }
      case kNodeConditions: {
        where_ = MakePredicate(ast->child_, table_name_, &column_in_condition_);
        break;
      }
      default:
        throw std::logic_error("the ast_type is not supported in planner yet");
    }
    SyntaxTree2Statement(ast->next_);
  };

  void MakeColumnList(pSyntaxNode ast) {
    TableInfo *info = nullptr;
    context_->GetCatalog()->GetTable(table_name_, info);
    auto schema = info->GetSchema();
    if (!ast) {
      auto columns = schema->GetColumns();
      for (auto column : columns) {
        auto expr = std::make_shared<ColumnValueExpression>(0, column->GetTableInd(), column->GetType());
        column_list_.emplace_back(make_pair(column->GetName(), expr));
      }
    } else {
      while (ast) {
        uint32_t index;
        if (schema->GetColumnIndex(ast->val_, index) != DB_SUCCESS) {
          throw std::logic_error("the column does not exist in table");
        }
        auto expr = std::make_shared<ColumnValueExpression>(0, index, schema->GetColumn(index)->GetType());
        column_list_.emplace_back(make_pair(ast->val_, expr));
        ast = ast->next_;
      }
    }
  }

  /** Bound FROM clause. */
  std::string table_name_;

  /** Bound SELECT list. */
  std::vector<std::pair<std::string, AbstractExpressionRef>> column_list_;

  /** Index of columns in condition. */
  std::vector<uint32_t> column_in_condition_;

  /** Has or in where clause */
  bool has_or = false;

  /** Bound WHERE clause. */
  AbstractExpressionRef where_ = nullptr;

  std::string ToString() const override {
    std::stringstream sstream;
    sstream << "Select {{\\n  table={" << table_name_ << "},\\n  columns={";
    for (const auto &column_pair : column_list_) {
      sstream << column_pair.first << " ";
    }
    sstream << "},\\n where={" << where_->GetReturnType() << "}\\n}";
    return sstream.str();
  };
};

#endif  // MINISQL_SELECT_STATEMENT_H
