//
// Created by njz on 2023/2/3.
//

#ifndef MINISQL_UPDATE_STATEMENT_H
#define MINISQL_UPDATE_STATEMENT_H

#include <unordered_map>

#include "abstract_statement.h"

class UpdateStatement : public AbstractStatement {
 public:
  explicit UpdateStatement(pSyntaxNode ast, ExecuteContext *context) : AbstractStatement(ast, context){};

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
      case kNodeUpdateValues: {
        auto child = ast->child_;
        while (child) {
          MakeUpdateValues(child);
          child = child->next_;
        }
        break;
      }
      case kNodeConditions: {
        where_ = MakePredicate(ast->child_, table_name_);
        break;
      }
      default:
        throw std::logic_error("the ast_type is not supported in planner yet");
    }
    SyntaxTree2Statement(ast->next_);
  };

  void MakeUpdateValues(pSyntaxNode ast) {
    pSyntaxNode col = ast->child_;
    pSyntaxNode value = ast->child_->next_;
    TableInfo *info = nullptr;
    context_->GetCatalog()->GetTable(table_name_, info);
    auto schema = info->GetSchema();
    uint32_t index;
    if (schema->GetColumnIndex(col->val_, index) != DB_SUCCESS) {
      throw std::logic_error("the column does not exist in table");
    }
    auto col_type = schema->GetColumn(index)->GetType();
    auto const_expr = MakeConstantValueExpression(col_type, value);
    update_attrs[index] = const_expr;
  }

  std::string table_name_;

  AbstractExpressionRef where_;

  std::unordered_map<uint32_t, AbstractExpressionRef> update_attrs;

  std::string ToString() const override {
    std::stringstream sstream;
    sstream << "Update {{\\n  table={" << table_name_ << "}\\n }}";
    return sstream.str();
  };
};

#endif  // MINISQL_UPDATE_STATEMENT_H
