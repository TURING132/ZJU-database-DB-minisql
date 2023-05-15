//
// Created by njz on 2023/2/3.
//

#ifndef MINISQL_DELETE_STATEMENT_H
#define MINISQL_DELETE_STATEMENT_H

#include "abstract_statement.h"

class DeleteStatement : public AbstractStatement {
 public:
  explicit DeleteStatement(pSyntaxNode ast, ExecuteContext *context) : AbstractStatement(ast, context) {}

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
      case kNodeConditions: {
        where_ = MakePredicate(ast->child_, table_name_);
        break;
      }
      default:
        throw std::logic_error("the ast_type is not supported in planner yet");
    }
    SyntaxTree2Statement(ast->next_);
  };

  /** Bound FROM clause. */
  std::string table_name_;

  /** Bound WHERE clause. */
  AbstractExpressionRef where_ = nullptr;

  std::string ToString() const override {
    std::stringstream sstream;
    sstream << "Delete {{\\n  table={" << table_name_ << "}\\n }}";
    return sstream.str();
  };
};

#endif  // MINISQL_DELETE_STATEMENT_H
