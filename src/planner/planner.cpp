//
// Created by njz on 2023/2/2.
//
#include <algorithm>
#include "planner/planner.h"

void Planner::PlanQuery(pSyntaxNode ast) {
  switch (ast->type_) {
    case kNodeSelect: {
      auto statement = make_shared<SelectStatement>(ast, context_);
      statement->SyntaxTree2Statement(ast->child_);
      plan_ = PlanSelect(statement);
      return;
    }
    case kNodeInsert: {
      auto statement = make_shared<InsertStatement>(ast, context_);
      statement->SyntaxTree2Statement(ast->child_);
      plan_ = PlanInsert(statement);
      return;
    }
    case kNodeDelete: {
      auto statement = make_shared<DeleteStatement>(ast, context_);
      statement->SyntaxTree2Statement(ast->child_);
      plan_ = PlanDelete(statement);
      return;
    }
    case kNodeUpdate: {
      auto statement = make_shared<UpdateStatement>(ast, context_);
      statement->SyntaxTree2Statement(ast->child_);
      plan_ = PlanUpdate(statement);
      return;
    }
    default:
      throw std::logic_error("the statement is not supported in planner yet");
  }
}
AbstractPlanNodeRef Planner::PlanSelect(std::shared_ptr<SelectStatement> statement) {
  auto out_schema = MakeOutputSchema(statement->column_list_);
  vector<IndexInfo *> indexes;
  vector<IndexInfo *> available_index;
  context_->GetCatalog()->GetTableIndexes(statement->table_name_, indexes);
  for (auto index : indexes) {
    if (index->GetIndexKeySchema()->GetColumns().size() == 1) {
      auto col_id = index->GetIndexKeySchema()->GetColumn(0)->GetTableInd();
      if (std::find(statement->column_in_condition_.begin(), statement->column_in_condition_.end(), col_id) !=
          statement->column_in_condition_.end()) {
        available_index.push_back(index);
      }
    }
  }
  if (available_index.empty() || statement->has_or) {
    return make_shared<SeqScanPlanNode>(out_schema, statement->table_name_, statement->where_);
  }
  return make_shared<IndexScanPlanNode>(out_schema, statement->table_name_, available_index,
                                        available_index.size() != statement->column_in_condition_.size(),
                                        statement->where_);
}

AbstractPlanNodeRef Planner::PlanInsert(std::shared_ptr<InsertStatement> statement) {
  auto value_plan = std::make_shared<ValuesPlanNode>(nullptr, statement->raw_values_);
  return std::make_shared<InsertPlanNode>(nullptr, value_plan, statement->table_name_);
}

AbstractPlanNodeRef Planner::PlanDelete(std::shared_ptr<DeleteStatement> statement) {
  TableInfo *info = nullptr;
  context_->GetCatalog()->GetTable(statement->table_name_, info);
  auto scan_plan = make_shared<SeqScanPlanNode>(info->GetSchema(), statement->table_name_, statement->where_);
  return std::make_shared<DeletePlanNode>(info->GetSchema(), scan_plan, statement->table_name_);
}

AbstractPlanNodeRef Planner::PlanUpdate(std::shared_ptr<UpdateStatement> statement) {
  TableInfo *info = nullptr;
  context_->GetCatalog()->GetTable(statement->table_name_, info);
  auto scan_plan = make_shared<SeqScanPlanNode>(info->GetSchema(), statement->table_name_, statement->where_);
  return std::make_shared<UpdatePlanNode>(info->GetSchema(), scan_plan, statement->table_name_,
                                          statement->update_attrs);
}

Schema *Planner::MakeOutputSchema(const vector<std::pair<std::string, AbstractExpressionRef>> &exprs) {
  std::vector<Column *> cols;
  cols.reserve(exprs.size());
  for (const auto &input : exprs) {
    uint32_t col_idx = dynamic_pointer_cast<ColumnValueExpression>(input.second)->GetColIdx();
    if (input.second->GetReturnType() != TypeId::kTypeChar) {
      cols.emplace_back(new Column(input.first, input.second->GetReturnType(), col_idx, false, false));
    } else {
      cols.emplace_back(
          new Column(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, col_idx, false, false));
    }
  }
  return new Schema(cols);
}
