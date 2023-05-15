//
// Created by njz on 2023/1/31.
//

#include "executor/executors/values_executor.h"

ValuesExecutor::ValuesExecutor(ExecuteContext *exec_ctx, const ValuesPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void ValuesExecutor::Init() {
  value_size_ = plan_->GetValues().size();
}

bool ValuesExecutor::Next(Row *row, RowId *rid) {
  if (cursor_ < value_size_) {
    std::vector<Field> values;
    auto exprs = plan_->GetValues().at(cursor_);
    for (auto expr : exprs) {
      values.emplace_back(expr->Evaluate(nullptr));
    }
    *row = Row{values};
    cursor_++;
    return true;
  }
  return false;
}