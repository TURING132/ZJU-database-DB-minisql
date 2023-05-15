//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  return Row();
}