//
// Created by njz on 2023/1/29.
//

#ifndef MINISQL_DELETE_EXECUTOR_H
#define MINISQL_DELETE_EXECUTOR_H

#include <vector>

#include "executor/execute_context.h"
#include "executor/executors/abstract_executor.h"
#include "executor/plans/delete_plan.h"

/**
 * DeletedExecutor executes a delete on a table.
 * Deleted values are always pulled from a child.
 */
class DeleteExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DeleteExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The delete plan to be executed
   * @param child_executor The child executor that feeds the delete
   */
  DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the delete */
  void Init() override;

  /**
   * Yield the number of rows deleted from the table.
   * @param[out] row The next row produced by the delete (ignore, not used)
   * @param[out] rid The next row RID produced by the delete (ignore, not used)
   * @return `true` if a row was produced, `false` if there are no more rows
   *
   * NOTE: DeleteExecutor::Next() does not use the `row` out-parameter.
   * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
   */
  bool Next(Row *row, RowId *rid) override;

  /** @return The output schema for the delete */
  const Schema *GetOutputSchema() const override { return plan_->OutputSchema(); }

 private:
  /** The delete plan node to be executed */
  const DeletePlanNode *plan_;
  /** The child executor from which RIDs for deleted rows are pulled */
  std::unique_ptr<AbstractExecutor> child_executor_;
};

#endif  // MINISQL_DELETE_EXECUTOR_H
