//
// Created by njz on 2023/1/30.
//

#ifndef MINISQL_UPDATE_EXECUTOR_H
#define MINISQL_UPDATE_EXECUTOR_H

#include "executor/execute_context.h"
#include "executor/executors/abstract_executor.h"
#include "executor/plans/update_plan.h"

/**
 * UpdateExecutor executes an update on a table.
 * Updated values are always pulled from a child.
 */
class UpdateExecutor : public AbstractExecutor {
  friend class UpdatePlanNode;

 public:
  /**
   * Construct a new UpdateExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The update plan to be executed
   * @param child_executor The child executor that feeds the update
   */
  UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the update */
  void Init() override;

  /**
   * Yield the next row from the udpate.
   * @param[out] row The next row produced by the update
   * @param[out] rid The next row RID produced by the update
   * @return `true` if a row was produced, `false` if there are no more rows
   *
   * NOTE: UpdateExecutor::Next() does not use the `row` out-parameter.
   * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
   */
  bool Next([[maybe_unused]] Row *row, RowId *rid) override;

  /** @return The output schema for the update */
  const Schema *GetOutputSchema() const override { return plan_->OutputSchema(); }

 private:
  /**
   * Given a row, creates a new, updated row
   * based on the `UpdateInfo` provided in the plan.
   * @param src_row The row to be updated
   */
  Row GenerateUpdatedTuple(const Row &src_row);

  /** The update plan node to be executed */
  const UpdatePlanNode *plan_;
  /** Metadata identifying the table that should be updated */
  std::vector<IndexInfo *> index_info_;
  /** The child executor to obtain value from */
  std::unique_ptr<AbstractExecutor> child_executor_;
};

#endif  // MINISQL_UPDATE_EXECUTOR_H
