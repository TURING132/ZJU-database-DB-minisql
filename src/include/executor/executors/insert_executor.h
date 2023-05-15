//
// Created by njz on 2023/1/27.
//

#ifndef MINISQL_INSERT_EXECUTOR_H
#define MINISQL_INSERT_EXECUTOR_H

#include "executor/execute_context.h"
#include "executor/executors/abstract_executor.h"
#include "executor/plans/insert_plan.h"

/**
 * InsertExecutor executes an insert on a table.
 *
 * Inserted values are always pulled from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted rows are pulled
   */
  InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */
  void Init() override;

  /**
   * Yield the next row from the insert.
   * @param[out] row The next row produced by the insert
   * @param[out] rid The next row RID produced by the insert(ignore, not used)
   * @return `true` if a row was produced, `false` if there are no more rows
   *
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   */
  bool Next([[maybe_unused]] Row *row, RowId *rid) override;

  /** @return The output schema for the insert */
  const Schema *GetOutputSchema() const override { return plan_->OutputSchema(); }

 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
};

#endif  // MINISQL_INSERT_EXECUTOR_H
