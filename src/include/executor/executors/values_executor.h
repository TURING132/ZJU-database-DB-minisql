//
// Created by njz on 2023/1/31.
//

#ifndef MINISQL_VALUES_EXECUTOR_H
#define MINISQL_VALUES_EXECUTOR_H

#include "executor/execute_context.h"
#include "executor/executors/abstract_executor.h"
#include "executor/plans/values_plan.h"

/**
 * The ValuesExecutor executor produces rows of values.
 */
class ValuesExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new ValuesExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The values plan to be executed
   */
  ValuesExecutor(ExecuteContext *exec_ctx, const ValuesPlanNode *plan);

  /** Initialize the values */
  void Init() override;

  /**
   * Yield the next row from the values.
   * @param[out] row The next row produced by the values
   * @param[out] rid The next row RID produced by the values, not used by values executor
   * @return `true` if a row was produced, `false` if there are no more rows
   */
  bool Next(Row *row, RowId *rid) override;

  /** @return The output schema for the values */
  const Schema *GetOutputSchema() const override { return plan_->OutputSchema(); }

 private:
  /** The values plan node to be executed */
  const ValuesPlanNode *plan_;
  size_t value_size_{0};
  size_t cursor_{0};
};

#endif  // MINISQL_VALUES_EXECUTOR_H
