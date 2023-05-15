//
// Created by njz on 2023/1/17.
//

#ifndef MINISQL_SEQ_SCAN_EXECUTOR_H
#define MINISQL_SEQ_SCAN_EXECUTOR_H

#include <vector>

#include "executor/execute_context.h"
#include "executor/executors/abstract_executor.h"
#include "executor/plans/seq_scan_plan.h"

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next row from the sequential scan.
   * @param[out] row The next row produced by the scan
   * @param[out] rid The next row RID produced by the scan
   * @return `true` if a row was produced, `false` if there are no more rows
   */
  bool Next(Row *row, RowId *rid) override;

  /** @return The output schema for the sequential scan */
  const Schema *GetOutputSchema() const override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
};

#endif  // MINISQL_SEQ_SCAN_EXECUTOR_H
