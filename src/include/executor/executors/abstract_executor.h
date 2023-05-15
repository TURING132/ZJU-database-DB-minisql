#ifndef MINISQL_ABSTRACT_EXECUTOR_H
#define MINISQL_ABSTRACT_EXECUTOR_H

#include "executor/execute_context.h"
/**
 * The AbstractExecutor implements the Volcano row-at-a-time iterator model.
 * This is the base class from which all executors in the execution engine
 * inherit, and defines the minimal interface that all executors support.
 */
class AbstractExecutor {
 public:
  /**
   * Construct a new AbstractExecutor instance.
   * @param exec_ctx the executor context that the executor runs with
   */
  explicit AbstractExecutor(ExecuteContext *exec_ctx) : exec_ctx_{exec_ctx} {}

  /** Virtual destructor. */
  virtual ~AbstractExecutor() = default;

  /**
   * Initialize the executor.
   * @warning This function must be called before Next() is called!
   */
  virtual void Init() = 0;

  /**
   * Yield the next row from this executor.
   * @param[out] row The next row produced by this executor
   * @param[out] rid The next row RID produced by this executor
   * @return `true` if a row was produced, `false` if there are no more rows
   */
  virtual bool Next(Row *row, RowId *rid) = 0;

  /** @return The schema of the rows that this executor produces */
  virtual const Schema *GetOutputSchema() const = 0;

  /** @return The executor context in which this executor runs */
  ExecuteContext *GetExecutorContext() { return exec_ctx_; }

 protected:
  /** The executor context in which the executor runs */
  ExecuteContext *exec_ctx_;
};

#endif  // MINISQL_ABSTRACT_EXECUTOR_H
