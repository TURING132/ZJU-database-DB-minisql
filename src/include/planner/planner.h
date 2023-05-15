//
// Created by njz on 2023/2/2.
//

#ifndef MINISQL_PLANNER_H
#define MINISQL_PLANNER_H

#include "common/instance.h"
#include "executor/plans/abstract_plan.h"
#include "executor/plans/delete_plan.h"
#include "executor/plans/index_scan_plan.h"
#include "executor/plans/insert_plan.h"
#include "executor/plans/seq_scan_plan.h"
#include "executor/plans/update_plan.h"
#include "executor/plans/values_plan.h"
#include "planner/statement/abstract_statement.h"
#include "planner/statement/delete_statement.h"
#include "planner/statement/insert_statement.h"
#include "planner/statement/select_statement.h"
#include "planner/statement/update_statement.h"

/**
 * The planner takes a bound statement, and transforms it into the BusTub plan tree.
 * The plan tree will be taken by the execution engine to execute the statement.
 */
class Planner {
 public:
  explicit Planner(ExecuteContext *context) : context_(context) {}

  // The following parts are undocumented. One `PlanXXX` functions simply corresponds to a
  // bound thing in the binder.

  void PlanQuery(pSyntaxNode ast);

  AbstractPlanNodeRef PlanSelect(std::shared_ptr<SelectStatement> statement);

  AbstractPlanNodeRef PlanInsert(std::shared_ptr<InsertStatement> statement);

  AbstractPlanNodeRef PlanDelete(std::shared_ptr<DeleteStatement> statement);

  AbstractPlanNodeRef PlanUpdate(std::shared_ptr<UpdateStatement> statement);

  /** the root plan node of the plan tree */
  AbstractPlanNodeRef plan_;

  Schema *MakeOutputSchema(const std::vector<std::pair<std::string, AbstractExpressionRef>> &exprs);

  /** Catalog will be used during the planning process. SHOULD ONLY BE USED IN
   * CODE PATH OF `PlanQuery`.
   */
  ExecuteContext *context_;

  /** The maximum size allowed for VARCHAR columns */
  static constexpr const uint32_t MAX_VARCHAR_SIZE = 128;
};

#endif  // MINISQL_PLANNER_H
