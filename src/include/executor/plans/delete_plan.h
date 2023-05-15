//
// Created by njz on 2023/1/29.
//

#ifndef MINISQL_DELETE_PLAN_H
#define MINISQL_DELETE_PLAN_H

#include "abstract_plan.h"
#include "catalog/catalog.h"
#include "planner/expressions/abstract_expression.h"

/**
 * The DeletePlanNode identifies a table from which rows should be deleted.
 *
 * NOTE: To simplify the assignment, DeletePlanNode has at most one child.
 */
class DeletePlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new DeletePlanNode.
   * @param child The child plan to obtain row from
   * @param table_name The identifier of the table from which rows are deleted
   */
  DeletePlanNode(const Schema *output, AbstractPlanNodeRef child, std::string table_name)
      : AbstractPlanNode(output, {std::move(child)}), table_name_(table_name) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::Delete; }

  /** @return The identifier of the table from which rows are deleted*/
  std::string GetTableName() const { return table_name_; }

  /** @return The child plan providing rows to be deleted */
  AbstractPlanNodeRef GetChildPlan() const {
    ASSERT(GetChildren().size() == 1, "Delete should have only one child plan.");
    return GetChildAt(0);
  }

  /** The identifier of the table from which rows are deleted */
  std::string table_name_;
};

#endif  // MINISQL_DELETE_PLAN_H
