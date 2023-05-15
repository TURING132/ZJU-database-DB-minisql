//
// Created by njz on 2023/1/27.
//

#ifndef MINISQL_INSERT_PLAN_H
#define MINISQL_INSERT_PLAN_H

#include "abstract_plan.h"
#include "catalog/catalog.h"
#include "planner/expressions/abstract_expression.h"

/**
 * The InsertPlanNode identifies a table into which rows are inserted.
 * The values to be inserted will come from the child of the node.
 */
class InsertPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new insert plan node for inserting values from a child plan.
   * @param child the child plan to obtain values from
   * @param table_name the identifier of the table that should be inserted into
   */
  InsertPlanNode(Schema *output, AbstractPlanNodeRef child, std::string table_name)
      : AbstractPlanNode(output, {std::move(child)}), table_name_(std::move(table_name)) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::Insert; }

  /** @return The identifier of the table which rows are inserted intol*/
  std::string GetTableName() const { return table_name_; }

  /** @return the child plan providing rows to be inserted */
  AbstractPlanNodeRef GetChildPlan() const {
    ASSERT(GetChildren().size() == 1, "Insert should have only one child plan.");
    return GetChildAt(0);
  }

  /** The table to be inserted into. */
  std::string table_name_;
};

#endif  // MINISQL_INSERT_PLAN_H
