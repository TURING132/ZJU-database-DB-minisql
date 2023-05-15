//
// Created by njz on 2023/1/30.
//

#ifndef MINISQL_UPDATE_H
#define MINISQL_UPDATE_H

#include "abstract_plan.h"
#include "catalog/catalog.h"
#include "planner/expressions/abstract_expression.h"

/**
 * The UpdatePlanNode identifies a table that should be updated.
 * The row(s) to be updated come from the child of the UpdateExecutor.
 */
class UpdatePlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new UpdatePlanNode instance.
   * @param child the child plan to obtain row from
   * @param table_name the identifier of the table that should be updated
   * @param target_expressions the target expressions for new rows
   */
  UpdatePlanNode(const Schema *output, AbstractPlanNodeRef child, std::string table_name,
                 std::unordered_map<uint32_t, AbstractExpressionRef> update_attrs)
      : AbstractPlanNode(output, {std::move(child)}),
        table_name_(std::move(table_name)),
        update_attrs_(std::move(update_attrs)) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::Update; }

  /** @return The identifier of the table into which rows are inserted */
  std::string GetTableName() const { return table_name_; }

  /** @return The update attributes */
  const std::unordered_map<uint32_t, AbstractExpressionRef> &GetUpdateAttr() const { return update_attrs_; }

  /** @return The child plan providing rows to be inserted */
  AbstractPlanNodeRef GetChildPlan() const {
    ASSERT(GetChildren().size() == 1, "UPDATE should have exactly one child plan.");
    return GetChildAt(0);
  }

  /** The table to be updated. */
  std::string table_name_;

  /** Map from column index -> update operation */
  std::unordered_map<uint32_t, AbstractExpressionRef> update_attrs_;
};
#endif  // MINISQL_UPDATE_H
