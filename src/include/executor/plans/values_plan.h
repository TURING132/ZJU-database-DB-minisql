//
// Created by njz on 2023/1/31.
//

#ifndef MINISQL_VALUES_PLAN_H
#define MINISQL_VALUES_PLAN_H

#include "abstract_plan.h"
#include "catalog/catalog.h"
#include "planner/expressions/abstract_expression.h"

/**
 * The ValuesPlanNode represents rows of values. For example,
 * `INSERT INTO table VALUES ((0, 1), (1, 2))`, where we will have
 * `(0, 1)` and `(1, 2)` as the output of this executor.
 */
class ValuesPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new ValuesPlanNode instance.
   * @param output The output schema of this values plan node
   * @param values The values produced by this plan node
   */
  explicit ValuesPlanNode(Schema *output, std::vector<std::vector<AbstractExpressionRef>> values)
      : AbstractPlanNode(std::move(output), {}), values_(std::move(values)) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::Values; }

  const std::vector<std::vector<AbstractExpressionRef>> &GetValues() const { return values_; }

  std::vector<std::vector<AbstractExpressionRef>> values_;
};

#endif  // MINISQL_VALUES_PLAN_H
