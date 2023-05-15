//
// Created by njz on 2023/1/16.
//

#ifndef MINISQL_SEQ_SCAN_PLAN_H
#define MINISQL_SEQ_SCAN_PLAN_H

#include "abstract_plan.h"
#include "catalog/catalog.h"
#include "planner/expressions/abstract_expression.h"

class SeqScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new SeqScanPlanNode instance.
   * @param output The output schema of this sequential scan plan node
   * @param table_name The identifier of table to be scanned
   */
  SeqScanPlanNode(const Schema *output, std::string table_name, AbstractExpressionRef filter_predicate = nullptr)
      : AbstractPlanNode(output, {}),
        table_name_(std::move(table_name)),
        filter_predicate_(std::move(filter_predicate)) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::SeqScan; }

  /** @return The identifier of the table that should be scanned */
  std::string GetTableName() const { return table_name_; }

  AbstractExpressionRef GetPredicate() const { return filter_predicate_; }

  /** The table name */
  std::string table_name_;

  /** The predicate to filter in SeqScan.*/
  AbstractExpressionRef filter_predicate_;
};

#endif  // MINISQL_SEQ_SCAN_PLAN_H
