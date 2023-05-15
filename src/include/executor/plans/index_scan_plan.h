#pragma once

#include <string>
#include <utility>

#include "abstract_plan.h"
#include "catalog/catalog.h"
#include "planner/expressions/abstract_expression.h"

/**
 * IndexScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class IndexScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new index scan plan node.
   * @param output the output format of this scan plan node
   * @param table_name The identifier of table to be scanned
   */
  IndexScanPlanNode(const Schema *output, std::string table_name, std::vector<IndexInfo *> indexes, bool need_filter,
                    AbstractExpressionRef filter_predicate = nullptr)
      : AbstractPlanNode(output, {}),
        table_name_(std::move(table_name)),
        indexes_(std::move(indexes)),
        need_filter_(need_filter),
        filter_predicate_(std::move(filter_predicate)) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::IndexScan; }

  /** @return The identifier of the table that should be scanned */
  std::string GetTableName() const { return table_name_; }

  AbstractExpressionRef GetPredicate() const { return filter_predicate_; }

  /** The table name */
  std::string table_name_;

  /** The indexes*/
  std::vector<IndexInfo *> indexes_;

  /** Whether there are indexes on all columns in the predicate*/
  bool need_filter_ = true;

  /** The predicate to filter in IndexScan.*/
  AbstractExpressionRef filter_predicate_;
};