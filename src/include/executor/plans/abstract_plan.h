//
// Created by njz on 2023/1/16.
//

#ifndef MINISQL_ABSTRACT_PLAN_H
#define MINISQL_ABSTRACT_PLAN_H

#include <record/schema.h>

#include <utility>
#include <vector>
#include <memory>

/** PlanType represents the types of plans that we have in our system. */
enum class PlanType {
  SeqScan,
  IndexScan,
  Insert,
  Update,
  Delete,
  Values,
  Aggregation,
  Limit,
  Distinct,
  NestedLoopJoin,
};

class AbstractPlanNode;
using AbstractPlanNodeRef = std::shared_ptr<const AbstractPlanNode>;

/**
 * AbstractPlanNode represents all the possible types of plan nodes in our system.
 * Plan nodes are modeled as trees, so each plan node can have a variable number of children.
 * Per the Volcano model, the plan node receives the rows of its children.
 * The ordering of the children may matter.
 */
class AbstractPlanNode {
 public:
  /**
   * Create a new AbstractPlanNode with the specified output schema and children.
   * @param output_schema the schema for the output of this plan node
   * @param children the children of this plan node
   */
  AbstractPlanNode(const Schema *output_schema, std::vector<AbstractPlanNodeRef> children)
      : output_schema_(output_schema), children_(std::move(children)) {}

  /** Virtual destructor. */
  virtual ~AbstractPlanNode() = default;

  /** @return the schema for the output of this plan node */
  const Schema *OutputSchema() const { return output_schema_; }

  /** @return the child of this plan node at index child_idx */
  AbstractPlanNodeRef GetChildAt(uint32_t child_idx) const { return children_[child_idx]; }

  /** @return the children of this plan node */
  const std::vector<AbstractPlanNodeRef> &GetChildren() const { return children_; }

  /** @return the type of this plan node */
  virtual PlanType GetType() const = 0;

 private:
  /**
   * The schema for the output of this plan node. In the volcano model, every plan node will spit out rows,
   * and this tells you what schema this plan node's rows will have.
   */
  const Schema *output_schema_;

  /** The children of this plan node. */
  std::vector<AbstractPlanNodeRef> children_;
};

#endif  // MINISQL_ABSTRACT_PLAN_H
