//
// Created by njz on 2023/1/26.
//

#ifndef MINISQL_EXECUTOR_TEST_UTIL_H
#define MINISQL_EXECUTOR_TEST_UTIL_H

#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/table.h"
#include "common/instance.h"
#include "executor/execute_context.h"
#include "executor/execute_engine.h"
#include "executor/plans/seq_scan_plan.h"
#include "gtest/gtest.h"
#include "planner/expressions/column_value_expression.h"
#include "planner/expressions/comparison_expression.h"
#include "planner/expressions/constant_value_expression.h"
#include "utils/utils.h"

/**
 * The ExecutorTest class defines a test fixture for executor tests.
 * Any test that is defined as part of the `ExecutorTest` fixture
 * will have access to the helper functions defined below.
 */
using Fields = std::vector<Field>;

class ExecutorTest : public ::testing::Test {
 public:
  /** Called before every executor test. */
  void SetUp() override {
    ::testing::Test::SetUp();

    // Initialize the database subsystems
    db_test_ = new DBStorageEngine("executor_test.db", true);
    auto &catalog_01 = db_test_->catalog_mgr_;
    TableInfo *table_info = nullptr;
    std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                     new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                     new Column("account", TypeId::kTypeFloat, 2, true, false)};
    auto schema = std::make_shared<Schema>(columns);
    catalog_01->CreateTable("table-1", schema.get(), txn_, table_info);
    TableHeap *table_heap = table_info->GetTableHeap();
    for (int i = 0; i < 1000; i++) {
      int32_t len = RandomUtils::RandomInt(0, 64);
      char *characters = new char[len];
      RandomUtils::RandomString(characters, len);
      auto *fields =
          new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                     Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
      Row row(*fields);
      ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
      delete[] characters;
    }
    // Create an executor context for our executors
    exec_ctx_ = std::make_unique<ExecuteContext>(txn_, db_test_->catalog_mgr_, db_test_->bpm_);

    // Construct the executor engine for the test
    execution_engine_ = std::make_unique<ExecuteEngine>();
  }

  /** Called after every executor test. */
  void TearDown() override { delete db_test_; };

  /** @return The executor context for our test instance. */
  ExecuteContext *GetExecutorContext() { return exec_ctx_.get(); }

  /** @return The execution engine for our test instance. */
  ExecuteEngine *GetExecutionEngine() { return execution_engine_.get(); }

  /** @return Get the transaction for our test instance. */
  Transaction *GetTxn() { return txn_; }

  /**
   * Make a column value expression.
   * @param schema The schema for the expression
   * @param row_idx The row index in a JOIN operation (0 for non-JOINs)
   * @param col_name The name of the column in the schema that is referenced
   * @return A non-owning pointer to the ColumnValueExpression
   */
  AbstractExpressionRef MakeColumnValueExpression(const Schema &schema, uint32_t row_idx, const std::string &col_name) {
    uint32_t col_idx;
    schema.GetColumnIndex(col_name, col_idx);
    auto col_type = schema.GetColumn(col_idx)->GetType();
    allocated_exprs_.emplace_back(std::make_shared<ColumnValueExpression>(row_idx, col_idx, col_type));
    return allocated_exprs_.back();
  }

  /**
   * Allocate a column value expression and return it to the caller.
   * @param schema The schema for the expression
   * @param row_idx The row index in a JOIN operation (0 for non-JOINs)
   * @param col_name The name of the column in the schema that is referenced
   * @return An owning-pointer to the ColumnValueExpression
   */
  AbstractExpressionRef AllocateColumnValueExpression(const Schema &schema, uint32_t row_idx,
                                                      const std::string &col_name) {
    uint32_t col_idx;
    schema.GetColumnIndex(col_name, col_idx);
    auto col_type = schema.GetColumn(col_idx)->GetType();
    return std::make_shared<ColumnValueExpression>(row_idx, col_idx, col_type);
  }

  /**
   * Make a constant value expression.
   * @param val The constant value of the expression
   * @return A non-owning pointer to the ConstantValueExpression
   */
  AbstractExpressionRef MakeConstantValueExpression(const Field &val) {
    allocated_exprs_.emplace_back(std::make_shared<ConstantValueExpression>(val));
    return allocated_exprs_.back();
  }

  /**
   * Allocate a constant value expression and return it to the caller.
   * @param val The constant value of the expression
   * @return An owning pointer to the ConstantValueExpression
   */
  AbstractExpressionRef AllocateConstantValueExpression(const Field &val) {
    return std::make_shared<ConstantValueExpression>(val);
  }

  /**
   * Make a comparison expression.
   * @param lhs The abstract expression for the left-hand side of the comparison
   * @param rhs The abstract expression for the right-hand side of the comparison
   * @param comp_type The type of the comparison operation
   * @return A non-owning pointer to the ComparisonExpression
   */
  AbstractExpressionRef MakeComparisonExpression(AbstractExpressionRef lhs, AbstractExpressionRef rhs,
                                                 string comp_type) {
    allocated_exprs_.emplace_back(std::make_shared<ComparisonExpression>(lhs, rhs, comp_type));
    return allocated_exprs_.back();
  }

  /**
   * Allocate a comparison expression and return it to the caller.
   * @param lhs The abstract expression for the left-hand side of the comparison
   * @param rhs The abstract expression for the right-hand side of the comparison
   * @param comp_type The type of the comparison operation
   * @return An owning pointer to the ComparisonExpression
   */
  AbstractExpressionRef AllocateComparisonExpression(AbstractExpressionRef lhs, AbstractExpressionRef rhs,
                                                     string comp_type) {
    return std::make_shared<ComparisonExpression>(lhs, rhs, comp_type);
  }
  /**
   * Make an output schema.
   * @param exprs The expressions that define the columns of the output schema
   * @return A non-owning pointer to the Schema
   */
  Schema *MakeOutputSchema(const std::vector<std::pair<std::string, AbstractExpressionRef>> &exprs) {
    std::vector<Column *> cols;
    cols.reserve(exprs.size());
    for (const auto &input : exprs) {
      uint32_t col_idx = dynamic_pointer_cast<ColumnValueExpression>(input.second)->GetColIdx();
      if (input.second->GetReturnType() != TypeId::kTypeChar) {
        cols.emplace_back(new Column(input.first, input.second->GetReturnType(), col_idx, false, false));
      } else {
        cols.emplace_back(
            new Column(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, col_idx, false, false));
      }
    }
    return new Schema(cols);
  }

 private:
  /** The transaction context for the test */
  Transaction *txn_{nullptr};

  DBStorageEngine *db_test_;
  /** The executor context for the test */
  std::unique_ptr<ExecuteContext> exec_ctx_;
  /** The execution engine */
  std::unique_ptr<ExecuteEngine> execution_engine_;
  /** The collection of allocated expressions, owned by the fixture */
  std::vector<AbstractExpressionRef> allocated_exprs_;

  /** The maximum size allowed for VARCHAR columns */
  static constexpr const uint32_t MAX_VARCHAR_SIZE = 128;
};

#endif  // MINISQL_EXECUTOR_TEST_UTIL_H
