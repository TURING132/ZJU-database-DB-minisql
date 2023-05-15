//
// Created by njz on 2023/1/26.
//
#include "executor/plans/delete_plan.h"
#include "executor/plans/insert_plan.h"
#include "executor/plans/seq_scan_plan.h"
#include "executor/plans/update_plan.h"
#include "executor/plans/values_plan.h"
#include "executor_test_util.h"  // NOLINT

// SELECT id FROM table-1 WHERE id < 500
TEST_F(ExecutorTest, SimpleSeqScanTest) {
  // Construct query plan
  TableInfo *table_info;
  GetExecutorContext()->GetCatalog()->GetTable("table-1", table_info);
  const Schema *schema = table_info->GetSchema();
  auto col_a = MakeColumnValueExpression(*schema, 0, "id");
  auto col_b = MakeColumnValueExpression(*schema, 0, "name");
  auto const500 = MakeConstantValueExpression(Field(kTypeInt, 500));
  auto predicate = MakeComparisonExpression(col_a, const500, "<");
  auto out_schema = MakeOutputSchema({{"id", col_a}, {"name", col_b}});
  auto plan = make_shared<SeqScanPlanNode>(out_schema, table_info->GetTableName(), predicate);
  // Execute
  std::vector<Row> result_set{};
  GetExecutionEngine()->ExecutePlan(plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify
  ASSERT_EQ(result_set.size(), 500);
  for (const auto &row : result_set) {
    ASSERT_TRUE(row.GetField(0)->CompareLessThan(Field(kTypeInt, 500)));
  }
}

// DELETE FROM table-1 WHERE id == 50;
TEST_F(ExecutorTest, SimpleDeleteTest) {
  // Construct query plan
  TableInfo *table_info;
  GetExecutorContext()->GetCatalog()->GetTable("table-1", table_info);
  const Schema *schema = table_info->GetSchema();
  auto col_id = MakeColumnValueExpression(*schema, 0, "id");
  auto const50 = MakeConstantValueExpression(Field(kTypeInt, 50));
  auto predicate = MakeComparisonExpression(col_id, const50, "=");
  auto out_schema = MakeOutputSchema({{"id", col_id}});
  auto scan_plan = std::make_shared<SeqScanPlanNode>(out_schema, table_info->GetTableName(), predicate);

  // Create the index
  IndexInfo *index_info = nullptr;
  std::vector<std::string> index_keys{"id"};
  auto r3 =
      GetExecutorContext()->GetCatalog()->CreateIndex("table-1", "index-1", index_keys, GetTxn(), index_info, "bptree");
  ASSERT_EQ(DB_SUCCESS, r3);

  std::vector<Row> result_set;
  GetExecutionEngine()->ExecutePlan(scan_plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify
  ASSERT_EQ(result_set.size(), 1);
  for (const auto &row : result_set) {
    ASSERT_TRUE(row.GetField(0)->CompareEquals(Field(kTypeInt, 50)));
  }

  // DELETE FROM table-1 WHERE id == 50
  const Row index_key = Row(result_set[0]);
  auto delete_plan = std::make_shared<DeletePlanNode>(out_schema, scan_plan, table_info->GetTableName());
  GetExecutionEngine()->ExecutePlan(delete_plan, &result_set, GetTxn(), GetExecutorContext());
  result_set.clear();

  // SELECT id FROM table-1 WHERE id == 50
  GetExecutionEngine()->ExecutePlan(scan_plan, &result_set, GetTxn(), GetExecutorContext());
  ASSERT_TRUE(result_set.empty());

  // Ensure the key was removed from the index
  std::vector<RowId> rids{};
  index_info->GetIndex()->ScanKey(index_key, rids, GetTxn());
  ASSERT_TRUE(rids.empty());
}

// INSERT INTO table-1 VALUES (1001, "aaa", 2.33);
TEST_F(ExecutorTest, SimpleRawInsertTest) {
  // Create values plan node
  auto const1 = MakeConstantValueExpression(Field(kTypeInt, 1001));
  auto const2 = MakeConstantValueExpression(Field(kTypeChar, const_cast<char *>("aaa"), 3, false));
  auto const3 = MakeConstantValueExpression(Field(kTypeFloat, static_cast<float>(2.33)));
  std::vector<std::vector<AbstractExpressionRef>> raw_values{{const1, const2, const3}};
  auto value_plan = std::make_shared<ValuesPlanNode>(nullptr, raw_values);

  // Create insert plan node
  TableInfo *table_info;
  GetExecutorContext()->GetCatalog()->GetTable("table-1", table_info);
  auto insert_plan = std::make_shared<InsertPlanNode>(nullptr, value_plan, "table-1");

  // Execute insert plan, then iterate through table make sure that values were inserted
  std::vector<Row> result_set{};
  GetExecutionEngine()->ExecutePlan(insert_plan, &result_set, GetTxn(), GetExecutorContext());
  result_set.clear();

  // SELECT * FROM table-1 where id = 1001;
  const Schema *schema = table_info->GetSchema();
  auto col_a = MakeColumnValueExpression(*schema, 0, "id");
  auto const500 = MakeConstantValueExpression(Field(kTypeInt, 1001));
  auto predicate = MakeComparisonExpression(col_a, const500, "=");
  auto scan_plan = make_shared<SeqScanPlanNode>(schema, table_info->GetTableName(), predicate);

  GetExecutionEngine()->ExecutePlan(scan_plan, &result_set, GetTxn(), GetExecutorContext());

  // Size
  ASSERT_EQ(result_set.size(), 1);

  // Value
  ASSERT_TRUE(result_set[0].GetField(0)->CompareEquals(Field(kTypeInt, 1001)));
  ASSERT_TRUE(result_set[0].GetField(1)->CompareEquals(Field(kTypeChar, const_cast<char *>("aaa"), 3, false)));
  ASSERT_TRUE(result_set[0].GetField(2)->CompareEquals(Field(kTypeFloat, static_cast<float>(2.33))));
}

// UPDATE table-1 SET name = "minisql" where id = 500;
TEST_F(ExecutorTest, SimpleUpdateTest) {
  // Construct a sequential scan of the table
  TableInfo *table_info;
  GetExecutorContext()->GetCatalog()->GetTable("table-1", table_info);
  const Schema *schema = table_info->GetSchema();
  auto col_a = MakeColumnValueExpression(*schema, 0, "id");
  auto const500 = MakeConstantValueExpression(Field(kTypeInt, 500));
  auto predicate = MakeComparisonExpression(col_a, const500, "=");
  auto scan_plan = make_shared<SeqScanPlanNode>(schema, table_info->GetTableName(), predicate);

  // Execute an initial sequential scan
  std::vector<Row> result_set{};
  GetExecutionEngine()->ExecutePlan(scan_plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify
  ASSERT_EQ(result_set.size(), 1);
  for (const auto &row : result_set) {
    ASSERT_TRUE(row.GetField(0)->CompareEquals(Field(kTypeInt, 500)));
    ASSERT_FALSE(row.GetField(1)->CompareEquals(Field(kTypeChar, const_cast<char *>("minisql"), 7, false)));
  }
  result_set.clear();

  // Construct an update plan
  std::unordered_map<uint32_t, AbstractExpressionRef> update_attrs{};
  auto content = MakeConstantValueExpression(Field(kTypeChar, const_cast<char *>("minisql"), 7, false));
  update_attrs.emplace(static_cast<uint32_t>(1), content);
  auto update_plan = std::make_shared<UpdatePlanNode>(schema, scan_plan, "table-1", update_attrs);

  // Execute update for all rows in the table
  GetExecutionEngine()->ExecutePlan(update_plan, &result_set, GetTxn(), GetExecutorContext());
  result_set.clear();

  // Execute another sequential scan; no rows should be present in the table
  GetExecutionEngine()->ExecutePlan(scan_plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify
  ASSERT_EQ(result_set.size(), 1);
  for (const auto &row : result_set) {
    ASSERT_TRUE(row.GetField(0)->CompareEquals(Field(kTypeInt, 500)));
    ASSERT_TRUE(row.GetField(1)->CompareEquals(Field(kTypeChar, const_cast<char *>("minisql"), 7, false)));
  }
}
