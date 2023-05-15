#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/comparator.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, IndexIteratorTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 16);
  BPlusTree tree(0, engine.bpm_, KP);
  // Generate insert record
  vector<GenericKey *> insert_key;
  for (int i = 1; i <= 50; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    insert_key.emplace_back(key);
    tree.Insert(key, RowId(i * 100), nullptr);
  }
  // Generate delete record
  vector<GenericKey *> delete_key;
  for (int i = 2; i <= 50; i += 2) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    delete_key.emplace_back(key);
    tree.Remove(key);
  }
  // Search keys
  vector<RowId> v;
  vector<GenericKey *> not_delete_key;
  for (auto key : delete_key) {
    ASSERT_FALSE(tree.GetValue(key, v));
  }
  for (int i = 1; i <= 49; i += 2) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    not_delete_key.emplace_back(key);
    ASSERT_TRUE(tree.GetValue(key, v));
    ASSERT_EQ(i * 100, v[v.size() - 1].Get());
  }
  // Iterator
  int i = 0;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    ASSERT_TRUE(KP.CompareKeys(not_delete_key[i++], (*iter).first) == 0);  // if equal, CompareKeys return 0
    EXPECT_EQ(RowId((2 * i - 1) * 100), (*iter).second);
  }
}
