//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){
  TableInfo* tableInfo = TableInfo::Create();
  exec_ctx->GetCatalog()->GetTable(plan->table_name_,tableInfo);
  tableHeap = tableInfo->GetTableHeap();//给私有变量赋值指针
  cur = (tableHeap->Begin(nullptr));
}

void SeqScanExecutor::Init() {

}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  while(cur!=tableHeap->End()){
      if(Field(kTypeInt, 1).CompareEquals(plan_->GetPredicate()->Evaluate(&(*cur)))){
      *row = (*cur);
      *rid = (cur)->GetRowId();
      (cur)++;
      return true;
    }
    (cur)++;
  }
  return false;
}
