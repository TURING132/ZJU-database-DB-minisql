//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {

}

void InsertExecutor::Init() {
  tableInfo = TableInfo::Create();
  exec_ctx_->GetCatalog()->GetTable(plan_->table_name_,tableInfo);
  tableHeap = tableInfo->GetTableHeap();
  exec_ctx_->GetCatalog()->GetTableIndexes(tableInfo->GetTableName(),indices);
  child_executor_->Init();
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row childRow;RowId childRowId;
  if(child_executor_->Next(&childRow,&childRowId)){
    for(auto itr = indices.begin();itr!=indices.end();itr++){//遍历所有index，看是否有unique冲突
      vector<Field> fields;
      auto keySchema = (*itr)->GetIndexKeySchema();
      for(int i=0;i<keySchema->GetColumnCount();i++){//生成此row上面的key
        uint32_t idx = keySchema->GetColumn(i)->GetTableInd();//获得在表中的第几列
        Field* field = childRow.GetField(idx);
        fields.push_back(*field);
      }
      Row temp(fields);
      vector<RowId>scanResult;
      if((*itr)->GetIndex()->ScanKey(temp,scanResult, nullptr)==DB_SUCCESS ){
        printf("unique conflict in insert\n");
        return false;//有冲突直接返回
      }
    }
    tableHeap->InsertTuple(childRow, nullptr);//插入到表中
    *rid = childRow.GetRowId();//返回索引
    //更新索引
    for(auto itr = indices.begin();itr!=indices.end();itr++){
      auto keySchema = (*itr)->GetIndexKeySchema();
      vector<Field>fields;
      for(int i=0;i<keySchema->GetColumnCount();i++){//获取索引
        uint32_t idx = keySchema->GetColumn(i)->GetTableInd();
        Field* field = childRow.GetField(idx);
        fields.push_back(*field);
      }
      Row temp(fields);
      (*itr)->GetIndex()->InsertEntry(temp,childRow.GetRowId(),nullptr);
    }
    return true;
  }else{
    return false;
  }

}