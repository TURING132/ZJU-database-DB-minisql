//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {

}

void DeleteExecutor::Init() {
  TableInfo* tableInfo;
  tableInfo = TableInfo::Create();
   exec_ctx_->GetCatalog()->GetTable(plan_->table_name_,tableInfo);
   tableHeap = tableInfo->GetTableHeap();
   exec_ctx_->GetCatalog()->GetTableIndexes(tableInfo->GetTableName(),indices);

}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row child_row; RowId child_rowid;//获得扫描到的row

  if(child_executor_->Next(&child_row,&child_rowid)){
    tableHeap->MarkDelete(child_rowid, nullptr);//删除
    //更新index
    for(auto itr = indices.begin();itr!=indices.end();itr++){//遍历每一个indexInfo，删除其中对应的键值对
      vector<Field> fields;
      auto keySchema = (*itr)->GetIndexKeySchema();
      for(int i=0;i<keySchema->GetColumnCount();i++){
        uint32_t idx = keySchema->GetColumn(i)->GetTableInd();//获得索引的序号
        auto field = child_row.GetField(idx);
        fields.push_back(*field);
      }
      Row temp(fields);//对应的索引
      (*itr)->GetIndex()->RemoveEntry(temp, child_rowid,nullptr);//这里的rid并不会被使用
    }
    return true;
  }
  return false;
}