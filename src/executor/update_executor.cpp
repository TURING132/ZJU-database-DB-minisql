//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
  tableInfo = TableInfo::Create();
  exec_ctx_->GetCatalog()->GetTable(plan_->table_name_,tableInfo);
  tableHeap = tableInfo->GetTableHeap();
  exec_ctx_->GetCatalog()->GetTableIndexes(tableInfo->GetTableName(),indices);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row childRow;
  RowId childRowId;
  if(child_executor_->Next(&childRow,&childRowId)){
    Row updatedRow = GenerateUpdatedTuple(childRow);
    tableHeap->UpdateTuple(updatedRow,childRowId, nullptr);
    //更新索引，删除原来的索引，添加新的索引
    for(auto itr = indices.begin();itr!=indices.end();itr++){
      auto keySchema = (*itr)->GetIndexKeySchema();
      vector<Field>oldFields,newFields;
      for(int i=0;i<keySchema->GetColumnCount();i++){
        uint32_t idx = keySchema->GetColumn(i)->GetTableInd();
        Field * field = childRow.GetField(idx);
        oldFields.push_back(*field);
        field = updatedRow.GetField(idx);
        newFields.push_back(*field);
      }
      Row oldTemp(oldFields),newTemp(newFields);
      (*itr)->GetIndex()->RemoveEntry(oldTemp,childRowId, nullptr);
      (*itr)->GetIndex()->InsertEntry(newTemp,updatedRow.GetRowId(), nullptr);
    }
    return true;
  }else{
    return false;
  }
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  Row updatedRow(src_row);//遍历所有要更新的属性，按照map到的expression更新
  for(auto itr = plan_->update_attrs_.begin();itr!=plan_->update_attrs_.end();itr++){
    Field newField = (*itr).second->Evaluate(&src_row);//这里必然是常数表达式，参数其实也没用
    updatedRow.GetField((*itr).first)->operator=(newField);//更新
  }
  return updatedRow;
}