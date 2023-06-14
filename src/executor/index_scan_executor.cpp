#include "executor/executors/index_scan_executor.h"
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  for(auto it:plan_->indexes_)
    index_idx[it->GetIndexMetadata().GetKeyMapping().at(0)]=it->GetIndex();//存放所有索引列号
  vector<RowId>result;
  Traverse(plan_->GetPredicate(),result);//遍历整个谓词树，返回结果集合
  TableInfo *table_info;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
  auto heap=table_info->GetTableHeap();
  for(auto it:result){
    Row row(it);
    ASSERT(heap->GetTuple(&row, nullptr),"not exist tuple");
    if(plan_->need_filter_){//如果需要进一步筛选就继续
      Field res=plan_->GetPredicate()->Evaluate(&row);
      if(res.CompareEquals(Field(kTypeInt,1))==CmpBool::kTrue)result_.push_back(row);
    }
    else result_.push_back(row);
  }
  iter=result_.begin();

}

bool compare(const RowId &x,const RowId &y){
  if(x.GetPageId()!=y.GetPageId()){
    return x.GetPageId()<y.GetPageId();
  }else{
    return x.GetSlotNum()<y.GetSlotNum();
  }
}

void IndexScanExecutor::Traverse(const AbstractExpressionRef &exp,vector<RowId>&result){
  vector<RowId>res;
  if(exp->GetType()==ExpressionType::LogicExpression){//逻辑运算，也就是and
    Traverse(exp->GetChildAt(0),res);
    Traverse(exp->GetChildAt(1),res);
  }else if(exp->GetType()==ExpressionType::ComparisonExpression){
    ASSERT(exp->GetChildAt(0)->GetType()==ExpressionType::ColumnExpression,"Node type error in index");
    ASSERT(exp->GetChildAt(1)->GetType()==ExpressionType::ConstantExpression,"Node type error in index");
    auto idx =  exp->GetChildAt(0)->GetColIdx();
    auto con = exp->GetChildAt(1)->Evaluate(nullptr);//获得左右两边的值
    auto itr = index_idx.find(idx);//查有没有索引
    if(itr==index_idx.end()) return ;
    vector<Field>fields;
    fields.push_back(con);
    Row temp(fields);
    itr->second->ScanKey(temp,res,nullptr,exp->GetComparisonType());//有索引就到索引里面去搜
  }else{
    ASSERT(false,"error in index scan");
  }
  if(!result.empty()){
    std::sort(res.begin(),res.end(), compare);
    std::sort(result.begin(),result.end(), compare);
    vector<RowId>inter(std::min(res.size(),result.size()));
    std::set_intersection(res.begin(),res.end(),result.begin(),result.end(),inter.begin()
                          ,compare);
    result = inter;//取交集
  }else{
    result = res;//第一次取出来
  }
}


bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  if(iter!=result_.end()){
    *row = *iter;
    *rid = iter->GetRowId();
    iter++;
    return true;
  }
  return false;
}
