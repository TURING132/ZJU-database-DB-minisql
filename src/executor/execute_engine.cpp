#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** After you finish the code for the CatalogManager section,
   *  you can uncomment the commented code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast,double *time,int *affected) {  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      int size = result_set.size();
      int i = 0;
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        i=0;
        for(auto column:schema->GetColumns()){
          int idx = column->GetTableInd();
          auto temp = row.GetField(idx);
          if(temp->GetTypeId()>4){
            printf("error\n");
          }
          data_width[i] = max(data_width[i], int(row.GetField(idx)->toString().size()));
          i++;
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
//      for (const auto &column : schema->GetColumns()) {
//        writer.WriteHeaderCell(column->GetName(), data_width[column->GetTableInd()]);
//      }
      for(int i=0;i<schema->GetColumnCount();i++){
        writer.WriteHeaderCell(schema->GetColumn(i)->GetName(), data_width[i]);
      }

      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
//        for (const auto &column : schema->GetColumns()) {
//          uint32_t idx = column->GetTableInd();
//          writer.WriteCell(row.GetField(column->GetTableInd())->toString(), data_width[idx]);
//        }
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          uint32_t idx = schema->GetColumn(i)->GetTableInd();
          writer.WriteCell(row.GetField(idx)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  if(dbs_.find(db_name)!=dbs_.end())return DB_ALREADY_EXIST;
  auto db=new DBStorageEngine(db_name);
  dbs_[db_name]=db;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  if(dbs_.find(db_name)==dbs_.end())return DB_NOT_EXIST;
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if(current_db_==db_name)current_db_.clear();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  std::stringstream ss;
  ResultWriter writer(ss);
  vector<int>w;
  w.push_back(8);//每一个的宽度
  for(auto itr = dbs_.begin();itr!=dbs_.end();itr++)
    w[0]=max(w[0],(int)((*itr).first.length()));//宽度为最长的那个数据库名
  writer.Divider(w);
  writer.BeginRow();
  writer.WriteHeaderCell("Database",w[0]);
  writer.EndRow();
  writer.Divider(w);

  for(auto it:dbs_){
    writer.BeginRow();
    writer.WriteCell(it.first,w[0]);
    writer.EndRow();
  }

  writer.Divider(w);

  std::cout<<writer.stream_.rdbuf();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string dbName(ast->child_->val_);
  if(dbs_.find(dbName)==dbs_.end())return DB_NOT_EXIST;
  current_db_=dbName;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(current_db_.empty())return DB_FAILED;
  auto clm=context->GetCatalog();
  string header="Tables_in_"+current_db_;
  std::stringstream ss;
  ResultWriter writer(ss);
  vector<int>width;
  vector<TableInfo*>tables;
  width.push_back(header.length());
  clm->GetTables(tables);
  for(auto it:tables)
    width[0]=max(width[0],(int)it->GetTableName().length());
  writer.Divider(width);
  writer.BeginRow();
  writer.WriteHeaderCell(header,width[0]);
  writer.EndRow();
  writer.Divider(width);

  for(auto it:tables){
    writer.BeginRow();
    writer.WriteCell(it->GetTableName(),width[0]);
    writer.EndRow();
  }

  writer.Divider(width);

  std::cout<<writer.stream_.rdbuf();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_.empty())return DB_FAILED;
  auto clm=context->GetCatalog();
  string table_name(ast->child_->val_);
  auto column_first_node=ast->child_->next_->child_;
  auto node=column_first_node;
  vector<string>primary_keys;
  std::vector<string>unique_keys;
  while(node!= nullptr){
    if(node->type_==kNodeColumnList&&string(node->val_)=="primary keys"){//获取primary key
      auto primary_node=node->child_;
      while(primary_node!= nullptr){
        primary_keys.push_back(string(primary_node->val_));
        primary_node=primary_node->next_;
      }
    }
    node=node->next_;
  }
  node=column_first_node;
  uint32_t index=0;
  vector<Column*>columns;

  while(node!= nullptr&&node->type_==kNodeColumnDefinition){
    bool unique=(node->val_!=nullptr&&string(node->val_)=="unique");
    auto detail_node=node->child_;
    string column_name(detail_node->val_);
//    for(auto it:primary_keys)if(it==column_name)unique=true;
    string type(detail_node->next_->val_);
    Column *column;
    if(type=="int")column=new Column(column_name,kTypeInt,index,true,unique);
    if(type=="float")column=new Column(column_name,kTypeFloat,index,true,unique);
    if(type=="char"){
      string len(detail_node->next_->child_->val_);
      for(auto it:len)if(!isdigit(it))return DB_FAILED;
      if(stoi(len)<0)return DB_FAILED;
      column=new Column(column_name,kTypeChar,stoi(len),index,true,unique);
    }
    if(unique){
      unique_keys.push_back(column_name);
    }
    columns.push_back(column);
    ++index;
    node=node->next_;
  }
  Schema *schema=new Schema(columns);
  TableInfo *table_info;
  auto res=clm->CreateTable(table_name,schema,context->GetTransaction(),table_info);
  if(res!=DB_SUCCESS)return res;

  for(auto it:unique_keys){
    string index_name = "UNIQUE_";
    index_name += it + "_";
    index_name += "ON_" + table_name;
    IndexInfo *index_info;
    clm->CreateIndex(table_name, index_name, unique_keys, context->GetTransaction(), index_info, "btree");
  }
  if(primary_keys.size()>0) {
    string index_name = "AUTO_CREATED_INDEX_OF_";
    for (auto it: primary_keys)index_name += it + "_";
    index_name += "ON_" + table_name;
    IndexInfo *index_info;
    clm->CreateIndex(table_name, index_name, primary_keys, context->GetTransaction(), index_info, "btree");
  }

  return res;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_.empty())return DB_FAILED;
  auto clm=context->GetCatalog();
  string table_name(ast->child_->val_);
  dberr_t res=clm->DropTable(table_name);
  if(res!=DB_SUCCESS)return res;
  vector<IndexInfo*>indexes;
  clm->GetTableIndexes(table_name,indexes);
  for(auto it:indexes)
    clm->DropIndex(table_name,it->GetIndexName());
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty())return DB_FAILED;
  auto clm=context->GetCatalog();
  vector<TableInfo*>tables;
  clm->GetTables(tables);
  vector<IndexInfo*>indexes;
  for(auto it:tables)
    clm->GetTableIndexes(it->GetTableName(),indexes);
  std::stringstream ss;
  ResultWriter writer(ss);
  vector<int>width;
  width.push_back(5);
  for(auto it:indexes)
    width[0]=max(width[0],(int)it->GetIndexName().length());
  writer.Divider(width);
  writer.BeginRow();
  writer.WriteHeaderCell("Index",width[0]);
  writer.EndRow();
  writer.Divider(width);

  for(auto it:indexes){
    writer.BeginRow();
    writer.WriteCell(it->GetIndexName(),width[0]);
    writer.EndRow();
  }

  writer.Divider(width);

  std::cout<<writer.stream_.rdbuf();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty())return DB_FAILED;
  auto clm=context->GetCatalog();
  auto node=ast->child_;
  string index_name(node->val_);
  node=node->next_;
  string table_name(node->val_);
  node=node->next_;
  ASSERT(node->type_==kNodeColumnList,"Unexpected type of syntax node when creating index!");
  node=node->child_;
  vector<string>index_keys;
  while(node!= nullptr){
    index_keys.push_back(string(node->val_));
    node=node->next_;
  }
  IndexInfo *index_info;
  return clm->CreateIndex(table_name,index_name,index_keys,context->GetTransaction(),index_info,"btree");
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_.empty())return DB_FAILED;
  auto clm=context->GetCatalog();
  string index_name(ast->child_->val_);
  vector<TableInfo*>tables;
  auto res=DB_INDEX_NOT_FOUND;
  clm->GetTables(tables);
  for(auto it:tables)
    if(clm->DropIndex(it->GetTableName(),index_name)==DB_SUCCESS)res=DB_SUCCESS;
  return res;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name(ast->child_->val_);
  fstream file;
  file.open(file_name);
  if(!file.is_open())return DB_FAILED;
  const int buf_size=1024;
  char cmd[buf_size];
  double time=0;
  int affected=0;
  std::stringstream ss;
  ResultWriter writer(ss);
  while(!file.eof()){
    memset(cmd,0,buf_size);
    int cnt=0;
    char ch;
    while(!file.eof()&&(ch=file.get())!=';')cmd[cnt++]=ch;
    if(ch!=';')break;
    if(!file.eof())file.get();
    cmd[cnt]=ch;
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    MinisqlParserInit();
    // parse
    yyparse();
    // parse result handle
    if (MinisqlParserGetError())printf("%s\n", MinisqlParserGetErrorMessage());
    auto result=Execute(MinisqlGetParserRootNode(),&time,&affected);
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    if(result==DB_QUIT){
      writer.EndInformation(affected,time,false);
      std::cout<<writer.stream_.rdbuf()<<endl;
      return DB_QUIT;
    }
  }
  writer.EndInformation(affected,time,false);
  std::cout<<writer.stream_.rdbuf()<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  current_db_ = "";
 return DB_SUCCESS;
}
