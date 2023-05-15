#ifndef MINISQL_INSTANCE_H
#define MINISQL_INSTANCE_H

#include <memory>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/dberr.h"
#include "common/macros.h"
#include "executor/execute_context.h"
#include "storage/disk_manager.h"

class DBStorageEngine {
 public:
  explicit DBStorageEngine(std::string db_name, bool init = true, uint32_t buffer_pool_size = DEFAULT_BUFFER_POOL_SIZE);

  ~DBStorageEngine();

  std::unique_ptr<ExecuteContext> MakeExecuteContext(Transaction *txn);

 public:
  DiskManager *disk_mgr_;
  BufferPoolManager *bpm_;
  CatalogManager *catalog_mgr_;
  std::string db_file_name_;
  bool init_;
};

#endif  // MINISQL_INSTANCE_H
