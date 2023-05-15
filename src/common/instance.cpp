//
// Created by njz on 2023/1/15.
//
#include "common/instance.h"

DBStorageEngine::DBStorageEngine(std::string db_name, bool init, uint32_t buffer_pool_size)
    : db_file_name_(std::move(db_name)), init_(init) {
  // Init database file if needed
  db_file_name_ = "./databases/"+db_file_name_;
  if (init_) {
    remove(db_file_name_.c_str());
  }
  // Initialize components
  disk_mgr_ = new DiskManager(db_file_name_);
  bpm_ = new BufferPoolManager(buffer_pool_size, disk_mgr_);

  // Allocate static page for db storage engine
  if (init) {
    page_id_t id;
    if (!bpm_->IsPageFree(CATALOG_META_PAGE_ID)) {
      throw logic_error("Catalog meta page not free.");
    }
    if (!bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID)) {
      throw logic_error("Header page not free.");
    }
    if (bpm_->NewPage(id) == nullptr || id != CATALOG_META_PAGE_ID) {
      throw logic_error("Failed to allocate catalog meta page.");
    }
    if (bpm_->NewPage(id) == nullptr || id != INDEX_ROOTS_PAGE_ID) {
      throw logic_error("Failed to allocate header page.");
    }
    if (bpm_->IsPageFree(CATALOG_META_PAGE_ID) || bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID)) {
      exit(1);
    }
    bpm_->UnpinPage(CATALOG_META_PAGE_ID, false);
    bpm_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  } else {
    ASSERT(!bpm_->IsPageFree(CATALOG_META_PAGE_ID), "Invalid catalog meta page.");
    ASSERT(!bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID), "Invalid header page.");
  }
  catalog_mgr_ = new CatalogManager(bpm_, nullptr, nullptr, init);
}

DBStorageEngine::~DBStorageEngine() {
  delete catalog_mgr_;
  delete bpm_;
  delete disk_mgr_;
}

std::unique_ptr<ExecuteContext> DBStorageEngine::MakeExecuteContext(Transaction *txn) {
  return std::make_unique<ExecuteContext>(txn, catalog_mgr_, bpm_);
}
