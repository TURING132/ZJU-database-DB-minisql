#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  //强制类型初始化，操作metaPage
  auto metaPage = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  if (metaPage->GetAllocatedPages() >= MAX_VALID_PAGE_ID) return INVALID_PAGE_ID;

  //Find可分配page的extent
  u_int32_t extentsN = metaPage->GetExtentNums();
  u_int32_t extenti;
  for(extenti = 0;extenti<extentsN;extenti++){
    if(metaPage->extent_used_page_[extenti]<BITMAP_SIZE)
      break;
  }

  //计算物理页号
  u_int32_t physical_page_id = extenti * (BITMAP_SIZE + 1) + 1;//每个extent内有BITMAP_SIZE个数据页和一个位图页
                                                               //再加上metaPage是物理页
  char str[PAGE_SIZE]={'\0'};
  ReadPhysicalPage(physical_page_id, str);

  //处理位图页
  BitmapPage<PAGE_SIZE> *bMap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(str);
  u_int32_t page_offset = 0;
  bMap->AllocatePage(page_offset);
  WritePhysicalPage(physical_page_id, str);
  metaPage->num_allocated_pages_++;
  metaPage->extent_used_page_[extenti]++;
  metaPage->num_extents_ = extenti + 1 > extentsN? extenti + 1: extentsN;
  return  extenti * BITMAP_SIZE + page_offset;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  //强制类型初始化，操作metaPage
  auto metaPage = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  //计算物理页号
  u_int32_t physical_page_id = logical_page_id + 1 + logical_page_id / BITMAP_SIZE - logical_page_id % BITMAP_SIZE;
  char str[PAGE_SIZE]={'\0'};
  ReadPhysicalPage(physical_page_id, str);

  //处理位图页
  BitmapPage<PAGE_SIZE>* bMap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(str);
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;
  bMap->DeAllocatePage(page_offset);
  WritePhysicalPage(logical_page_id + 1 + logical_page_id / BITMAP_SIZE - logical_page_id % BITMAP_SIZE, str);
  metaPage->num_allocated_pages_--;
  metaPage->extent_used_page_[logical_page_id / BITMAP_SIZE]--;
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  if(logical_page_id > MAX_VALID_PAGE_ID) return false;
  //计算物理页号
  char str[PAGE_SIZE]={'\0'};
  u_int32_t  physical_page_id = logical_page_id + 1 + logical_page_id / BITMAP_SIZE - logical_page_id % BITMAP_SIZE;
  ReadPhysicalPage(physical_page_id, str);

  //处理位图页
  BitmapPage<PAGE_SIZE> *bMap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(str);
  return bMap->IsPageFree(logical_page_id % BITMAP_SIZE);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) { return logical_page_id + 1 + logical_page_id / BITMAP_SIZE + 1; }

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}