#ifndef MINISQL_HEADER_PAGE_H
#define MINISQL_HEADER_PAGE_H

#include <cstring>
#include <string>

#include "page.h"

class HeaderPage : public Page {
 public:
  void Init() { SetRecordCount(0); }

  /**
   * Record related
   *
   * TODO: need to handle overflow
   */
  bool InsertRecord(const std::string &name, page_id_t root_id);

  bool DeleteRecord(const std::string &name);

  bool UpdateRecord(const std::string &name, page_id_t root_id);

  // return root_id if success
  bool GetRootId(const std::string &name, page_id_t *root_id);

  int GetRecordCount();

  static const uint32_t HEADER_PAGE_MAX_ENTRY_NAME_LEN = 32;

 private:
  /**
   * helper functions
   */
  int FindRecord(const std::string &name);

  void SetRecordCount(int record_count);
};

#endif  // MINISQL_HEADER_PAGE_H
