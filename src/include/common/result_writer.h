//
// Created by njz on 2023/2/8.
//

#ifndef MINISQL_RESULTWRITER_H
#define MINISQL_RESULTWRITER_H

#include <iomanip>
#include <iostream>
#include <string>

#include "record/field.h"
class ResultWriter {
 public:
  explicit ResultWriter(std::ostream &stream, bool disable_header = false, const char *separator = "|")
      : disable_header_(disable_header), stream_(stream), separator_(separator) {}
  void WriteCell(const std::string &cell, int width) {
    stream_ << " " << std::setfill(' ') << std::setw(width) << std::left << cell << " " << separator_;
  }
  void WriteHeaderCell(const std::string &cell, int width) {
    if (!disable_header_) {
      stream_ << " " << std::setfill(' ') << std::setw(width) << std::left << cell << " " << separator_;
    }
  }
  void Divider(vector<int> &data_width) {
    stream_ << "+";
    for (auto width : data_width) {
      stream_ << std::setfill('-') << std::setw(width + 3) << std::right << "+";
    }
    stream_ << "\n";
  }
  void BeginRow() { stream_ << "|"; }
  void EndRow() { stream_ << std::endl; }
  void EndInformation(size_t result_size, double time, bool is_scan) {
    if (is_scan) {
      if (!result_size)
        stream_ << "Empty set";
      else
        stream_ << result_size << " row in set";
    } else {
      stream_ << "Query OK, " << result_size << " row affected";
    }
    stream_ << "(" << fixed << setprecision(4) << time / 1000 << " sec)." << std::endl;
  }
  bool disable_header_;
  std::ostream &stream_;
  std::string separator_;
};

#endif  // MINISQL_RESULTWRITER_H
