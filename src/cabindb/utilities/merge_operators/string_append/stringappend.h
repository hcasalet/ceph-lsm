/**
 * A MergeOperator for cabindb that implements string append.
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include "include/cabindb/merge_operator.h"
#include "include/cabindb/slice.h"

namespace CABINDB_NAMESPACE {

class StringAppendOperator : public AssociativeMergeOperator {
 public:
  // Constructor: specify delimiter
  explicit StringAppendOperator(char delim_char);

  virtual bool Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const override;

  virtual const char* Name() const override;

 private:
  char delim_;         // The delimiter is inserted between elements

};

}  // namespace CABINDB_NAMESPACE
