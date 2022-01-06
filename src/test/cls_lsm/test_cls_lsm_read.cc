#include <errno.h>
#include <string>

#include "include/types.h"
#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_client.h"
#include "cls/lsm/cls_lsm_ops.h"

using namespace librados;

TEST(ClsLsm, TestLsmPrepareRead) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.write_full("mytree", in));

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  col_grps.push_back(cols1);

  std::set<std::string> cols2;
  cols2.insert("c3");
  cols2.insert("c4");
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  bufferlist bl_head;
  cls_lsm_node_head head;
  head.my_object_id = "mytree";
  head.pool = pool_name;
  head.my_level = 0;
  head.levels = 1;
  head.key_range = key_range;
  head.size = 0;
  head.capacity = 3;
  head.column_group_splits = col_grps;
  head.bloomfilter_store_ever = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
  head.bloomfilter_store = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
  
  encode(head, bl_head);
  head.entry_start_offset = bl_head.length() + sizeof(uint64_t) * 3 + sizeof(uint16_t) + LSM_DATA_START_PADDING;
  head.entry_end_offset = head.entry_start_offset;
  encode(head, in);

  std::vector<cls_lsm_entry> entries;
  cls_lsm_entry entry1;
  entry1.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl_data1;
  encode("colsvalue", bl_data1);
  entry1.value.insert(std::pair<std::string, bufferlist>("c1", bl_data1));
  entry1.value.insert(std::pair<std::string, bufferlist>("c2", bl_data1));
  entry1.value.insert(std::pair<std::string, bufferlist>("c3", bl_data1));
  entry1.value.insert(std::pair<std::string, bufferlist>("c4", bl_data1));
  entries.push_back(entry1);

  cls_lsm_entry entry2;
  entry2.key = std::hash<std::string>{}(to_string(25));
  bufferlist bl_data2;
  encode("colsvalue", bl_data2);
  entry2.value.insert(std::pair<std::string, bufferlist>("c1", bl_data2));
  entry2.value.insert(std::pair<std::string, bufferlist>("c2", bl_data2));
  entry2.value.insert(std::pair<std::string, bufferlist>("c3", bl_data2));
  entry2.value.insert(std::pair<std::string, bufferlist>("c4", bl_data2));
  entries.push_back(entry2);

  encode(entries, in);

  std::vector<uint64_t> extra_keys;
  encode(extra_keys, in);

  ASSERT_EQ(0, ioctx.exec("mytree", "lsm", "lsm_create_object", in, out));

}

TEST(ClsLsm, TestLsmReadLeve0) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.write_full("mytree", in));

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  col_grps.push_back(cols1);

  std::set<std::string> cols2;
  cols2.insert("c3");
  cols2.insert("c4");
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  bufferlist bl_head;
  cls_lsm_node_head head;
  head.my_object_id = "mytree";
  head.pool = pool_name;
  head.my_level = 0;
  head.levels = 1;
  head.key_range = key_range;
  head.size = 0;
  head.capacity = 3;
  head.column_group_splits = col_grps;
  head.bloomfilter_store_ever = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
  head.bloomfilter_store = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
  
  encode(head, bl_head);
  head.entry_start_offset = bl_head.length() + sizeof(uint64_t) * 3 + sizeof(uint16_t) + LSM_DATA_START_PADDING;
  head.entry_end_offset = head.entry_start_offset;
  encode(head, in);

  std::vector<cls_lsm_entry> entries;
  cls_lsm_entry entry1;
  entry1.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl_data1;
  encode("colsvalue", bl_data1);
  entry1.value.insert(std::pair<std::string, bufferlist>("c1", bl_data1));
  entry1.value.insert(std::pair<std::string, bufferlist>("c2", bl_data1));
  entry1.value.insert(std::pair<std::string, bufferlist>("c3", bl_data1));
  entry1.value.insert(std::pair<std::string, bufferlist>("c4", bl_data1));
  entries.push_back(entry1);

  cls_lsm_entry entry2;
  entry2.key = std::hash<std::string>{}(to_string(25));
  bufferlist bl_data2;
  encode("colsvalue", bl_data2);
  entry2.value.insert(std::pair<std::string, bufferlist>("c1", bl_data2));
  entry2.value.insert(std::pair<std::string, bufferlist>("c2", bl_data2));
  entry2.value.insert(std::pair<std::string, bufferlist>("c3", bl_data2));
  entry2.value.insert(std::pair<std::string, bufferlist>("c4", bl_data2));
  entries.push_back(entry2);

  encode(entries, in);

  std::vector<uint64_t> extra_keys;
  encode(extra_keys, in);

  ASSERT_EQ(0, ioctx.exec("mytree", "lsm", "lsm_create_object", in, out));

  std::vector<uint64_t> readkeys;
  readkeys.push_back(std::hash<std::string>{}(to_string(25)));
  std::vector<std::string> cols;
  cols.push_back("c2");
  std::vector<cls_lsm_entry> return_entries;
  auto ret = cls_lsm_read(ioctx, "mytree", readkeys, cols, return_entries);
  ASSERT_EQ(1, ret);
}

TEST(ClsLsm, TestLsmReadLeve1) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.write_full("mytree", in));

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  col_grps.push_back(cols1);

  std::set<std::string> cols2;
  cols2.insert("c3");
  cols2.insert("c4");
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  bufferlist bl_head;
  cls_lsm_node_head head;
  head.my_object_id = "mytree";
  head.pool = pool_name;
  head.my_level = 0;
  head.levels = 1;
  head.key_range = key_range;
  head.size = 0;
  head.capacity = 3;
  head.column_group_splits = col_grps;
  head.bloomfilter_store_ever = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
  head.bloomfilter_store = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
  
  encode(head, bl_head);
  head.entry_start_offset = bl_head.length() + sizeof(uint64_t) * 3 + sizeof(uint16_t) + LSM_DATA_START_PADDING;
  head.entry_end_offset = head.entry_start_offset;
  encode(head, in);

  std::vector<cls_lsm_entry> entries;
  cls_lsm_entry entry1;
  entry1.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl_data1;
  encode("colsvalue", bl_data1);
  entry1.value.insert(std::pair<std::string, bufferlist>("c1", bl_data1));
  entry1.value.insert(std::pair<std::string, bufferlist>("c2", bl_data1));
  entry1.value.insert(std::pair<std::string, bufferlist>("c3", bl_data1));
  entry1.value.insert(std::pair<std::string, bufferlist>("c4", bl_data1));
  entries.push_back(entry1);

  cls_lsm_entry entry2;
  entry2.key = std::hash<std::string>{}(to_string(25));
  bufferlist bl_data2;
  encode("colsvalue", bl_data2);
  entry2.value.insert(std::pair<std::string, bufferlist>("c1", bl_data2));
  entry2.value.insert(std::pair<std::string, bufferlist>("c2", bl_data2));
  entry2.value.insert(std::pair<std::string, bufferlist>("c3", bl_data2));
  entry2.value.insert(std::pair<std::string, bufferlist>("c4", bl_data2));
  entries.push_back(entry2);

  encode(entries, in);

  std::vector<uint64_t> extra_keys;
  encode(extra_keys, in);

  ASSERT_EQ(0, ioctx.exec("mytree", "lsm", "lsm_create_object", in, out));

  auto ret = cls_lsm_compact(ioctx, "mytree");
  ASSERT_EQ(0, ret);

  std::vector<uint64_t> readkeys;
  readkeys.push_back(std::hash<std::string>{}(to_string(25)));
  std::vector<std::string> cols;
  cols.push_back("c2");
  std::vector<cls_lsm_entry> return_entries;
  ret = cls_lsm_gather(ioctx, "mytree", readkeys, cols, return_entries);
  ASSERT_EQ(1, ret);
}

TEST(ClsLsm, TestLsmRead) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.write_full("mytree", in));
  ASSERT_EQ(0, ioctx.write_full("mytree/kr-1:cg-1", in));
  ASSERT_EQ(0, ioctx.write_full("mytree/kr-1:cg-2", in));
  ASSERT_EQ(0, ioctx.write_full("mytree/kr-2:cg-1", in));
  ASSERT_EQ(0, ioctx.write_full("mytree/kr-2:cg-2", in));

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  col_grps.push_back(cols1);

  std::set<std::string> cols2;
  cols2.insert("c3");
  cols2.insert("c4");
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  bufferlist bl_head;
  cls_lsm_node_head head;
  head.my_object_id = "mytree";
  head.pool = pool_name;
  head.my_level = 0;
  head.levels = 1;
  head.key_range = key_range;
  head.size = 0;
  head.capacity = 3;
  head.column_group_splits = col_grps;
  head.bloomfilter_store_ever = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
  head.bloomfilter_store = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
  
  encode(head, bl_head);
  head.entry_start_offset = bl_head.length() + sizeof(uint64_t) * 3 + sizeof(uint16_t) + LSM_DATA_START_PADDING;
  head.entry_end_offset = head.entry_start_offset;
  encode(head, in);

  std::vector<cls_lsm_entry> entries;
  cls_lsm_entry entry;
  entry.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl_data;
  encode("colsvalue", bl_data);
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl_data));
  entry.value.insert(std::pair<std::string, bufferlist>("c2", bl_data));
  entry.value.insert(std::pair<std::string, bufferlist>("c3", bl_data));
  entry.value.insert(std::pair<std::string, bufferlist>("c4", bl_data));

  entries.push_back(entry);

  encode(entries, in);

  std::vector<uint64_t> extra_keys;
  extra_keys.push_back(std::hash<std::string>{}(to_string(2)));
  encode(extra_keys, in);

  ASSERT_EQ(0, ioctx.exec("mytree", "lsm", "lsm_create_object", in, out));

  cols1.clear();
  cols1.insert("c1");
  cols2.clear();
  cols2.insert("c2");

  col_grps.clear();
  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  key_range.high_bound = key_range.high_bound / 2;

  in.clear();
  head.my_object_id = "mytree/kr-1:cg-1";
  head.my_level = 1;
  head.key_range = key_range;
  head.column_group_splits = col_grps;

  bl_head.clear();
  encode(head, bl_head);
  head.entry_start_offset = bl_head.length() + sizeof(uint64_t) * 3 + sizeof(uint16_t) + LSM_DATA_START_PADDING;
  head.entry_end_offset = head.entry_start_offset;

  encode(head, in);

  entries.clear();
  entry.key = std::hash<std::string>{}(to_string(2));
  bl_data.clear();
  encode("colsvalue", bl_data);
  entry.value.clear();
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl_data));
  entry.value.insert(std::pair<std::string, bufferlist>("c2", bl_data));

  entries.push_back(entry);

  encode(entries, in);
  extra_keys.clear();
  encode(extra_keys, in);

  ASSERT_EQ(0, ioctx.exec("mytree/kr-1:cg-1", "lsm", "lsm_create_object", in, out));

  cols1.clear();
  cols1.insert("c3");
  cols2.clear();
  cols2.insert("c4");

  col_grps.clear();
  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  in.clear();
  head.my_object_id = "mytree/kr-1:cg-2";
  head.my_level = 1;
  head.column_group_splits = col_grps;

  bl_head.clear();
  encode(head, bl_head);
  head.entry_start_offset = bl_head.length() + sizeof(uint64_t) * 3 + sizeof(uint16_t) + LSM_DATA_START_PADDING;
  head.entry_end_offset = head.entry_start_offset;

  encode(head, in);

  entries.clear();
  entry.key = std::hash<std::string>{}(to_string(2));
  bl_data.clear();
  encode("colsvalue", bl_data);
  entry.value.clear();
  entry.value.insert(std::pair<std::string, bufferlist>("c3", bl_data));
  entry.value.insert(std::pair<std::string, bufferlist>("c4", bl_data));

  entries.push_back(entry);

  encode(entries, in);
  extra_keys.clear();
  encode(extra_keys, in);

  ASSERT_EQ(0, ioctx.exec("mytree/kr-1:cg-2", "lsm", "lsm_create_object", in, out));

  std::vector<uint64_t> keys;
  keys.push_back(std::hash<std::string>{}(to_string(2)));
  std::vector<std::string> cols;
  cols.push_back("c2");

  std::vector<cls_lsm_entry> write_entries;

  const auto ret = cls_lsm_gather(ioctx, "mytree", keys, cols, write_entries);
  ASSERT_EQ(1, ret);

}