#include <errno.h>
#include <string>

#include "include/types.h"
#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_client.h"
#include "cls/lsm/cls_lsm_ops.h"

using namespace librados;

TEST(ClsLsm, TestLsmInit)
{
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  col_grps.push_back(cols1);

  std::set<std::string> cols2;
  cols2.insert("c5");
  cols2.insert("c6");
  cols2.insert("c7");
  cols2.insert("c8");
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  client.cls_lsm_init(op, pool_name, "mytree", 5, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  ASSERT_EQ(524534, ioctx.read("mytree", out, 0, 0));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteOne4Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  std::set<std::string> cols2;
  cols2.insert("c3");
  cols2.insert("c4");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  cls_lsm_entry entry;
  entry.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl;
  encode("col1value", bl);
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
  entries.clear();
  entries.push_back(entry);

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteOne8Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  std::set<std::string> cols2;
  cols2.insert("c5");
  cols2.insert("c6");
  cols2.insert("c7");
  cols2.insert("c8");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  cls_lsm_entry entry;
  entry.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl;
  encode("col1value", bl);
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
  entries.clear();
  entries.push_back(entry);

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteOne16Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  cols1.insert("c5");
  cols1.insert("c6");
  cols1.insert("c7");
  cols1.insert("c8");
  std::set<std::string> cols2;
  cols2.insert("c9");
  cols2.insert("c10");
  cols2.insert("c11");
  cols2.insert("c12");
  cols2.insert("c13");
  cols2.insert("c14");
  cols2.insert("c15");
  cols2.insert("c16");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  cls_lsm_entry entry;
  entry.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl;
  encode("col1value", bl);
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c9", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c10", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C11", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c12", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c13", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c14", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c15", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c16", bl));
  entries.clear();
  entries.push_back(entry);

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteOne32Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  cols1.insert("c5");
  cols1.insert("c6");
  cols1.insert("c7");
  cols1.insert("c8");
  cols1.insert("c9");
  cols1.insert("c10");
  cols1.insert("c11");
  cols1.insert("c12");
  cols1.insert("c13");
  cols1.insert("c14");
  cols1.insert("c15");
  cols1.insert("c16");
  std::set<std::string> cols2;
  cols2.insert("c17");
  cols2.insert("c18");
  cols2.insert("c19");
  cols2.insert("c20");
  cols2.insert("c21");
  cols2.insert("c22");
  cols2.insert("c23");
  cols2.insert("c24");
  cols2.insert("c25");
  cols2.insert("c26");
  cols2.insert("c27");
  cols2.insert("c28");
  cols2.insert("c29");
  cols2.insert("c30");
  cols2.insert("c31");
  cols2.insert("c32");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  cls_lsm_entry entry;
  entry.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl;
  encode("col1value", bl);
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c9", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c10", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C11", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c12", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c13", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c14", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c15", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c16", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c17", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c18", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c19", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c20", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c21", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c22", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c23", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c24", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c25", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c26", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C27", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c28", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c29", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c30", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c31", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c32", bl));
  entries.clear();
  entries.push_back(entry);

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteOne64Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  cols1.insert("c5");
  cols1.insert("c6");
  cols1.insert("c7");
  cols1.insert("c8");
  cols1.insert("c9");
  cols1.insert("c10");
  cols1.insert("c11");
  cols1.insert("c12");
  cols1.insert("c13");
  cols1.insert("c14");
  cols1.insert("c15");
  cols1.insert("c16");
  cols1.insert("c17");
  cols1.insert("c18");
  cols1.insert("c19");
  cols1.insert("c20");
  cols1.insert("c21");
  cols1.insert("c22");
  cols1.insert("c23");
  cols1.insert("c24");
  cols1.insert("c25");
  cols1.insert("c26");
  cols1.insert("c27");
  cols1.insert("c28");
  cols1.insert("c29");
  cols1.insert("c30");
  cols1.insert("c31");
  cols1.insert("c32");
  std::set<std::string> cols2;
  cols2.insert("c33");
  cols2.insert("c34");
  cols2.insert("c35");
  cols2.insert("c36");
  cols2.insert("c37");
  cols2.insert("c38");
  cols2.insert("c39");
  cols2.insert("c40");
  cols2.insert("c41");
  cols2.insert("c42");
  cols2.insert("c43");
  cols2.insert("c44");
  cols2.insert("c45");
  cols2.insert("c46");
  cols2.insert("c47");
  cols2.insert("c48");
  cols2.insert("c49");
  cols2.insert("c50");
  cols2.insert("c51");
  cols2.insert("c52");
  cols2.insert("c53");
  cols2.insert("c54");
  cols2.insert("c55");
  cols2.insert("c56");
  cols2.insert("c57");
  cols2.insert("c58");
  cols2.insert("c59");
  cols2.insert("c60");
  cols2.insert("c61");
  cols2.insert("c62");
  cols2.insert("c63");
  cols2.insert("c64");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  cls_lsm_entry entry;
  entry.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl;
  encode("col1value", bl);
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c9", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c10", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C11", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c12", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c13", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c14", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c15", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c16", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c17", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c18", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c19", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c20", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c21", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c22", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c23", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c24", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c25", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c26", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C27", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c28", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c29", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c30", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c31", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c32", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c33", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c34", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c35", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c36", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c37", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c38", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c39", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c40", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c41", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c42", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C43", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c44", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c45", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c46", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c47", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c48", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c49", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c50", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c51", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c52", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c53", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c54", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c55", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c56", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c57", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c58", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C59", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c60", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c61", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c62", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c63", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c64", bl));
  entries.clear();
  entries.push_back(entry);

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteOne128Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  cols1.insert("c5");
  cols1.insert("c6");
  cols1.insert("c7");
  cols1.insert("c8");
  cols1.insert("c9");
  cols1.insert("c10");
  cols1.insert("c11");
  cols1.insert("c12");
  cols1.insert("c13");
  cols1.insert("c14");
  cols1.insert("c15");
  cols1.insert("c16");
  cols1.insert("c17");
  cols1.insert("c18");
  cols1.insert("c19");
  cols1.insert("c20");
  cols1.insert("c21");
  cols1.insert("c22");
  cols1.insert("c23");
  cols1.insert("c24");
  cols1.insert("c25");
  cols1.insert("c26");
  cols1.insert("c27");
  cols1.insert("c28");
  cols1.insert("c29");
  cols1.insert("c30");
  cols1.insert("c31");
  cols1.insert("c32");
  cols1.insert("c33");
  cols1.insert("c34");
  cols1.insert("c35");
  cols1.insert("c36");
  cols1.insert("c37");
  cols1.insert("c38");
  cols1.insert("c39");
  cols1.insert("c40");
  cols1.insert("c41");
  cols1.insert("c42");
  cols1.insert("c43");
  cols1.insert("c44");
  cols1.insert("c45");
  cols1.insert("c46");
  cols1.insert("c47");
  cols1.insert("c48");
  cols1.insert("c49");
  cols1.insert("c50");
  cols1.insert("c51");
  cols1.insert("c52");
  cols1.insert("c53");
  cols1.insert("c54");
  cols1.insert("c55");
  cols1.insert("c56");
  cols1.insert("c57");
  cols1.insert("c58");
  cols1.insert("c59");
  cols1.insert("c60");
  cols1.insert("c61");
  cols1.insert("c62");
  cols1.insert("c63");
  cols1.insert("c64");
  std::set<std::string> cols2;
  cols2.insert("c65");
  cols2.insert("c66");
  cols2.insert("c67");
  cols2.insert("c68");
  cols2.insert("c69");
  cols2.insert("c70");
  cols2.insert("c71");
  cols2.insert("c72");
  cols2.insert("c73");
  cols2.insert("c74");
  cols2.insert("c75");
  cols2.insert("c76");
  cols2.insert("c77");
  cols2.insert("c78");
  cols2.insert("c79");
  cols2.insert("c80");
  cols2.insert("c81");
  cols2.insert("c82");
  cols2.insert("c83");
  cols2.insert("c84");
  cols2.insert("c85");
  cols2.insert("c86");
  cols2.insert("c87");
  cols2.insert("c88");
  cols2.insert("c89");
  cols2.insert("c90");
  cols2.insert("c91");
  cols2.insert("c92");
  cols2.insert("c93");
  cols2.insert("c94");
  cols2.insert("c95");
  cols2.insert("c96");
  cols2.insert("c97");
  cols2.insert("c98");
  cols2.insert("c99");
  cols2.insert("c100");
  cols2.insert("c101");
  cols2.insert("c102");
  cols2.insert("c103");
  cols2.insert("c104");
  cols2.insert("c105");
  cols2.insert("106");
  cols2.insert("c107");
  cols2.insert("c108");
  cols2.insert("c109");
  cols2.insert("c110");
  cols2.insert("c111");
  cols2.insert("c112");
  cols2.insert("c113");
  cols2.insert("c114");
  cols2.insert("c115");
  cols2.insert("c116");
  cols2.insert("c117");
  cols2.insert("c118");
  cols2.insert("c119");
  cols2.insert("c120");
  cols2.insert("c121");
  cols2.insert("c122");
  cols2.insert("c123");
  cols2.insert("c124");
  cols2.insert("c125");
  cols2.insert("c126");
  cols2.insert("c127");
  cols2.insert("c128");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  cls_lsm_entry entry;
  entry.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl;
  encode("col1value", bl);
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c9", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c10", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C11", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c12", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c13", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c14", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c15", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c16", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c17", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c18", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c19", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c20", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c21", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c22", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c23", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c24", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c25", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c26", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C27", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c28", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c29", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c30", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c31", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c32", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c33", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c34", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c35", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c36", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c37", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c38", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c39", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c40", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c41", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c42", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C43", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c44", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c45", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c46", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c47", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c48", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c49", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c50", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c51", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c52", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c53", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c54", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c55", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c56", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c57", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c58", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C59", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c60", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c61", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c62", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c63", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c64", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c65", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c66", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c67", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c68", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c69", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c70", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c71", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c72", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c73", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c74", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C75", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c76", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c77", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c78", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c79", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c80", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c81", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c82", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c83", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c84", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c85", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c86", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c87", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c88", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c89", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c90", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C91", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c92", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c93", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c94", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c95", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c96", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c97", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c98", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c99", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c100", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c101", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c102", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c103", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c104", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c105", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c106", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C107", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c108", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c109", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c110", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c111", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c112", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c113", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c114", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c115", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c116", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c117", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c118", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c119", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c120", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c121", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c122", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("C123", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c124", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c125", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c126", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c127", bl));
  entry.value.insert(std::pair<std::string, bufferlist>("c128", bl));
  entries.clear();
  entries.push_back(entry);

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteTen4Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  std::set<std::string> cols2;
  cols2.insert("c3");
  cols2.insert("c4");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;
  bufferlist bl;
  encode("col1value", bl);

  for (int i = 0; i < 10; i++)
  {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i));
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
    entries.push_back(entry);
  }

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteTen8Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  std::set<std::string> cols2;
  cols2.insert("c5");
  cols2.insert("c6");
  cols2.insert("c7");
  cols2.insert("c8");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  bufferlist bl;
  encode("col1value", bl);

  for (int i = 0; i < 10; i++)
  {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i));
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
    entries.push_back(entry);
  }

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteTen16Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  cols1.insert("c5");
  cols1.insert("c6");
  cols1.insert("c7");
  cols1.insert("c8");
  std::set<std::string> cols2;
  cols2.insert("c9");
  cols2.insert("c10");
  cols2.insert("c11");
  cols2.insert("c12");
  cols2.insert("c13");
  cols2.insert("c14");
  cols2.insert("c15");
  cols2.insert("c16");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  bufferlist bl;
  encode("col1value", bl);

  for (int i = 0; i < 10; i++)
  {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i));
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c9", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c10", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C11", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c12", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c13", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c14", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c15", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c16", bl));
    entries.push_back(entry);
  }

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteTen32Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  cols1.insert("c5");
  cols1.insert("c6");
  cols1.insert("c7");
  cols1.insert("c8");
  cols1.insert("c9");
  cols1.insert("c10");
  cols1.insert("c11");
  cols1.insert("c12");
  cols1.insert("c13");
  cols1.insert("c14");
  cols1.insert("c15");
  cols1.insert("c16");
  std::set<std::string> cols2;
  cols2.insert("c17");
  cols2.insert("c18");
  cols2.insert("c19");
  cols2.insert("c20");
  cols2.insert("c21");
  cols2.insert("c22");
  cols2.insert("c23");
  cols2.insert("c24");
  cols2.insert("c25");
  cols2.insert("c26");
  cols2.insert("c27");
  cols2.insert("c28");
  cols2.insert("c29");
  cols2.insert("c30");
  cols2.insert("c31");
  cols2.insert("c32");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  bufferlist bl;
  encode("col1value", bl);

  for (int i = 0; i < 10; i++)
  {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i));
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c9", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c10", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C11", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c12", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c13", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c14", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c15", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c16", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c17", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c18", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c19", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c20", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c21", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c22", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c23", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c24", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c25", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c26", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C27", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c28", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c29", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c30", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c31", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c32", bl));
    entries.push_back(entry);
  }

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteTen64Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  cols1.insert("c5");
  cols1.insert("c6");
  cols1.insert("c7");
  cols1.insert("c8");
  cols1.insert("c9");
  cols1.insert("c10");
  cols1.insert("c11");
  cols1.insert("c12");
  cols1.insert("c13");
  cols1.insert("c14");
  cols1.insert("c15");
  cols1.insert("c16");
  cols1.insert("c17");
  cols1.insert("c18");
  cols1.insert("c19");
  cols1.insert("c20");
  cols1.insert("c21");
  cols1.insert("c22");
  cols1.insert("c23");
  cols1.insert("c24");
  cols1.insert("c25");
  cols1.insert("c26");
  cols1.insert("c27");
  cols1.insert("c28");
  cols1.insert("c29");
  cols1.insert("c30");
  cols1.insert("c31");
  cols1.insert("c32");
  std::set<std::string> cols2;
  cols2.insert("c33");
  cols2.insert("c34");
  cols2.insert("c35");
  cols2.insert("c36");
  cols2.insert("c37");
  cols2.insert("c38");
  cols2.insert("c39");
  cols2.insert("c40");
  cols2.insert("c41");
  cols2.insert("c42");
  cols2.insert("c43");
  cols2.insert("c44");
  cols2.insert("c45");
  cols2.insert("c46");
  cols2.insert("c47");
  cols2.insert("c48");
  cols2.insert("c49");
  cols2.insert("c50");
  cols2.insert("c51");
  cols2.insert("c52");
  cols2.insert("c53");
  cols2.insert("c54");
  cols2.insert("c55");
  cols2.insert("c56");
  cols2.insert("c57");
  cols2.insert("c58");
  cols2.insert("c59");
  cols2.insert("c60");
  cols2.insert("c61");
  cols2.insert("c62");
  cols2.insert("c63");
  cols2.insert("c64");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  bufferlist bl;
  encode("col1value", bl);

  for (int i = 0; i < 10; i++)
  {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i));
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c9", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c10", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C11", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c12", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c13", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c14", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c15", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c16", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c17", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c18", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c19", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c20", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c21", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c22", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c23", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c24", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c25", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c26", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C27", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c28", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c29", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c30", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c31", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c32", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c33", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c34", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c35", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c36", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c37", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c38", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c39", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c40", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c41", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c42", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C43", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c44", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c45", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c46", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c47", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c48", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c49", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c50", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c51", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c52", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c53", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c54", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c55", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c56", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c57", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c58", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C59", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c60", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c61", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c62", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c63", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c64", bl));
    entries.push_back(entry);
  }

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteTen128Cols)
{
  Rados cluster;
  std::vector<std::string> obj_ids{"mytree_all",
                                   "mytree",
                                   "mytree/kr-1:cg-1",
                                   "mytree/kr-1:cg-2",
                                   "mytree/kr-2:cg-1",
                                   "mytree/kr-2:cg-2"};
  ClsLsmClient client(obj_ids);
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string> > col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  cols1.insert("c5");
  cols1.insert("c6");
  cols1.insert("c7");
  cols1.insert("c8");
  cols1.insert("c9");
  cols1.insert("c10");
  cols1.insert("c11");
  cols1.insert("c12");
  cols1.insert("c13");
  cols1.insert("c14");
  cols1.insert("c15");
  cols1.insert("c16");
  cols1.insert("c17");
  cols1.insert("c18");
  cols1.insert("c19");
  cols1.insert("c20");
  cols1.insert("c21");
  cols1.insert("c22");
  cols1.insert("c23");
  cols1.insert("c24");
  cols1.insert("c25");
  cols1.insert("c26");
  cols1.insert("c27");
  cols1.insert("c28");
  cols1.insert("c29");
  cols1.insert("c30");
  cols1.insert("c31");
  cols1.insert("c32");
  cols1.insert("c33");
  cols1.insert("c34");
  cols1.insert("c35");
  cols1.insert("c36");
  cols1.insert("c37");
  cols1.insert("c38");
  cols1.insert("c39");
  cols1.insert("c40");
  cols1.insert("c41");
  cols1.insert("c42");
  cols1.insert("c43");
  cols1.insert("c44");
  cols1.insert("c45");
  cols1.insert("c46");
  cols1.insert("c47");
  cols1.insert("c48");
  cols1.insert("c49");
  cols1.insert("c50");
  cols1.insert("c51");
  cols1.insert("c52");
  cols1.insert("c53");
  cols1.insert("c54");
  cols1.insert("c55");
  cols1.insert("c56");
  cols1.insert("c57");
  cols1.insert("c58");
  cols1.insert("c59");
  cols1.insert("c60");
  cols1.insert("c61");
  cols1.insert("c62");
  cols1.insert("c63");
  cols1.insert("c64");
  std::set<std::string> cols2;
  cols2.insert("c65");
  cols2.insert("c66");
  cols2.insert("c67");
  cols2.insert("c68");
  cols2.insert("c69");
  cols2.insert("c70");
  cols2.insert("c71");
  cols2.insert("c72");
  cols2.insert("c73");
  cols2.insert("c74");
  cols2.insert("c75");
  cols2.insert("c76");
  cols2.insert("c77");
  cols2.insert("c78");
  cols2.insert("c79");
  cols2.insert("c80");
  cols2.insert("c81");
  cols2.insert("c82");
  cols2.insert("c83");
  cols2.insert("c84");
  cols2.insert("c85");
  cols2.insert("c86");
  cols2.insert("c87");
  cols2.insert("c88");
  cols2.insert("c89");
  cols2.insert("c90");
  cols2.insert("c91");
  cols2.insert("c92");
  cols2.insert("c93");
  cols2.insert("c94");
  cols2.insert("c95");
  cols2.insert("c96");
  cols2.insert("c97");
  cols2.insert("c98");
  cols2.insert("c99");
  cols2.insert("c100");
  cols2.insert("c101");
  cols2.insert("c102");
  cols2.insert("c103");
  cols2.insert("c104");
  cols2.insert("c105");
  cols2.insert("106");
  cols2.insert("c107");
  cols2.insert("c108");
  cols2.insert("c109");
  cols2.insert("c110");
  cols2.insert("c111");
  cols2.insert("c112");
  cols2.insert("c113");
  cols2.insert("c114");
  cols2.insert("c115");
  cols2.insert("c116");
  cols2.insert("c117");
  cols2.insert("c118");
  cols2.insert("c119");
  cols2.insert("c120");
  cols2.insert("c121");
  cols2.insert("c122");
  cols2.insert("c123");
  cols2.insert("c124");
  cols2.insert("c125");
  cols2.insert("c126");
  cols2.insert("c127");
  cols2.insert("c128");

  col_grps.push_back(cols1);
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  client.cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  bufferlist bl;
  encode("col1value", bl);

  for (int i = 0; i < 10; i++)
  {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i));
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c2", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c3", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c4", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c5", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c6", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c7", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c8", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c9", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c10", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C11", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c12", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c13", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c14", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c15", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c16", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c17", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c18", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c19", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c20", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c21", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c22", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c23", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c24", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c25", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c26", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C27", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c28", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c29", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c30", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c31", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c32", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c33", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c34", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c35", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c36", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c37", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c38", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c39", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c40", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c41", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c42", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C43", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c44", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c45", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c46", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c47", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c48", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c49", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c50", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c51", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c52", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c53", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c54", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c55", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c56", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c57", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c58", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C59", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c60", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c61", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c62", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c63", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c64", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c65", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c66", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c67", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c68", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c69", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c70", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c71", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c72", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c73", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c74", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C75", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c76", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c77", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c78", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c79", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c80", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c81", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c82", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c83", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c84", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c85", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c86", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c87", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c88", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c89", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c90", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C91", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c92", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c93", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c94", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c95", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c96", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c97", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c98", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c99", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c100", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c101", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c102", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c103", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c104", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c105", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c106", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C107", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c108", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c109", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c110", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c111", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c112", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c113", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c114", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c115", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c116", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c117", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c118", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c119", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c120", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c121", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c122", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("C123", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c124", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c125", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c126", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c127", bl));
    entry.value.insert(std::pair<std::string, bufferlist>("c128", bl));
    entries.push_back(entry);
  }

  ObjectWriteOperation op2;
  client.cls_lsm_write(op2, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op2));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

/*
TEST(ClsLsm, TestLsmWriteTwo) {
  Rados cluster;
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  std::set<std::string> cols2;
  cols2.insert("c2");
  std::set<std::string> cols3;
  cols3.insert("c3");
  std::set<std::string> cols4;
  cols1.insert("c4");
  col_grps.push_back(cols1);
  col_grps.push_back(cols2);
  col_grps.push_back(cols3);
  col_grps.push_back(cols4);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  cls_lsm_init(op, pool_name, "mytree", 1, key_range, 3, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  for (int i=0; i < 4; i++) {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i+1));
    bufferlist bl;
    encode("col1value", bl);
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entries.clear();
    entries.push_back(entry);

    ObjectWriteOperation op2;
    cls_lsm_write(op2, tree_name, entries);
    ASSERT_EQ(0, ioctx.operate("mytree", &op2));
  }


  // read
  
  std::vector<uint64_t> keys;
  keys.push_back(std::hash<std::string>{}(to_string(4)));
  std::cout << "Start reading...." << endl;
  std::vector<std::string> cols;
  cols.push_back("c2");

  std::vector<cls_lsm_entry> write_entries;
  const auto ret = cls_lsm_read(ioctx, tree_name, keys, cols, write_entries);
  ASSERT_EQ(1, ret);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteTen_noCompaction) {
  Rados cluster;
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  std::set<std::string> cols2;
  cols2.insert("c2");
  std::set<std::string> cols3;
  cols3.insert("c3");
  std::set<std::string> cols4;
  cols1.insert("c4");
  col_grps.push_back(cols1);
  col_grps.push_back(cols2);
  col_grps.push_back(cols3);
  col_grps.push_back(cols4);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  for (int i=0; i < 10; i++) {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i+1));
    bufferlist bl;
    encode("col1value", bl);
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entries.clear();
    entries.push_back(entry);

    ObjectWriteOperation op2;
    cls_lsm_write(op2, tree_name, entries);
    ASSERT_EQ(0, ioctx.operate("mytree", &op2));
  }

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteTwelve_noCompaction) {
  Rados cluster;
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  std::set<std::string> cols2;
  cols2.insert("c2");
  std::set<std::string> cols3;
  cols3.insert("c3");
  std::set<std::string> cols4;
  cols1.insert("c4");
  col_grps.push_back(cols1);
  col_grps.push_back(cols2);
  col_grps.push_back(cols3);
  col_grps.push_back(cols4);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  for (int i=0; i < 12; i++) {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i+1));
    bufferlist bl;
    encode("col1value", bl);
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entries.clear();
    entries.push_back(entry);

    ObjectWriteOperation op2;
    cls_lsm_write(op2, tree_name, entries);
    ASSERT_EQ(0, ioctx.operate("mytree", &op2));
  }

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteThirteen_withCompaction) {
  Rados cluster;
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  std::set<std::string> cols2;
  cols2.insert("c2");
  std::set<std::string> cols3;
  cols3.insert("c3");
  std::set<std::string> cols4;
  cols1.insert("c4");
  col_grps.push_back(cols1);
  col_grps.push_back(cols2);
  col_grps.push_back(cols3);
  col_grps.push_back(cols4);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  for (int i=0; i < 13; i++) {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i+1));
    bufferlist bl;
    encode("col1value", bl);
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entries.clear();
    entries.push_back(entry);

    ObjectWriteOperation op2;
    cls_lsm_write(op2, tree_name, entries);
    ASSERT_EQ(0, ioctx.operate("mytree", &op2));
  }

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteTwenty_withCompaction) {
  Rados cluster;
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  std::set<std::string> cols2;
  cols2.insert("c2");
  std::set<std::string> cols3;
  cols3.insert("c3");
  std::set<std::string> cols4;
  cols1.insert("c4");
  col_grps.push_back(cols1);
  col_grps.push_back(cols2);
  col_grps.push_back(cols3);
  col_grps.push_back(cols4);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  cls_lsm_init(op, pool_name, "mytree", 1, key_range, 15, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  std::vector<cls_lsm_entry> entries;

  for (int i=0; i < 20; i++) {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i+1));
    bufferlist bl;
    encode("col1value", bl);
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
    entries.clear();
    entries.push_back(entry);

    ObjectWriteOperation op2;
    cls_lsm_write(op2, tree_name, entries);
    ASSERT_EQ(0, ioctx.operate("mytree", &op2));
  }

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}


TEST(ClsLsm, TestLsmWriteMultiple) {
  Rados cluster;
  const std::string tree_name = "mytree";
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;
  op.create(true);

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  col_grps.push_back(cols1);

  std::set<std::string> cols2;
  cols2.insert("c5");
  cols2.insert("c6");
  cols2.insert("c7");
  cols2.insert("c8");
  col_grps.push_back(cols2);

  cls_lsm_key_range key_range;
  key_range.low_bound = 1;
  key_range.high_bound = (size_t)-1;
  key_range.splits = 2;

  cls_lsm_init(op, pool_name, "mytree", 5, key_range, 3, col_grps);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));

  std::vector<cls_lsm_entry> entries;
  for (int i = 1; i < 4; i++) {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i));
    bufferlist bl;
    encode("col1value", bl);
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
  
    entries.clear();
    entries.push_back(entry);

    cls_lsm_write(op, tree_name, entries);
    //ASSERT_EQ(0, ioctx.operate("mytree", &op)); 
  }
  
  bufferlist out;
  ASSERT_EQ(524528, ioctx.read("mytree", out, 0, 0));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteCompact) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  std::vector<std::set<std::string>> col_grps;
  std::set<std::string> cols1;
  cols1.insert("c1");
  cols1.insert("c2");
  cols1.insert("c3");
  cols1.insert("c4");
  col_grps.push_back(cols1);

  std::set<std::string> cols2;
  cols2.insert("c5");
  cols2.insert("c6");
  cols2.insert("c7");
  cols2.insert("c8");
  col_grps.push_back(cols2);

  bufferlist in, out;
  cls_lsm_init_op call;
  call.pool_name = pool_name;
  call.tree_name = "mytree";
  call.levels = 5;
  call.key_range.low_bound = 1;
  call.key_range.high_bound = (size_t)-1;
  call.key_range.splits = 2;
  call.fan_out = 2;
  call.max_capacity = 3;
  call.all_column_splits = col_grps;
  encode(call, in);
  ASSERT_EQ(0, ioctx.exec("mytree", "lsm", "lsm_init", in, out));

  std::vector<cls_lsm_entry> entries;
  for (int i = 1; i < 4; i++) {
    cls_lsm_entry entry;
    entry.key = std::hash<std::string>{}(to_string(i));
    bufferlist bl;
    encode("col1value", bl);
    entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
  
    entries.clear();
    entries.push_back(entry);

    in.clear();
    cls_lsm_append_entries_op call2;
    call2.tree_name = "mytree";
    call2.entries = std::move(entries);
    encode(call2, in);
    ASSERT_EQ(0, ioctx.exec("mytree", "lsm", "lsm_write_node", in, out));
  }
  
  ASSERT_EQ(526745, ioctx.read("mytree", out, 0, 0));

  cls_lsm_entry entry;
  entry.key = std::hash<std::string>{}(to_string(4));
  bufferlist bl;
  encode("col1value", bl);
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));

  entries.clear();
  entries.push_back(entry);

  in.clear();
  cls_lsm_append_entries_op call3;
  call3.tree_name = "mytree";
  call3.entries = std::move(entries);
  encode(call3, in);

  std::cout << "key = " << entry.key << endl;
  ASSERT_EQ(0, ioctx.exec("mytree", "lsm", "lsm_write_node", in, out));

  //ASSERT_EQ(526745, ioctx.read("mytree", out, 0, 0));

  //ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}*/