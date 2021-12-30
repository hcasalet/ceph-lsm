#include <errno.h>
#include <string>

#include "include/types.h"
#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_client.h"
#include "cls/lsm/cls_lsm_ops.h"

using namespace librados;
/*
class TestClsLsm : public ::testing::Test {
public:

  static void SetUpTestCase() {
    _pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(_pool_name, _rados));
  }

  static void TearDownTestCase() {
    ASSERT_EQ(0, destroy_one_pool_pp(_pool_name, _rados));
  }

  static std::string _pool_name;
  static librados::Rados _rados;

};

class TestClsLsm : public ::testing::Test {
protected:
    Rados rados;
    std::string pool_name;
    IoCtx ioctx;

    void SetUp() override {
        pool_name = get_temp_pool_name();
        ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
        ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
    }

    void TearDown() override {
        ioctx.close();
        ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
    }

    void test_write(const std::string& tree_name,
            int number_of_elems,
            int expected_rc) {
        ObjectWriteOperation op;
        std::vector<cls_lsm_entry> bl_entries;
        for (auto i = 0; i < number_of_elems; ++i) {
            cls_lsm_entry entry;
            const std::string col_name("col"+to_string(i+1));

            bufferlist bl;
            const std::string element_prefix("op-" +to_string(i+6) + "-element-");
            encode(element_prefix, bl);

            entry.key = std::hash<std::string>{}(to_string(i+6));
            entry.value.insert(std::pair<std::string, bufferlist>(col_name, bl));

            bl_entries.push_back(entry);
        }
        cls_lsm_write(op, tree_name, bl_entries);
        ASSERT_EQ(expected_rc, ioctx.operate(tree_name, &op));
    }
};

TEST_F(TestClsLsm, Read) {
    const std::string tree_name = "my-tree";
    ObjectWriteOperation op;
    op.create(true);
    std::vector<uint64_t> key_range;
    key_range.push_back(1);
    key_range.push_back((size_t)-1);
    key_range.push_back(10);
    
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

    cls_lsm_init(op, pool_name, tree_name, 5, key_range, 8, 3, col_grps);
    ASSERT_EQ(0, ioctx.operate(tree_name, &op));

    // test write: 100 elelemts each, expecting 0 (OK)
    test_write(tree_name, 1, 0);

    std::vector<uint64_t> keys;
    //keys.push_back(std::hash<std::string>{}(to_string(5)));
    keys.push_back(std::hash<std::string>{}(to_string(6)));
    //keys.push_back(std::hash<std::string>{}(to_string(12)));
    //keys.push_back(std::hash<std::string>{}(to_string(15)));
    //keys.push_back(std::hash<std::string>{}(to_string(17)));

    std::vector<std::string> cols;
    cols.push_back("c2");
    cols.push_back("c3");

    auto total_elements = 0;
    std::vector<cls_lsm_entry> entries;
    const auto ret = cls_lsm_read(ioctx, tree_name, keys, cols, entries);
    ASSERT_EQ(0, ret);

    total_elements += entries.size();
    ASSERT_EQ(total_elements, 1);
    
}*/

TEST(ClsLsm, TestLsmInit) {
  Rados cluster;
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

  bufferlist out;
  ASSERT_EQ(524528, ioctx.read("mytree", out, 0, 0));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLsm, TestLsmWriteOne) {
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

  cls_lsm_entry entry;
  entry.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl;
  encode("col1value", bl);
  entry.value.insert(std::pair<std::string, bufferlist>("c1", bl));
  std::vector<cls_lsm_entry> entries;
  entries.push_back(entry);

  cls_lsm_write(op, tree_name, entries);
  //ASSERT_EQ(0, ioctx.operate("mytree", &op));

  bufferlist out;
  ASSERT_EQ(524528, ioctx.read("mytree", out, 0, 0));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

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

  cls_lsm_entry entry1;
  entry1.key = std::hash<std::string>{}(to_string(1));
  bufferlist bl1;
  encode("col1value", bl1);
  entry1.value.insert(std::pair<std::string, bufferlist>("c1", bl1));
  entries.push_back(entry1);

  cls_lsm_write(op, tree_name, entries);
  ASSERT_EQ(0, ioctx.operate("mytree", &op));  

  bufferlist out;
  ASSERT_EQ(526643, ioctx.read("mytree", out, 0, 0));

  cls_lsm_entry entry2;
  entry2.key = std::hash<std::string>{}(to_string(2));
  bufferlist bl2;
  encode("col1value", bl2);
  entry2.value.insert(std::pair<std::string, bufferlist>("c1", bl2));

  entries.clear();
  entries.push_back(entry2);

  cls_lsm_write(op, tree_name, entries);
  //ASSERT_EQ(0, ioctx.operate("mytree", &op));  

  ASSERT_EQ(526694, ioctx.read("mytree", out, 0, 0));

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
/*
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