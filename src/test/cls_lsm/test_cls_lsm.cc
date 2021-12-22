#include <errno.h>
#include <string>

#include "include/types.h"
#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_client.h"
#include "cls/lsm/cls_lsm_ops.h"

using namespace librados;

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

    /*void TearDown() override {
        ioctx.close();
        ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
    }*/

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

/*TEST_F(TestClsLsm, Init) {
    const std::string tree_name = "my-tree";
    ObjectWriteOperation op;
    op.create(true);
    std::vector<uint64_t> key_range;
    key_range.push_back(1);
    key_range.push_back(100);
    std::vector<std::string> cols;
    cols.push_back("c1");
    cols.push_back("c2");
    cols.push_back("c3");
    cols.push_back("c4");
    cls_lsm_init(op, pool_name, tree_name, 5, key_range, 8, 100, cols);
    ASSERT_EQ(0, ioctx.operate(tree_name, &op));
}

TEST_F(TestClsLsm, Write) {
    const std::string tree_name = "my-tree";
    ObjectWriteOperation op;
    op.create(true);
    std::vector<uint64_t> key_range;
    key_range.push_back(1);
    key_range.push_back(100);
    std::vector<std::string> cols;
    cols.push_back("c1");
    cols.push_back("c2");
    cols.push_back("c3");
    cols.push_back("c4");
    cls_lsm_init(op, pool_name, tree_name, 5, key_range, 8, 100, cols);
    ASSERT_EQ(0, ioctx.operate(tree_name, &op));

    // test write: 100 elelemts each, expecting 0 (OK)
    test_write(tree_name, 10, 0);
}*/

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
    ASSERT_EQ(total_elements, 3);
    
}
