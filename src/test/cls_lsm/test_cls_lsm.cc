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
        std::map<uint64_t, bufferlist> bl_map;
        for (auto i = 0; i < number_of_elems; ++i) {
            bufferlist bl;
            encode(i+1, bl);
            const std::string element_prefix("op-" +to_string(i+1) + "-element-");
            encode(element_prefix, bl);
            bl_map.insert(std::pair<uint64_t, bufferlist>(std::hash<std::string>{}(to_string(i+1)), bl));
        }
        cls_lsm_write(op, tree_name, bl_map);
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

    /*std::vector<std::string> keys;
    keys.push_back(to_string(3));
    keys.push_back(to_string(4));
    keys.push_back(to_string(5));

    auto total_elements = 0;
    std::vector<cls_lsm_entry> entries;
    const auto ret = cls_lsm_read(ioctx, tree_name, cols, entries);
    ASSERT_EQ(0, ret);

    total_elements += entries.size();
    ASSERT_EQ(total_elements, 3);*/
}
