#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_const.h"
#include "cls/lsm/cls_lsm_bloomfilter.h"
#include "cls/lsm/cls_lsm_util.h"
#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_read_optimized.h"

using namespace librados;

void ClsReadOptimizedClient::InitClient(std::string tree, uint64_t key_low, uint64_t key_high, int splits, int levels,
        std::map<int, std::vector<std::vector<std::string>>>& col_map)
{
    tree_name = tree;
    key_low_bound = key_low;
    key_high_bound = key_high;
    key_splits = splits;
    levels = levels;
    column_map = col_map;
    uint64_t filters = 1;
    for (int i = 0; i <= levels; i++) {
        std::vector<std::vector<bool>> bloomfilters;
        for (uint64_t j = 0; j < filters; j++) {
            bloomfilters.push_back(std::vector<bool>(BLOOM_FILTER_STORE_SIZE_64K, false));
        }
        bloomfilter_store.insert(std::pair<int, std::vector<std::vector<bool>>>(i, bloomfilters));

        if (i > 1) {
            filters *= splits;
        }
    }
}

void ClsReadOptimizedClient::cls_read_optimized_init(librados::ObjectWriteOperation& op,
                  const std::string& pool_name,
                  const std::string& tree_name,
                  cls_lsm_key_range& key_range)
{
    int level_splits = 0;
    uint64_t columns = 1;
    for (int i = 0; i <= levels; i++) {
        if (i <= 1) {
            level_splits = 1;
        } else {
            level_splits *= key_range.splits;
        }

        uint64_t low_bound = key_range.low_bound;
        uint64_t increment = (key_range.high_bound - key_range.low_bound) / level_splits;
        uint64_t high_bound = low_bound + increment;

        for (int j = 0; j < level_splits; j++) {

            if (i > 0) {
                columns = column_map[i].size();
            }

            for (uint64_t k = 0; k < columns; k++) {
                bufferlist in;
                cls_lsm_init_op call;
                call.pool_name = pool_name;
                call.obj_name = tree_name+"/level-"+to_string(i)+"/keyrange-"+to_string(j)+"/columngroup-"+to_string(k);
                call.key_range.low_bound = low_bound;
                call.key_range.high_bound = high_bound;
                call.key_range.splits  = level_splits;
                encode(call, in);
                op.exec(LSM_CLASS, LSM_INIT, in);
            }

            low_bound = high_bound;
            high_bound = low_bound + increment;
        }
    }
}

int ClsReadOptimizedClient::cls_read_optimized_read(librados::IoCtx& io_ctx, const std::string& pool_name,
                uint64_t key, const std::vector<std::string> *columns, cls_lsm_entry& entry)
{
    std::vector<std::string> obj_ids;
    for (int i = 1; i <= levels; i++) {
        int key_group = 0;
        if (i > 1) {
            key_group = get_key_group(key_low_bound, key_high_bound, key_splits, i, key);
        }
        if (key_group == -1) {
            std::cout << "key " << key << " out of range!" << endl;
            return 0;
        }

        if (lsm_bloomfilter_contains(bloomfilter_store[i][key_group], to_string(key))) {
            std::vector<int> col_groups;
            if (!columns) {
                for (uint64_t j = 0; j < column_map[i].size(); j++) {
                    col_groups.push_back(j);
                }
            } else {
                col_groups = get_col_group(*columns, i, column_map);
            }

            for (auto col_group : col_groups) {
                obj_ids.push_back(construct_object_id(tree_name, i, key_group, col_group));
            }
            break;
        }
    }

    if (obj_ids.size() == 0) {
        std::cout << "key " << key << " does not exist!" << endl;
        return 0;
    }

    bufferlist in, out;
    int r;
    if (obj_ids.size() == 1) {
        // read from one single object
        encode(key, in);

        r = io_ctx.exec(obj_ids[0], LSM_CLASS, LSM_READ_NODE, in, out);
        if (r < 0) {
            return r;
        }

        auto iter = out.cbegin();
        try {
            decode(entry, iter);
        } catch (buffer::error &err) {
            std::cout << "in cls_lsm_read : decoding cls_lsm_entry - " << err.what() << endl;
            return -EIO;
        }
    } else {
        // gather
        encode(obj_ids, in);
        encode(pool_name, in);

        std::string root = tree_name+"/level-0/keyrange-0/columngroup-0";
        r = io_ctx.exec(root, LSM_CLASS, LSM_GATHER, in, out);
    }

    return r;
}

void ClsReadOptimizedClient::cls_read_optimized_write(librados::IoCtx& io_ctx, const std::string& oid, cls_lsm_entry& entry)
{
    std::map<std::string, bufferlist> tgt_child_objects;
    int i = 0;
    for (auto item : entry.value) {
        std::map<std::string, bufferlist> new_entry_value;
        new_entry_value.insert(std::pair<std::string, bufferlist>(item.first, item.second));

        std::vector<cls_lsm_entry> new_entries;
        cls_lsm_entry new_entry;
        new_entry.key = entry.key;
        new_entry.value = new_entry_value;
        new_entries.push_back(new_entry);

        bufferlist bl_entry;
        encode(new_entries, bl_entry);

        std::string obj_name = tree_name+"/level-1/keyrange-0/columngroup-"+to_string(i);
        tgt_child_objects.insert(std::pair<std::string, bufferlist>(obj_name, bl_entry));
        
        i += 1;
    }

    bufferlist in, out;
    encode(tgt_child_objects, in);
 
    io_ctx.exec(oid, LSM_CLASS, LSM_COMPACT, in, out);

    // register data in the bloomfilter stores
    for (uint64_t i = 0; i < bloomfilter_store[1].size(); i++) {
        lsm_bloomfilter_insert(bloomfilter_store[1][i], to_string(entry.key));
    }
}

int ClsReadOptimizedClient::cls_read_optimized_compact(librados::IoCtx& io_ctx, const std::string& oid)
{
    // get level from object_id
    int level = get_level_from_object_id(oid);

    bufferlist in, out, in2;
    encode(column_map[level], in);

    // Get ready to call scatter
    int r = io_ctx.exec(oid, LSM_CLASS, LSM_PREPARE_COMPACTION, in, out);
    if (r < 0) {
        return r;
    }

    // get the result
    in2 = out;

    in.clear();
    in.claim_append(out);

    // call scatter to compact
    r = io_ctx.exec(oid, LSM_CLASS, LSM_COMPACT, in, out);
    if (r < 0) {
        return r;
    }

    // update the bloomfilters for the child objects to be compacted to
    ClsReadOptimizedClient::update_bloomfilter(in2, level);

    // clear all data out of the compacted object
    r = io_ctx.exec(oid, LSM_CLASS, LSM_UPDATE_POST_COMPACTION, in, out);
    if (r < 0) {
        return r;
    }

    // clear bloomfilter for the compacted object
    int keyrange = get_key_range_from_object_id(oid);
    lsm_bloomfilter_clear(bloomfilter_store[level][keyrange]);
    
    return 0;
}

int ClsReadOptimizedClient::cls_read_optimized_scan(librados::IoCtx& io_ctx,
                 uint64_t start_key, uint64_t max_key,
                 std::vector<std::string>& columns,
                 std::vector<cls_lsm_entry>& entries)
{
    std::vector<std::string> obj_ids;
    // first scan the object at level 0

    for (int i = 0; i <= levels; i++) {
        int key_group_start = get_key_group(key_low_bound, key_high_bound, key_splits, i, start_key);
        if (key_group_start == -1) {
            std::cout << "start key " << start_key << " out of range!" << endl;
            return 0;
        }

        int key_group_end = get_key_group(key_low_bound, key_high_bound, key_splits, i, max_key);
        if (key_group_end == -1) {
            std::cout << "max key " << max_key << " out of range!" << endl;
            return 0;
        }

        std::vector<int> col_groups = get_col_group(columns, i, column_map);
        for (int j = key_group_start; j <= key_group_end; j++) {
            for (auto col_group : col_groups) {
                obj_ids.push_back(construct_object_id(tree_name, i, j, col_group));
            }
        }

        if (obj_ids.size() == 1) {
            // read all data between (start_key, end_key) in one object
        } else {
            // gather data between (start_key, end_key) from all objects
        }
    }

    return entries.size();
}

int ClsReadOptimizedClient::update_bloomfilter(bufferlist in, int level)
{
    std::map<std::string, bufferlist> tgt_objects;
    auto it = in.cbegin();
    try {
        decode(tgt_objects, it);
    } catch (buffer::error &err) {
        std::cout << "in update_bloomfilter: failed to decode target objects" << err.what() << endl;
        return -EIO;
    }

    for (auto tgt_object : tgt_objects) {
        auto itt = tgt_object.second.cbegin();

        std::vector<cls_lsm_entry> new_entries;
        try {
            decode(new_entries, itt);
        } catch (const ceph::buffer::error& err) {
            std::cout << "ERROR: update_bloomfilter: failed to decode entries" << endl;
            return -EINVAL;
        }

        int key_group = get_key_range_from_object_id(tgt_object.first);
        for (auto new_entry : new_entries) {
            lsm_bloomfilter_insert(bloomfilter_store[level][key_group], to_string(new_entry.key));
        }
    }

    return 0;
}


