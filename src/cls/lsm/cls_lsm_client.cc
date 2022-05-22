#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_const.h"
#include "cls/lsm/cls_lsm_bloomfilter.h"
#include "cls/lsm/cls_lsm_client.h"
#include "cls/lsm/cls_lsm_util.h"

using namespace librados;

void ClsLsmClient::InitClient(std::string pool, std::string tree, uint64_t key_low, uint64_t key_high, int splits, int levels,
        std::map<int, std::vector<std::vector<std::string>>>& col_map)
{
    pool_name = pool;
    tree_name = tree;
    key_low_bound = key_low;
    key_high_bound = key_high;
    key_splits = splits;
    levels = levels;
    column_map = col_map;
    int filters = 1;
    for (int i = 0; i <= levels; i++) {
        std::vector<std::vector<bool>> bloomfilters;
        for (int j = 0; j < filters; j++) {
            bloomfilters.push_back(std::vector<bool>(BLOOM_FILTER_STORE_SIZE_64K, false));
        }
        bloomfilter_store.insert(std::make_pair(i, bloomfilters));
        filters *= splits;
    }

    for (int i = 1; i <= levels; i++) {
        level_inventory.insert(std::make_pair(i, 0));
        level_col_grps.insert(std::make_pair(i, (col_map.find(i)->second).size()));
    }
}

void ClsLsmClient::cls_lsm_init(librados::ObjectWriteOperation& op,
                  const std::string& pool_name,
                  const std::string& tree_name,
                  cls_lsm_key_range& key_range)
{
    int level_splits = 0;
    for (int i = 0; i <= levels; i++) {
        if (i == 0) {
            level_splits = 1;
        } else {
            level_splits *= key_range.splits;
        }

        uint64_t low_bound = key_range.low_bound;
        uint64_t increment = (key_range.high_bound - key_range.low_bound) / level_splits;
        uint64_t high_bound = low_bound + increment;

        for (int j = 0; j < level_splits; j++) {

            for (uint64_t k = 0; k < column_map[i].size(); k++) {
                bufferlist in;
                cls_lsm_init_op call;
                call.pool_name = pool_name;
                call.obj_name = tree_name+"/level-"+to_string(i)+"/keyrange-"+to_string(j)+"/columngroup-"+to_string(k);
                call.key_range.low_bound = low_bound;
                call.key_range.high_bound = high_bound;
                call.key_range.splits = level_splits;
                encode(call, in);
                op.exec(LSM_CLASS, LSM_INIT, in);
            }

            low_bound = high_bound;
            high_bound = low_bound + increment;
        }
    }
}

int ClsLsmClient::cls_lsm_read(librados::IoCtx& io_ctx, const std::string& pool_name,
                uint64_t key, const std::vector<std::string> *columns, cls_lsm_entry& entry)
{
    std::vector<std::string> obj_ids;
    for (int i = 0; i <= levels; i++) {
        int key_group = 0;
        if (i > 0) {
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

        r = io_ctx.exec(obj_ids[0], LSM_CLASS, LSM_READ_KEY, in, out);
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

void ClsLsmClient::cls_lsm_write(librados::IoCtx& io_ctx, const std::string& root_name, cls_lsm_entry& entry)
{
    if (in_mem_data.size() < LSM_LEVEL_0_CAPACITY) {
        in_mem_data[entry.key] = entry;

        // register data in the bloomfilter stores
        lsm_bloomfilter_insert(bloomfilter_store[0][0], to_string(entry.key));
    } else if (level_inventory.find(1)->second < LSM_LEVEL_OBJECT_CAPACITY) {
        bufferlist in, out;
        for (auto ent : in_mem_data) {
            bufferlist bl_entry;
            encode(ent.second, bl_entry);
            uint16_t entry_size = bl_entry.length();
            encode(entry_size, in);
            in.claim_append(bl_entry);
        }
        std::string oid = tree_name + "/level-1/colgrp-0/member-" + to_string(level_inventory.find(1)->second);
        io_ctx.exec(oid, LSM_CLASS, LSM_WRITE_NODE, in, out);
        lsm_bloomfilter_clear(bloomfilter_store[1][level_inventory.find(1)->second]);
        lsm_bloomfilter_copy(bloomfilter_store[1][level_inventory.find(1)->second], bloomfilter_store[0][0]);
        lsm_bloomfilter_clear(bloomfilter_store[0][0]);

        in_mem_data.clear();
        level_inventory[1] = level_inventory[1] + 1;
    } else {
        std::vector<cls_lsm_entry> ins;
        for (auto ent : in_mem_data) {
            ins.push_back(ent.second);
        }
        ClsLsmClient::cls_lsm_compact(io_ctx, ins);

        in_mem_data.clear();
    }
}

int ClsLsmClient::cls_lsm_compact(librados::IoCtx& io_ctx, std::vector<cls_lsm_entry>& input)
{
    int level = 2;
    std::vector<bufferlist> newins;

    std::vector<std::vector<cls_lsm_entry> > ins;
    ins.push_back(input);

    bufferlist out;

    while (level <= levels) {
        newins.clear();

        if (level == 1) {
            for (auto item : input) {
                bufferlist bl_item;
                encode(item, bl_item);

            }
        }
        ClsLsmClient::crack(ins, level_col_grps.find(level)->second, newins);

        if (level == levels || level_inventory.find(level)->second < LSM_LEVEL_OBJECT_CAPACITY) {

            for (int group = 0; group < level_col_grps.find(level)->second; group++) {
                bufferlist out;
                std::string oid = tree_name + "/level-" + to_string(level) + "/colgrp-" + to_string(group) + "/member-" + to_string(level_inventory.find(level)->second);
                io_ctx.exec(oid, LSM_CLASS, LSM_WRITE_NODE, newins[group], out);
            }

            lsm_bloomfilter_compact(bloomfilter_store[level-1], bloomfilter_store[level][level_inventory.find(level)->second]);
            lsm_bloomfilter_clearall(bloomfilter_store[level-1]);
            level_inventory[level] = level_inventory[level] + 1;

            break;

        } else {
            ins.clear();

            for (int group = 0; group < level_col_grps.find(level)->second; group++) {
                bufferlist in, out;

                encode(pool_name, in);
                encode(tree_name, in);
                encode(level, in);
                encode(level_col_grps.find(level)->second, in);
                encode(newins[group], in);

                std::string oid = tree_name + "/level-" + to_string(level-1) + "/colgrp-" + to_string(group) +"/member-0";
                io_ctx.exec(oid, LSM_CLASS, LSM_SORT, in, out);

                // Potential optimization: out was serialized in LSM_SORT, and de-serialized in 
                // the following get_entry_groups
                std::vector<cls_lsm_entry> sorted;
                ClsLsmClient::get_entry_groups(out, sorted);

                ins.push_back(sorted);
            }

            level_inventory[level] = 0;
            level += 1;
        }
    }
    
    return 0;
}

int ClsLsmClient::cls_lsm_scan(librados::IoCtx& io_ctx,
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

int ClsLsmClient::update_bloomfilter(bufferlist in, int level)
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

void ClsLsmClient::crack(std::vector<std::vector<cls_lsm_entry> >& entry_groups, int groups, std::vector<bufferlist>& newins)
{
    newins.clear();

    for (uint64_t i = 0; i < entry_groups.size(); i++) {

        std::vector<bufferlist> ins;
        for (int j = 0; j < groups; j++) {
            bufferlist bl;
            ins.push_back(bl);
        }
        
        for (auto entry : entry_groups[i]) {
            std::vector<cls_lsm_entry> split_entries;
            for (int j = 0; j < groups; j++) {
                cls_lsm_entry split_entry;
                split_entry.key = entry.key;
                split_entries.push_back(split_entry);
            }

            int i = 0;
            for (auto item : entry.value) {
                split_entries[i%groups].value.insert(std::pair<std::string, bufferlist>(item.first, item.second));
                i += 1;
            }

            std::vector<bufferlist> bl_entries;
            for (int j = 0; j < groups; j++) {
                bufferlist bl_entry;
                encode(split_entries[j], bl_entry);

                uint64_t entry_size = bl_entry.length();
                encode(entry_size, ins[j]);
                ins[j].claim_append(bl_entry);
            }
        }

        newins.insert(newins.end(), ins.begin(), ins.end());
    }
}

int ClsLsmClient::get_entry_groups(bufferlist& in, std::vector<cls_lsm_entry>& entries)
{
    auto it = in.cbegin();
    uint64_t size_to_process = in.length();

    do {
        uint64_t data_size = 0;
        try {
            decode(data_size, it);
        } catch (const ceph::buffer::error& err) {
            return -EINVAL;
        }
        size_to_process -= sizeof(uint64_t);

        if (data_size > size_to_process) {
            break;
        }

        cls_lsm_entry entry;
        try {
            decode(entry, it);
        } catch (const ceph::buffer::error& err) {
            return -EINVAL;
        }
        size_to_process -= data_size;

        entries.push_back(entry);

    } while (size_to_process > 0);

    return 0;
}
