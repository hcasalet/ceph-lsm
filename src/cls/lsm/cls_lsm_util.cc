#include "cls/lsm/cls_lsm_util.h"

int get_key_group(uint64_t low, uint64_t high, int splits, int level, uint64_t key)
{
    if (key < low || key > high) {
        return -1; 
    }

    if (level == 0) {
        return 0; 
    }

    int cand_high = splits * level;
    int cand_low  = 0;

    uint64_t middle = (high + low) / 2;
    while (cand_low < cand_high - 1) {
        if (key < middle) {
            cand_high = (cand_high + cand_low)/ 2;
            high = middle;
        } else {
            cand_low = (cand_high + cand_low)/ 2;
            low = middle;
        }
        middle = (high + low) / 2;
    }

    return cand_low;
}

std::vector<int> get_col_group(std::vector<std::string> cols, int level, std::map<int,
                            std::vector<std::vector<std::string>>>& col_map)
{
    std::vector<int> column_groups;
    std::vector<std::vector<std::string>> col_grps = col_map[level];
    for (uint64_t i = 0; i < col_grps.size(); i++) {
        if (intersects(col_grps[i], cols)) {
            column_groups.push_back(i);
        }
    }
    return column_groups;
}

bool intersects(std::vector<std::string>& vec1s, std::vector<std::string>& vec2s)
{
    for (auto vec1 : vec1s) {
        for (auto vec2 : vec2s) {
            if (vec1 == vec2) {
                return true;
            }
        }
    }
    return false;
}

std::string construct_object_id(std::string tree_name, int level, int key_group, int col_group)
{
    std::string object_id = tree_name;
    object_id += "/level-"+to_string(level)+"/keyrange-"+to_string(key_group)+"/columngroup-"+to_string(col_group);
    return object_id;
}

std::string get_tree_name_from_object_id(std::string object_id)
{
    std::size_t tree_end = object_id.find("/level-");
    std::string tree_name = object_id.substr(0, tree_end);

    return tree_name;
}

int get_key_range_from_object_id(std::string object_id)
{
    std::size_t key_range_start = object_id.find("/keyrange-");
    std::size_t keyrange_end = object_id.find("/columngroup-");

    std::string keyrange_str = object_id.substr(key_range_start+10, keyrange_end - key_range_start - 10);

    if (keyrange_str.length() > 0) {
        return stoi(keyrange_str);
    } else {
        return 0; 
    }
}

int get_level_from_object_id(std::string object_id)
{
    std::size_t level_start = object_id.find("/level-");
    std::size_t level_end = object_id.find("/keyrange-");

    std::string level_str = object_id.substr(level_start+7, level_end - level_start - 7);

    if (level_str.length() > 0) {
        return stoi(level_str);
    } else {
        return 0;
    }
}

void split_column_groups_for_entries(std::vector<cls_lsm_entry>& entries,
                            std::vector<std::vector<std::string>>& column_group_list,
                            std::vector<std::vector<cls_lsm_entry>>& split_entries)
{
    for (auto entry : entries) {
        int i = 0;
        for (auto column_group : column_group_list) {
            cls_lsm_entry new_entry;
            new_entry.key = entry.key;

            for (auto column : column_group) {
                new_entry.value[column] = entry.value[column];
            }
            
            split_entries[i].push_back(new_entry);

            i++;
        }
    }
}