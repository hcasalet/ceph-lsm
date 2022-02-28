#ifndef CEPH_CLS_LSM_UTIL_H
#define CEPH_CLS_LSM_UTIL_H

#include "include/types.h"
#include "cls/lsm/cls_lsm_types.h"

int get_key_group(uint64_t low, uint64_t high, int splits, int level, uint64_t key);
std::vector<int> get_col_group(std::vector<std::string> cols, int level, std::map<int, std::vector<std::vector<std::string>>>& col_map);
bool intersects(std::vector<std::string>& vec1s, std::vector<std::string>& vec2s);
std::string construct_object_id(std::string tree_name, int level, int key_group, int col_group);
std::string get_tree_name_from_object_id(std::string object_id);
int get_key_range_from_object_id(std::string object_id);
int get_level_from_object_id(std::string object_id);
void split_column_groups_for_entries(std::vector<cls_lsm_entry>& entries,
                            std::vector<std::vector<std::string>>& column_group_list,
                            std::vector<std::vector<cls_lsm_entry>>& split_entries);

#endif