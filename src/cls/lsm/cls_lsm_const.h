#ifndef CEPH_CLS_LSM_CONSTS_H
#define CEPH_CLS_LSM_CONSTS_H

#define LSM_CLASS "lsm"
#define LSM_INIT  "lsm_init"
#define LSM_WRITE_NODE "lsm_write_node"
#define LSM_READ_KEY "lsm_read_key"
#define LSM_READ_ALL "lsm_read_all"
#define LSM_READ_FROM_INTERNAL_NODES "lsm_read_from_internal_nodes"
#define LSM_COMPACT_ENTRIES_TO_TARGETS "lsm_compact_entries_to_targets"
#define LSM_REMOTE_READ_NODE "lsm_remote_read_node"
#define LSM_GET_REMOTE_READ_IDS "lsm_get_remote_read_ids"
#define LSM_CREATE_OBJECT "lsm_create_object"
#define LSM_PREPARE_COMPACTION "lsm_prepare_compaction"
#define LSM_COMPACT "lsm_compact"
#define LSM_SORT "lsm_sort"
#define LSM_UPDATE_POST_COMPACTION "lsm_update_post_compaction"
#define LSM_PREPARE_GATHERING "lsm_prepare_gathering"
#define LSM_GATHER "lsm_gather"

#define LSM_LEVEL_0_CAPACITY 1000
#define LSM_LEVEL_OBJECT_CAPACITY 4

#endif
