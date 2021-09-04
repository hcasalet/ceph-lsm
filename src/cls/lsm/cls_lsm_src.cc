#include "include/types.h"

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_bloomfilter.h"
#include "cls/lsm/cls_lsm_src.h"

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

// should we define the max size in cls_lsm_node?
int lsm_write_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head)
{
    bufferlist bl;
    uint16_t node_start = LSM_NODE_START;
    encode(node_start, bl);

    bufferlist bl_head;
    encode(node_head, bl_head);

    uint64_t encoded_len = bl_head.length();
    encode(encoded_len, bl);
    
    bl.claim_append(bl_head);

    int ret = cls_cxx_write2(hctx, 0, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: lsm_write_node: failed to write lsm node");
        return ret;
    }

    return 0;
}

int lsm_read_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head)
{
    uint64_t read_size = CHUNK_SIZE, start_offset = 0;

    bufferlist bl_node;
    const auto ret = cls_cxx_read(hctx, start_offset, read_size, &bl_node);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: lsm_read_node: failed read lsm node");
        return ret;
    }
    if (ret == 0) {
        CLS_LOG(20, "INFO: lsm_read_node: empty node, not initialized yet");
        return -EINVAL;
    }

    // Process the chunk of data read
    auto it = bl_node.cbegin();
    
    // Check node start
    uint16_t node_start;
    try {
        decode(node_start, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode node start");
        return -EINVAL;
    }
    if (node_start != LSM_NODE_START) {
        CLS_LOG(0, "ERROR: lsm_read_node: invalid node start");
        return -EINVAL;
    }

    uint64_t encoded_len;
    try {
        decode(encoded_len, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode encoded size: %s", err.what());
        return -EINVAL;
    }

    if (encoded_len > (read_size - LSM_NODE_OVERHEAD)) {
        start_offset = read_size;
        read_size = encoded_len - (read_size - LSM_NODE_OVERHEAD);
        bufferlist bl_remaining_node;
        const auto ret = cls_cxx_read2(hctx, start_offset, read_size, &bl_remaining_node, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
            CLS_LOG(5, "ERROR: lsm_read_node: failed to read the remaining part of the node");
            return ret;
        }
        bl_node.claim_append(bl_remaining_node);
    }

    try {
        decode(node_head, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode node: %s", err.what());
        return -EINVAL;
    }

    return 0;
}

int lsm_init(cls_method_context_t hctx, const cls_lsm_init_op& op)
{
    // check if node was already initialized
    cls_lsm_node_head head;
    int ret = lsm_read_node_head(hctx, head);
    if (ret == 0) {
        return -EEXIST;
    }

    // bail out for other errors
    if (ret < 0 && ret != -EINVAL) {
        return ret;
    }

    head.level = op.level;
    head.key_range = op.key_range;
    head.max_capacity = op.max_capacity;
    head.size = op.size;
    head.entry_start_offset = op.entry_start_offset;
    head.entry_end_offset = op.entry_end_offset;
    head.naming_map = op.naming_map;

    // get the length of head to initialize start_offset and end_offset
    bufferlist bl_head;
    encode(head, bl_head);
    uint64_t head_len = bl_head.length();

    head.entry_start_offset = head_len + LSM_NODE_OVERHEAD * 4 + LSM_PER_KEY_OVERHEAD * head.max_capacity;
    head.entry_end_offset = head.entry_start_offset;

    CLS_LOG(20, "INFO: lsm_init level %u", head.level);
    CLS_LOG(20, "INFO: lsm_init key range (%lu, %lu)", head.key_range.low_bound, head.key_range.high_bound);

    return lsm_write_node_head(hctx, head);
}

int lsm_append_entries(cls_method_context_t hctx, cls_lsm_append_entries_op& op, cls_lsm_node_head& head)
{
    // capacity checking
    if (head.size + op.bl_data_vec.size() > head.max_capacity) {
        CLS_LOG(0, "ERROR: No space left in the node, need compaction");
        return -ENOSPC;
    }

    for (auto& bl_data : op.bl_data_vec) {
        bufferlist bl;
        uint16_t entry_start = LSM_ENTRY_START;
        encode(entry_start, bl);
        bufferlist bl_data_entry;
        encode(bl_data, bl_data_entry);
        uint64_t data_size = bl_data_entry.length();
        encode(data_size, bl);
        bl.claim_append(bl_data_entry);

        CLS_LOG(10, "INFO: lsm_append_entries: total entry size to be written is %u and data size is %lu", bl.length(), data_size);

        auto ret = cls_cxx_write2(hctx, head.entry_end_offset, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
            return ret;
        }
        cls_lsm_marker marker{head.entry_end_offset, bl.length()};
        head.entry_map.insert(std::pair<uint64_t, cls_lsm_marker>(bl_data.key, marker));
        head.entry_end_offset += bl.length();
        head.size++; 
    }

    return lsm_write_node_head(hctx, head);
}

int lsm_get_entries(cls_method_context_t hctx, cls_lsm_node_head& head, cls_lsm_get_entries_ret& op_ret)
{
    if (head.entry_start_offset == head.entry_end_offset) {
        CLS_LOG(20, "INFO: lsm_get_entries: node is empty, offset is %lu", head.entry_end_offset);
        return 0;
    }

    bufferlist bl_chunk;
    auto ret = cls_cxx_read(hctx, head.entry_start_offset, (head.entry_end_offset - head.entry_start_offset), &bl_chunk);
    if (ret < 0) {
        return ret;
    }

    uint16_t entry_start = 0;
    uint64_t data_size = 0;
    uint64_t size_to_process = bl_chunk.length();
    unsigned index = 0;
    auto it = bl_chunk.cbegin();

    do {
        cls_lsm_entry entry;
        try {
            decode(entry_start, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_get_entries: failed to decode entry start: %s", err.what());
            return -EINVAL;
        }

        if (entry_start != LSM_ENTRY_START) {
            CLS_LOG(5, "ERROR: lsm_get_entries: invalid entry start %u", entry_start);
            return -EINVAL;
        }
        index += sizeof(uint16_t);
        size_to_process -= sizeof(uint16_t);

        try {
            decode(data_size, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_get_entries: failed to decode data size: %s", err.what());
            return -EINVAL;
        }
        CLS_LOG(10, "INFO: lsm_get_entries: data size %lu", data_size);
        index += sizeof(uint64_t);
        size_to_process -= sizeof(uint64_t);

        if (data_size > size_to_process) {
            CLS_LOG(10, "INFO: lsm_get_entries: not enough data to read, breaking the loop...");
            break;
        }

        try {
            decode(entry, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_retrieve_data: failed to decode entry %s", err.what());
            return -EINVAL;
        }

        op_ret.entries.emplace_back(entry);

    } while (index < bl_chunk.length());
    
    return 0;
}

int lsm_compact_node(cls_method_context_t hctx, cls_lsm_append_entries_op& op, cls_lsm_node_head& head)
{
    auto ret = cls_cxx_scatter_wait_for_completions(hctx);
    if (ret == -EAGAIN) {
        std::map<std::string, cls_lsm_append_entries_op> tgt_objs;
        std::vector<cls_lsm_entry> all_src;
        all_src.reserve(op.bl_data_vec.size() + head.size);
        cls_lsm_get_entries_ret entries_ret;
        ret = lsm_get_entries(hctx, head, entries_ret);
        all_src.insert(all_src.end(), op.bl_data_vec.begin(), op.bl_data_vec.end());
        all_src.insert(all_src.end(), entries_ret.entries.begin(), entries_ret.entries.end());

        uint64_t key_lb = head.key_range.low_bound;
        uint64_t key_hb = head.key_range.high_bound;
        uint64_t key_increment = (key_hb - key_lb)/head.naming_map.key_ranges + 1;
        uint64_t cg_size = head.naming_map.clm_groups.size();

        // initialize target objects with the keys
        for (uint32_t i = 0; i < head.naming_map.key_ranges; i++) {
            std::stringstream ss;
            ss << "/lv-" << (head.level + 1) << "/kr-" << i;
            for (uint32_t j = 0; j < cg_size; j++) {
                ss << "/cg-" << j;
                tgt_objs.insert(std::pair<std::string, cls_lsm_append_entries_op>(ss.str(), cls_lsm_append_entries_op()));
            }
        }

        // populate target objects with the values
        for (auto src : all_src) {
            uint64_t lowbound = key_lb;
            for (uint32_t i = 0; i < head.naming_map.key_ranges; i++) {
                if (src.key >= lowbound && src.key < lowbound + key_increment) {
                    std::vector<cls_lsm_entry> split_data;
                    split_data.reserve(cg_size);

                    // every split group has the same key
                    for (uint32_t j = 0; j < cg_size; j++) {
                        split_data[j].key = src.key;
                    }

                    // split value ("columns")
                    for (auto it = src.value.begin(); it != src.value.end(); it++) {
                        for (uint32_t j = 0; j < cg_size; j++) {
                            if (head.naming_map.clm_groups[j].columns.find(it->first) != head.naming_map.clm_groups[j].columns.end()) {
                                split_data[j].value.insert(std::pair<std::string, ceph::buffer::list>(it->first, it->second));
                            }
                        }
                    }

                    // place the groups in target objects
                    for (uint32_t j = 0; j < cg_size; j++) {
                        std::stringstream ss;
                        ss << "/lv-" << (head.level + 1) << "/kr-" << i << "/cg-" << j;
                        tgt_objs[ss.str()].bl_data_vec.push_back(split_data[j]);
                    }
                    break;
                }
                lowbound += key_increment;
            }
        }

        // remote write to child objects
        bufferlist *in; 
        ret = cls_cxx_scatter(hctx, tgt_objs, head.pool, "cls_lsm", "cls_lsm_write_node", *in);
    } else {
        if (ret != 0) {
            CLS_ERR("%s: compaction failed. error=%d", __PRETTY_FUNCTION__, ret);
        }
    }

    return 0; 
}

int lsm_get_child_object_names(cls_method_context_t hctx, cls_lsm_get_child_object_names_ret& op_ret)
{
    // get the node
    cls_lsm_node_head node;
    int ret = lsm_read_node_head(hctx, node);
    if (ret < 0) {
        return ret;
    }
    
    std::vector<std::string> children_objects;
    for (uint32_t i = 0; i < node.naming_map.key_ranges; i++) {
        for (uint32_t j = 0; j < node.naming_map.clm_groups.size(); j++) {
            std::stringstream ss;
            ss << "/lv" << std::to_string(node.level+1) << "-kr" << std::to_string(i) << "-cg" << std::to_string(j);
            children_objects.push_back(ss.str());
        }
    }

    op_ret.child_object_names = children_objects;

    CLS_LOG(20, "INFO: lsm_get_child_object_names: size of child objects: %lu", op_ret.child_object_names.size());
    
    return 0;
}