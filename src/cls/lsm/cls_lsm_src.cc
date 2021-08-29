#include "include/types.h"

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_src.h"

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

// should we define the max size in cls_lsm_node?
int lsm_write_node(cls_method_context_t hctx, cls_lsm_node& node)
{
    bufferlist bl;
    uint16_t entry_start = LSM_NODE_START;
    encode(entry_start, bl);

    bufferlist bl_node;
    encode(node, bl_node);

    uint64_t encoded_len = bl_node.length();
    encode(encoded_len, bl);
    
    bl.claim_append(bl_node);

    int ret = cls_cxx_write2(hctx, 0, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: lsm_write_node: failed to write lsm node");
        return ret;
    }

    return 0;
}

int lsm_read_node(cls_method_context_t hctx, cls_lsm_node& node)
{
    uint64_t chunk_size = 1024, start_offset = 0;

    bufferlist bl_node;
    const auto ret = cls_cxx_read(hctx, start_offset, chunk_size, &bl_node);
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
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode node start: %s", err.what());
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
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode encoded size", err.what());
        return -EINVAL;
    }

    if (encoded_len > (chunk_size - LSM_ENTRY_OVERHEAD)) {
        start_offset = chunk_size;
        chunk_size = encoded_len - (chunk_size - LSM_ENTRY_OVERHEAD);
        bufferlist bl_remaining_node;
        const auto ret = cls_cxx_read2(hctx, start_offset, chunk_size, &bl_remaining_node, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
            CLS_LOG(5, "ERROR: lsm_read_node: failed to read the remaining part of the node");
            return ret;
        }
        bl_node.claim_append(bl_remaining_node);
    }

    try {
        decode(node, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode node: %s", err.what());
        return -EINVAL;
    }

    return 0;
}

int lsm_init(cls_method_context_t hctx, const cls_lsm_init_op& op)
{
    cls_lsm_node_head node_head;
    int ret = lsm_read_node(hctx, node_head);

    // if node has already been initialized, return
    if (ret == 0) {
        return -EEXIST;
    }

    // bail out for other errors
    if (ret < 0 && ret != -EINVAL) {
        return ret;
    }

    node.object_name = op.object_name;
    node.level = op.level;
    node.key_range = op.key_range;
    node.naming = op.naming_scheme;
    node.max_bloomfilter_data_size = op.max_bloomfilter_data_size;
    node.bl_bloomfilter_data = op.bl_bloomfilter_data;

    CLS_LOG(20, "INFO: lsm_init object name %s", node.object_name);
    CLS_LOG(20, "INFO: lsm_init level %u", node.level);
    CLS_LOG(20, "INFO: lsm_init key range (%lu, %lu)", node.key_range.low_bound, node.key_range.high_bound);
    CLS_LOG(20, "INFO: lsm_init max bloom filter size %lu", node.max_bloomfilter_data_size);

    return lsm_write_node(hctx, node);
}

int lsm_get_child_object_names(cls_method_context_t hctx, cls_lsm_get_child_object_names_ret& op_ret)
{
    // get the node
    cls_lsm_node node;
    int ret = lsm_read_node(hctx, node);
    if (ret < 0) {
        return ret;
    }
    
    std::vector<std::string> children_objects;
    for (int i = 0; i < node.naming_scheme.key_range_pieces; i++) {
        for (int j = 0; j < node.naming_scheme.clm_group_pieces.size(); j++) {
            std::stringstream ss;
            ss << node.object_name << "/lv" << node.level+1 << "-kr" << std::to_string(i) << "-cg" << std::to_string(j);
            children_objects.push_back(ss.str())
        }
    }

    opt_ret.child_object_names = children_objects;

    CLS_LOG(20, "INFO: lsm_get_child_object_names: size of child objects: %lu", opt_ret.child_object_names.size());
    
    return 0;
}

int lsm_persist_data(cls_method_context_t hctx, cls_lsm_persist_data_op& op, cls_lsm_node& node)
{
    // add space checking

    for (auto& bl_data : op.bl_data_vec) {
        bufferlist bl;
        uint16_t entry_start = LSM_ENTRY_START;
        encode(entry_start, bl);
        uint64_t data_size = bl_data.length();
        encode(data_size, bl);
        bl.claim_append(bl_data);

        CLS_LOG(10, "INFO: lsm_persist_data: total size to be written is %u and data size is %lu", bl.length(), data_size);

        auto ret = cls_cxx_write2(hctx, node.data_end_offset, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
            return ret;
        }
        node.data_end_offset += bl.length();
    }

    return 0;
}

int lsm_retrieve_data(cls_method_context_t hctx, cls_lsm_retrieve_data_ret& op_ret, cls_lsm_node& node)
{
    if (node.data_start_offset == node.data_end_offset) {
        CLS_LOG(20, "INFO: lsm_retrieve_data: node is empty, offset is %u", node.data_end_offset);
        return 0;
    }

    bufferlist bl_chunk;
    auto ret = cls_cxx_read(hctx, node.data_start_offset, (node.data_end_offset - node.data_start_offset), &bl_chunk);
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
            CLS_LOG(10, "ERROR: lsm_retrieve_data: failed to decode entry start: %s", err.what());
            return -EINVAL;
        }

        if (entry_start != LSM_ENTRY_START) {
            CLS_LOG(5, "ERROR: lsm_retrieve_data: invalid entry start %u", entry_start);
            return -EINVAL;
        }
        index += sizeof(uint16_t);
        size_to_process -= sizeof(uint16_t);

        try {
            decode(data_size, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_retrieve_data: failed to decode data size: %s", err.what());
            retrun -EINVAL;
        }
        CLS_LOG(10, "INFO: lsm_retrieve data: data size %lu", data_size);
        index += sizeof(uint64_t);
        size_to_process -= sizeof(uint64_t);

        if (data_size > size_to_process) {
            CLS_LOG(10, "INFO: lsm_retrieve data: not enough data to read, breaking the loop...");
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

int lsm_compaction(cls_method_context_t hctx, cls_lsm_persist_data_op& op, 
                    cls_lsm_retrieve_data_ret& op_src, cls_lsm_node& node, 
                    cls_lsm_split_data_ret& op_ret)
{
    std::vector<cls_lsm_entry> all_src;
    all_src.reserve(op.size() + op_src.size());
    all_src.insert(all_src.end(), op.begin(), op.end());
    all_src.insert(all_src.end(), op_src.begin(), op_src.end());

    uint64_t key_lb = node.key_range.low_bound;
    uint64_t key_hb = node.key_range.high_bound;
    uint64_t increment = (key_hb - key_lb)/node.naming_scheme.key_range_pieces + 1;

    uint64_t cg_size = node.naming_scheme.clm_group_pieces.size();

    std::vector<cls_lsm_entry> tgt_objs[cg_size*node.naming_scheme.key_range_pieces];
    for (auto src : all_src) {
        uint64_t lowbound = key_lb;
        for (int i = 0; i < node.naming_scheme.key_range_pieces; i++) {
            if (src.key >= lowbound && src.key < lowbound + increment) {
                std::map<string, ceph::buffer::list> split_columns[cg_size];
                for (auto it = src.value.begin(); it != src.value.end(); it++) {
                    for (int j = 0; j < cg_size; j++) {
                        if (node.naming_scheme.clm_group_pieces[j].get(it->first)) {
                            split_columns[j].insert(it->first, it->second);
                        }
                    }
                }

                fpr (int j = 0; j < cg_size; j++) {
                    tgt_objs[cg_size*i+j] = split_columns[j];
                }
                break;
            }
            lowbound += increment;
        }
    }

    op_ret.split_entries.insert(op_ret.split_entries.end(), &tgt_objs[0], &tgt_objs[cg_size*node.naming_scheme.key_range_pieces]);

    return 0; 
}
