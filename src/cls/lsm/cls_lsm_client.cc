#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_const.h"
#include "cls/lsm/cls_lsm_bloomfilter.h"
#include "cls/lsm/cls_lsm_client.h"

using namespace librados;

void ClsLsmClient::cls_lsm_init(librados::ObjectWriteOperation& op,
                  const std::string& pool_name,
                  const std::string& tree_name,
                  uint64_t levels,
                  cls_lsm_key_range& key_range,
                  uint64_t capacity,
                  std::vector<std::set<std::string>>& columns)
{
    bufferlist in;
    cls_lsm_init_op call;
    call.pool_name = pool_name;
    call.tree_name = tree_name;
    call.levels = levels;
    call.key_range = key_range;
    call.capacity = capacity;
    call.column_group_splits = columns;
    encode(call, in);
    op.exec(LSM_CLASS, LSM_INIT, in);
}

int ClsLsmClient::cls_lsm_read(librados::IoCtx& io_ctx, const std::string& oid,
                 std::vector<uint64_t>& keys,
                 std::vector<std::string>& columns,
                 std::vector<cls_lsm_entry>& entries)
{
    bufferlist in, out;
    cls_lsm_get_entries_op op;
    op.keys = keys;
    op.columns = columns;
    encode(op, in);

    int r = io_ctx.exec(oid, LSM_CLASS, LSM_READ_NODE, in, out);
    if (r < 0) {
        return r;
    }

    cls_lsm_get_entries_ret ret;
    auto iter = out.cbegin();
    try {
        decode(ret, iter);
    } catch (buffer::error &err)
    {
        std::cout << "in cls_lsm_read : decoding cls_lsm_get_entries_ret - " << err.what() << endl;
        return -EIO;
    }
    entries = std::move(ret.entries);

    return entries.size();
}

void ClsLsmClient::cls_lsm_write(librados::ObjectWriteOperation& op,
                   const std::string& oid,
                   std::vector<cls_lsm_entry>& entries)
{
    bufferlist in, out;
    cls_lsm_append_entries_op call;
    call.tree_name = oid;
    call.entries = std::move(entries);
    encode(call, in);
    op.exec(LSM_CLASS, LSM_WRITE_NODE, in);

    // register data in the bloomfilter stores
    for (auto entry : entries) {
        lsm_bloomfilter_insert(bloomfilter_store[oid+"_all"], to_string(entry.key));
        lsm_bloomfilter_insert(bloomfilter_store[oid], to_string(entry.key));   
    }
}

int ClsLsmClient::cls_lsm_compact(librados::IoCtx& io_ctx, const std::string& oid)
{
    bufferlist in, out, in2;

    // Get ready to call scatter
    int r = io_ctx.exec(oid, LSM_CLASS, LSM_PREPARE_COMPACTION, in, out);
    if (r < 0) {
        return r;
    }

    // get the result
    in2 = out;
    in.claim_append(out);

    // update the bloomfilters for the child objects to be compacted to
    ClsLsmClient::update_bloomfilter(in2);

    // call scatter to compact
    r = io_ctx.exec(oid, LSM_CLASS, LSM_COMPACT, in, out);
    if (r < 0) {
        return r;
    }

    // clear all data out of the compacted object
    r = io_ctx.exec(oid, LSM_CLASS, LSM_UPDATE_POST_COMPACTION, in, out);
    if (r < 0) {
        return r;
    }

    // clear bloomfilter for the compacted object
    lsm_bloomfilter_clear(bloomfilter_store[oid]);
    
    return 0;
}

int ClsLsmClient::cls_lsm_gather(librados::IoCtx& io_ctx, const std::string& oid,
                 std::vector<uint64_t>& keys,
                 std::vector<std::string>& columns,
                 std::vector<cls_lsm_entry>& entries)
{
    bufferlist in, out;
    cls_lsm_get_entries_op op;
    op.keys = keys;
    op.columns = columns;
    encode(op, in);

    int r = io_ctx.exec(oid, LSM_CLASS, LSM_PREPARE_GATHERING, in, out);
    if (r < 0) {
        return r;
    }

    in.clear();
    in.claim_append(out);
    out.clear();
    r = io_ctx.exec(oid, LSM_CLASS, LSM_GATHER, in, out);
    if (r < 0) {
        return r;
    }

    cls_lsm_get_entries_ret ret;
    auto iter = out.cbegin();
    try {
        decode(ret, iter);
    } catch (buffer::error &err) {
        std::cout << "in cls_lsm_gather: failed decoding cls_lsm_get_entries_ret" << endl;
        return -EIO;
    }
    entries = std::move(ret.entries);

    return entries.size();
}

int ClsLsmClient::update_bloomfilter(bufferlist in)
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

        bool is_root;
        cls_lsm_node_head new_head;
        std::vector<cls_lsm_entry> new_entries;
        try {
            decode(is_root, itt);
            decode(new_head, itt);
            decode(new_entries, itt);
        } catch (const ceph::buffer::error& err) {
            std::cout << "ERROR: update_bloomfilter: failed to decode entries" << endl;
            return -EINVAL;
        }

        for (auto new_entry : new_entries) {
            lsm_bloomfilter_insert(bloomfilter_store[tgt_object.first], to_string(new_entry.key));
        }
    }

    return 0;
}