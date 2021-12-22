#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_const.h"
#include "cls/lsm/cls_lsm_client.h"

using namespace librados;

void cls_lsm_init(librados::ObjectWriteOperation& op,
                const std::string& pool_name,
                const std::string& tree_name,
                uint64_t levels,
                std::vector<uint64_t>& key_range,
                uint64_t fan_out, 
                uint64_t max_capacity,
                std::vector<std::set<std::string>>& columns)
{
    bufferlist in;
    cls_lsm_init_op call;
    call.pool_name = pool_name;
    call.tree_name = tree_name;
    call.levels = levels;
    call.key_range.low_bound = key_range[0];
    call.key_range.high_bound = key_range[1];
    call.key_range.splits = key_range[2];
    call.fan_out = fan_out;
    call.max_capacity = max_capacity;
    call.all_column_splits = columns;
    encode(call, in);
    op.exec(LSM_CLASS, LSM_INIT, in);
}

int cls_lsm_read(librados::IoCtx& io_ctx, const std::string& oid,
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
    if (r < 0)
        return r;

    cls_lsm_get_entries_ret ret;
    auto iter = out.cbegin();
    try {
        decode(ret, iter);
    } catch (buffer::error& err) {
        return -EIO;
    }
    entries = std::move(ret.entries);

    std::map<std::string, std::vector<uint64_t>> src_objs_map;
    try {
        decode(src_objs_map, iter);
    } catch (buffer::error& err) {
        return -EIO;
    }

    while (src_objs_map.size() > 0) {
        in.clear();
        out.clear();
        encode(src_objs_map, in);
        encode(op.columns, in);

        r = io_ctx.exec(oid, LSM_CLASS, LSM_READ_FROM_INTERNAL_NODES, in, out);
        if (r < 0) 
            return r;

        auto iter_int = out.cbegin();

        cls_lsm_get_entries_ret_int ret_int;
        try {
            decode(ret_int, iter_int);
        } catch (buffer::error& err) {
            return -EIO;
        }
        entries.insert(entries.end(), ret_int.entries.begin(), ret_int.entries.end());

        src_objs_map.clear();
        try {
            decode(src_objs_map, iter_int);
        } catch (buffer::error& err) {
            return -EIO;
        }
    }
    
    return 0;
}

void cls_lsm_write(librados::ObjectWriteOperation& op,
                const std::string& oid,
                std::vector<cls_lsm_entry> entries)
{
    bufferlist in;
    cls_lsm_append_entries_op call;
    call.tree_name = oid;
    call.entries = std::move(entries);
    encode(call, in);
    op.exec(LSM_CLASS, LSM_WRITE_NODE, in);
}