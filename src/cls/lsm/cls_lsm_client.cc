#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_const.h"
#include "cls/lsm/cls_lsm_client.h"

using namespace librados;

void cls_lsm_init(librados::ObjectWriteOperation& op,
                const std::string& pool_name,
                const std::string& lsm_tree_name,
                uint64_t levels,
                uint64_t[] key_range,
                uint64_t max_capacity,
                std::set<std::string>& columns)
{
    bufferlist in;
    cls_lsm_init_op call;
    call.pool = pool_name;
    call.app_name = lsm_tree_name;
    call.levels = levels;
    call.key_range.low_bound = key_range[0];
    call.key_range.high_bound = key_range[1];
    call.max_capacity = max_capacity;
    call.all_columns = columns;
    encode(call, in);
    op.exec(LSM_CLASS, LSM_INIT, in);
}

int cls_lsm_read(IoCtx& io_ctx, const std::string& oid,
                std::vector<cls_lsm_entry>& entries,
                std::vector<uint64_t>& keys,
                std::vector<std::string>& columns)
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

    return 0;
}

void cls_lsm_write(ObjectWriteOperation& op,
                std::vector<bufferlist> bl_data_vec)
{
    bufferlist in;
    cls_lsm_append_entries_op call;
    call.bl_data_vec = std::move(bl_data_vec);
    encode(call, in);
    op.exec(LSM_CLASS, LSM_WRITE_NODE, in);
}