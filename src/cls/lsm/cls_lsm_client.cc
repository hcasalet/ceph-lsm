#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_const.h"
#include "cls/lsm/cls_lsm_client.h"

using namespace librados;

void cls_lsm_init(librados::ObjectWriteOperation& op,
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

    std::cout << "reading key: " << op.keys[0] << endl;
    int r = io_ctx.exec(oid, LSM_CLASS, LSM_READ_NODE, in, out);
    if (r < 0) {
        return r;
    }
    std::cout << "reading finished with length: %u" << out.length() << endl;  
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

void cls_lsm_write(librados::ObjectWriteOperation& op,
                   const std::string& oid,
                   std::vector<cls_lsm_entry>& entries)
{
    bufferlist in, out;
    cls_lsm_append_entries_op call;
    call.tree_name = oid;
    call.entries = std::move(entries);
    encode(call, in);
    op.exec(LSM_CLASS, LSM_WRITE_NODE, in);
}

int cls_lsm_compact(librados::IoCtx& io_ctx, const std::string& oid)
{
    bufferlist in, out;

    int r = io_ctx.exec(oid, LSM_CLASS, LSM_PREPARE_COMPACTION, in, out);
    if (r < 0) {
        return r;
    }

    in.claim_append(out);
    out.clear();
    r = io_ctx.exec(oid, LSM_CLASS, LSM_COMPACT, in, out);
    if (r < 0) {
        return r;
    }

    r = io_ctx.exec(oid, LSM_CLASS, LSM_UPDATE_POST_COMPACTION, in, out);
    if (r < 0) {
        return r;
    }
    
    return r;
}

int cls_lsm_gather(librados::IoCtx& io_ctx, const std::string& oid,
                 std::vector<uint64_t>& keys,
                 std::vector<std::string>& columns,
                 std::vector<cls_lsm_entry>& entries)
{
    std::cout << "gathering data " << endl;

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
        std::cout << "in cls_lsm_gather : failed decoding cls_lsm_get_entries_ret " << endl;
        return -EIO;
    }
    entries = std::move(ret.entries);

    return entries.size();
}