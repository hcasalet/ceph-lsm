/*
 * This is an example RADOS object class that shows how to use remote operations.
 */

#include "common/ceph_json.h"
#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(test_remote_operations)

cls_handle_t h_class;
cls_method_handle_t h_test_read;
cls_method_handle_t h_test_write;
cls_method_handle_t h_test_scatter;
cls_method_handle_t h_test_gather;

static int test_gather(cls_method_context_t hctx, bufferlist *in, bufferlist *out);

/**
 * read data
 */
static int test_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  JSONParser parser;
  bool b = parser.parse(in->c_str(), in->length());

  auto *o_rec = parser.find_obj("recursive");
  ceph_assert(o_rec);
  std::string recursive = o_rec->get_data_val().str;

  auto *o_pool = parser.find_obj("pool");
  ceph_assert(o_pool);
  std::string pool_name = o_pool->get_data_val().str;

  if (recursive.compare("no") == 0) {
    int r = cls_cxx_read(hctx, 0, 4096, out);
    if (r < 0) {
      CLS_ERR("%s: error reading data", __PRETTY_FUNCTION__);
      return r;
    }
  } else {
      JSONFormatter *formatter = new JSONFormatter(true);
      formatter->open_object_section("foo");
      std::set<std::string> src_objects;
      src_objects.insert("src_object.1");
      src_objects.insert("src_object.2");
      src_objects.insert("src_object.3");
      encode_json("src_objects", src_objects, formatter);
      encode_json("cls", "test_remote_operations", formatter);
      encode_json("method", "test_read", formatter);
      encode_json("pool", pool_name, formatter);
      encode_json("recursive", "no", formatter);
      formatter->close_section();
      in->clear();
      formatter->flush(*in);

      bufferlist out2;
      test_gather(hctx, in, &out2);

      CLS_LOG(1, "winter debug: in test_read recursive section, out2 length = %u", out2.length());
      out->claim_append(out2);
      CLS_LOG(1, "winter debug: in test_read recursive section, out length = %u", out->length());
  }

  return 0;
}

/**
 * write data
 */
static int test_write(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  int r = cls_cxx_write(hctx, 0, in->length(), in);
  if (r < 0) {
    CLS_ERR("%s: error writing data", __PRETTY_FUNCTION__);
    return r;
  }
  return 0;
}

/**
 * scatter data to other objects using remote writes
 */
static int test_scatter(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  CLS_LOG(1, "Holly debug remote writes");
  int r = cls_cxx_scatter_wait_for_completions(hctx);
  if (r == -EAGAIN) {
    // start remote writes
    JSONParser parser;
    bool b = parser.parse(in->c_str(), in->length());
    if (!b) {
      CLS_ERR("%s: failed to parse json", __PRETTY_FUNCTION__);
      return -EBADMSG;
    }
    auto *o_cls = parser.find_obj("cls");
    ceph_assert(o_cls);
    std::string cls = o_cls->get_data_val().str;

    auto *o_method = parser.find_obj("method");
    ceph_assert(o_method);
    std::string method = o_method->get_data_val().str;

    auto *o_pool = parser.find_obj("pool");
    ceph_assert(o_pool);
    std::string pool = o_pool->get_data_val().str;

    bufferlist bl;
    r = cls_cxx_read(hctx, 0, 0, &bl);
    if (r < 0) {
      CLS_ERR("%s: error reading data", __PRETTY_FUNCTION__);
      return r;
    }
    auto *o_tgt_objects = parser.find_obj("tgt_objects");
    ceph_assert(o_tgt_objects);
    auto tgt_objects_v = o_tgt_objects->get_array_elements();
    std::map<std::string, bufferlist> tgt_objects;
    for (auto it = tgt_objects_v.begin(); it != tgt_objects_v.end(); it++) {
      std::string oid_without_double_quotes = it->substr(1, it->size()-2);
      tgt_objects[oid_without_double_quotes] = bl;
    }
    r = cls_cxx_scatter(hctx, tgt_objects, pool, cls.c_str(), method.c_str(), *in);
  } else {
    if (r != 0) {
      CLS_ERR("%s: remote write failed. error=%d", __PRETTY_FUNCTION__, r);
    }
  }
  return r;
}

/**
 * gather data from other objects using remote reads
 */
static int test_gather(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  std::map<std::string, bufferlist> src_obj_buffs;

  hobject_t soid = cls_get_oid(hctx);
  std::string oid = soid.oid.name;

  int r = cls_cxx_get_gathered_data(hctx, &src_obj_buffs);
  if (src_obj_buffs.empty()) {
    // start remote reads
    JSONParser parser;
    bool b = parser.parse(in->c_str(), in->length());
    if (!b) {
      CLS_ERR("%s: failed to parse json", __PRETTY_FUNCTION__);
      return -EBADMSG;
    }
    auto *o_cls = parser.find_obj("cls");
    ceph_assert(o_cls);
    std::string cls = o_cls->get_data_val().str;

    auto *o_method = parser.find_obj("method");
    ceph_assert(o_method);
    std::string method = o_method->get_data_val().str;

    auto *o_pool = parser.find_obj("pool");
    ceph_assert(o_pool);
    std::string pool = o_pool->get_data_val().str;

    auto *o_src_objects = parser.find_obj("src_objects_chain");
    ceph_assert(o_src_objects);
    auto src_objects_v = o_src_objects->get_array_elements();
    std::set<std::string> src_objects;
    for (auto it = src_objects_v.begin(); it != src_objects_v.end(); it++) {
      std::string oid_without_double_quotes = it->substr(1, it->size()-2);
      CLS_LOG(1, "Ken remote read: %s", oid_without_double_quotes.c_str());
      src_objects.insert(oid_without_double_quotes);
    }
    src_objects.insert("src_object.4");
    r = cls_cxx_gather(hctx, src_objects, pool, cls.c_str(), method.c_str(), *in);
  } else {
    // write data gathered using remote reads
    //int offset = 0;
    for (std::map<std::string, bufferlist>::iterator it = src_obj_buffs.begin(); it != src_obj_buffs.end(); it++) {
      bufferlist bl= it->second;
      //r = cls_cxx_write(hctx, offset, bl.length(), &bl);
      //offset += bl.length();
      out->claim_append(bl);
    }
    CLS_LOG(1, "winter debug: in test_gather: out length = %u", out->length());
  }
  return r;
}

CLS_INIT(test_remote_operations)
{
  CLS_LOG(0, "loading cls_test_remote_operations");

  cls_register("test_remote_operations", &h_class);
  
  cls_register_cxx_method(h_class, "test_read",
			  CLS_METHOD_RD,
			  test_read, &h_test_read);

  cls_register_cxx_method(h_class, "test_write",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  test_write, &h_test_write);

  cls_register_cxx_method(h_class, "test_scatter",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  test_scatter, &h_test_scatter);

  cls_register_cxx_method(h_class, "test_gather",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  test_gather, &h_test_gather);
}
