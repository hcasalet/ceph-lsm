//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

/* Copyright (c) 2011 The LevelDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file. See the AUTHORS file for names of contributors.

  C bindings for cabindb.  May be useful as a stable ABI that can be
  used by programs that keep cabindb in a shared library, or for
  a JNI api.

  Does not support:
  . getters for the option types
  . custom comparators that implement key shortening
  . capturing post-write-snapshot
  . custom iter, db, env, cache implementations using just the C bindings

  Some conventions:

  (1) We expose just opaque struct pointers and functions to clients.
  This allows us to change internal representations without having to
  recompile clients.

  (2) For simplicity, there is no equivalent to the Slice type.  Instead,
  the caller has to pass the pointer and length as separate
  arguments.

  (3) Errors are represented by a null-terminated c string.  NULL
  means no error.  All operations that can raise an error are passed
  a "char** errptr" as the last argument.  One of the following must
  be true on entry:
     *errptr == NULL
     *errptr points to a malloc()ed null-terminated error message
  On success, a leveldb routine leaves *errptr unchanged.
  On failure, leveldb frees the old value of *errptr and
  set *errptr to a malloc()ed error message.

  (4) Bools have the type unsigned char (0 == false; rest == true)

  (5) All of the pointer arguments must be non-NULL.
*/

#pragma once

#ifdef _WIN32
#ifdef CABINDB_DLL
#ifdef CABINDB_LIBRARY_EXPORTS
#define CABINDB_LIBRARY_API __declspec(dllexport)
#else
#define CABINDB_LIBRARY_API __declspec(dllimport)
#endif
#else
#define CABINDB_LIBRARY_API
#endif
#else
#define CABINDB_LIBRARY_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

/* Exported types */

typedef struct cabindb_t                 cabindb_t;
typedef struct cabindb_backup_engine_t   cabindb_backup_engine_t;
typedef struct cabindb_backup_engine_info_t   cabindb_backup_engine_info_t;
typedef struct cabindb_backupable_db_options_t cabindb_backupable_db_options_t;
typedef struct cabindb_restore_options_t cabindb_restore_options_t;
typedef struct cabindb_cache_t           cabindb_cache_t;
typedef struct cabindb_compactionfilter_t cabindb_compactionfilter_t;
typedef struct cabindb_compactionfiltercontext_t
    cabindb_compactionfiltercontext_t;
typedef struct cabindb_compactionfilterfactory_t
    cabindb_compactionfilterfactory_t;
typedef struct cabindb_comparator_t      cabindb_comparator_t;
typedef struct cabindb_dbpath_t          cabindb_dbpath_t;
typedef struct cabindb_env_t             cabindb_env_t;
typedef struct cabindb_fifo_compaction_options_t cabindb_fifo_compaction_options_t;
typedef struct cabindb_filelock_t        cabindb_filelock_t;
typedef struct cabindb_filterpolicy_t    cabindb_filterpolicy_t;
typedef struct cabindb_flushoptions_t    cabindb_flushoptions_t;
typedef struct cabindb_iterator_t        cabindb_iterator_t;
typedef struct cabindb_logger_t          cabindb_logger_t;
typedef struct cabindb_mergeoperator_t   cabindb_mergeoperator_t;
typedef struct cabindb_options_t         cabindb_options_t;
typedef struct cabindb_compactoptions_t cabindb_compactoptions_t;
typedef struct cabindb_block_based_table_options_t
    cabindb_block_based_table_options_t;
typedef struct cabindb_cuckoo_table_options_t
    cabindb_cuckoo_table_options_t;
typedef struct cabindb_randomfile_t      cabindb_randomfile_t;
typedef struct cabindb_readoptions_t     cabindb_readoptions_t;
typedef struct cabindb_seqfile_t         cabindb_seqfile_t;
typedef struct cabindb_slicetransform_t  cabindb_slicetransform_t;
typedef struct cabindb_snapshot_t        cabindb_snapshot_t;
typedef struct cabindb_writablefile_t    cabindb_writablefile_t;
typedef struct cabindb_writebatch_t      cabindb_writebatch_t;
typedef struct cabindb_writebatch_wi_t   cabindb_writebatch_wi_t;
typedef struct cabindb_writeoptions_t    cabindb_writeoptions_t;
typedef struct cabindb_universal_compaction_options_t cabindb_universal_compaction_options_t;
typedef struct cabindb_livefiles_t     cabindb_livefiles_t;
typedef struct cabindb_column_family_handle_t cabindb_column_family_handle_t;
typedef struct cabindb_envoptions_t      cabindb_envoptions_t;
typedef struct cabindb_ingestexternalfileoptions_t cabindb_ingestexternalfileoptions_t;
typedef struct cabindb_sstfilewriter_t   cabindb_sstfilewriter_t;
typedef struct cabindb_ratelimiter_t     cabindb_ratelimiter_t;
typedef struct cabindb_perfcontext_t     cabindb_perfcontext_t;
typedef struct cabindb_pinnableslice_t cabindb_pinnableslice_t;
typedef struct cabindb_transactiondb_options_t cabindb_transactiondb_options_t;
typedef struct cabindb_transactiondb_t cabindb_transactiondb_t;
typedef struct cabindb_transaction_options_t cabindb_transaction_options_t;
typedef struct cabindb_optimistictransactiondb_t
    cabindb_optimistictransactiondb_t;
typedef struct cabindb_optimistictransaction_options_t
    cabindb_optimistictransaction_options_t;
typedef struct cabindb_transaction_t cabindb_transaction_t;
typedef struct cabindb_checkpoint_t cabindb_checkpoint_t;
typedef struct cabindb_wal_iterator_t cabindb_wal_iterator_t;
typedef struct cabindb_wal_readoptions_t cabindb_wal_readoptions_t;
typedef struct cabindb_memory_consumers_t cabindb_memory_consumers_t;
typedef struct cabindb_memory_usage_t cabindb_memory_usage_t;

/* DB operations */

extern CABINDB_LIBRARY_API cabindb_t* cabindb_open(
    const cabindb_options_t* options, const char* name, char** errptr);

extern CABINDB_LIBRARY_API cabindb_t* cabindb_open_with_ttl(
    const cabindb_options_t* options, const char* name, int ttl, char** errptr);

extern CABINDB_LIBRARY_API cabindb_t* cabindb_open_for_read_only(
    const cabindb_options_t* options, const char* name,
    unsigned char error_if_wal_file_exists, char** errptr);

extern CABINDB_LIBRARY_API cabindb_t* cabindb_open_as_secondary(
    const cabindb_options_t* options, const char* name,
    const char* secondary_path, char** errptr);

extern CABINDB_LIBRARY_API cabindb_backup_engine_t* cabindb_backup_engine_open(
    const cabindb_options_t* options, const char* path, char** errptr);

extern CABINDB_LIBRARY_API cabindb_backup_engine_t*
cabindb_backup_engine_open_opts(const cabindb_backupable_db_options_t* options,
                                cabindb_env_t* env, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_backup_engine_create_new_backup(
    cabindb_backup_engine_t* be, cabindb_t* db, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_backup_engine_create_new_backup_flush(
    cabindb_backup_engine_t* be, cabindb_t* db, unsigned char flush_before_backup,
    char** errptr);

extern CABINDB_LIBRARY_API void cabindb_backup_engine_purge_old_backups(
    cabindb_backup_engine_t* be, uint32_t num_backups_to_keep, char** errptr);

extern CABINDB_LIBRARY_API cabindb_restore_options_t*
cabindb_restore_options_create();
extern CABINDB_LIBRARY_API void cabindb_restore_options_destroy(
    cabindb_restore_options_t* opt);
extern CABINDB_LIBRARY_API void cabindb_restore_options_set_keep_log_files(
    cabindb_restore_options_t* opt, int v);

extern CABINDB_LIBRARY_API void
cabindb_backup_engine_verify_backup(cabindb_backup_engine_t* be,
    uint32_t backup_id, char** errptr);

extern CABINDB_LIBRARY_API void
cabindb_backup_engine_restore_db_from_latest_backup(
    cabindb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
    const cabindb_restore_options_t* restore_options, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_backup_engine_restore_db_from_backup(
    cabindb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
    const cabindb_restore_options_t* restore_options, const uint32_t backup_id,
    char** errptr);

extern CABINDB_LIBRARY_API const cabindb_backup_engine_info_t*
cabindb_backup_engine_get_backup_info(cabindb_backup_engine_t* be);

extern CABINDB_LIBRARY_API int cabindb_backup_engine_info_count(
    const cabindb_backup_engine_info_t* info);

extern CABINDB_LIBRARY_API int64_t
cabindb_backup_engine_info_timestamp(const cabindb_backup_engine_info_t* info,
                                     int index);

extern CABINDB_LIBRARY_API uint32_t
cabindb_backup_engine_info_backup_id(const cabindb_backup_engine_info_t* info,
                                     int index);

extern CABINDB_LIBRARY_API uint64_t
cabindb_backup_engine_info_size(const cabindb_backup_engine_info_t* info,
                                int index);

extern CABINDB_LIBRARY_API uint32_t cabindb_backup_engine_info_number_files(
    const cabindb_backup_engine_info_t* info, int index);

extern CABINDB_LIBRARY_API void cabindb_backup_engine_info_destroy(
    const cabindb_backup_engine_info_t* info);

extern CABINDB_LIBRARY_API void cabindb_backup_engine_close(
    cabindb_backup_engine_t* be);

/* BackupableDBOptions */

extern CABINDB_LIBRARY_API cabindb_backupable_db_options_t*
cabindb_backupable_db_options_create(const char* backup_dir);

extern CABINDB_LIBRARY_API void cabindb_backupable_db_options_set_backup_dir(
    cabindb_backupable_db_options_t* options, const char* backup_dir);

extern CABINDB_LIBRARY_API void cabindb_backupable_db_options_set_env(
    cabindb_backupable_db_options_t* options, cabindb_env_t* env);

extern CABINDB_LIBRARY_API void
cabindb_backupable_db_options_set_share_table_files(
    cabindb_backupable_db_options_t* options, unsigned char val);

extern CABINDB_LIBRARY_API unsigned char
cabindb_backupable_db_options_get_share_table_files(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void cabindb_backupable_db_options_set_sync(
    cabindb_backupable_db_options_t* options, unsigned char val);

extern CABINDB_LIBRARY_API unsigned char cabindb_backupable_db_options_get_sync(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void
cabindb_backupable_db_options_set_destroy_old_data(
    cabindb_backupable_db_options_t* options, unsigned char val);

extern CABINDB_LIBRARY_API unsigned char
cabindb_backupable_db_options_get_destroy_old_data(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void
cabindb_backupable_db_options_set_backup_log_files(
    cabindb_backupable_db_options_t* options, unsigned char val);

extern CABINDB_LIBRARY_API unsigned char
cabindb_backupable_db_options_get_backup_log_files(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void
cabindb_backupable_db_options_set_backup_rate_limit(
    cabindb_backupable_db_options_t* options, uint64_t limit);

extern CABINDB_LIBRARY_API uint64_t
cabindb_backupable_db_options_get_backup_rate_limit(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void
cabindb_backupable_db_options_set_restore_rate_limit(
    cabindb_backupable_db_options_t* options, uint64_t limit);

extern CABINDB_LIBRARY_API uint64_t
cabindb_backupable_db_options_get_restore_rate_limit(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void
cabindb_backupable_db_options_set_max_background_operations(
    cabindb_backupable_db_options_t* options, int val);

extern CABINDB_LIBRARY_API int
cabindb_backupable_db_options_get_max_background_operations(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void
cabindb_backupable_db_options_set_callback_trigger_interval_size(
    cabindb_backupable_db_options_t* options, uint64_t size);

extern CABINDB_LIBRARY_API uint64_t
cabindb_backupable_db_options_get_callback_trigger_interval_size(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void
cabindb_backupable_db_options_set_max_valid_backups_to_open(
    cabindb_backupable_db_options_t* options, int val);

extern CABINDB_LIBRARY_API int
cabindb_backupable_db_options_get_max_valid_backups_to_open(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void
cabindb_backupable_db_options_set_share_files_with_checksum_naming(
    cabindb_backupable_db_options_t* options, int val);

extern CABINDB_LIBRARY_API int
cabindb_backupable_db_options_get_share_files_with_checksum_naming(
    cabindb_backupable_db_options_t* options);

extern CABINDB_LIBRARY_API void cabindb_backupable_db_options_destroy(
    cabindb_backupable_db_options_t*);

/* Checkpoint */

extern CABINDB_LIBRARY_API cabindb_checkpoint_t*
cabindb_checkpoint_object_create(cabindb_t* db, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_checkpoint_create(
    cabindb_checkpoint_t* checkpoint, const char* checkpoint_dir,
    uint64_t log_size_for_flush, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_checkpoint_object_destroy(
    cabindb_checkpoint_t* checkpoint);

extern CABINDB_LIBRARY_API cabindb_t* cabindb_open_column_families(
    const cabindb_options_t* options, const char* name, int num_column_families,
    const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles, char** errptr);

extern CABINDB_LIBRARY_API cabindb_t* cabindb_open_column_families_with_ttl(
    const cabindb_options_t* options, const char* name, int num_column_families,
    const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles, const int* ttls,
    char** errptr);

extern CABINDB_LIBRARY_API cabindb_t*
cabindb_open_for_read_only_column_families(
    const cabindb_options_t* options, const char* name, int num_column_families,
    const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles,
    unsigned char error_if_wal_file_exists, char** errptr);

extern CABINDB_LIBRARY_API cabindb_t* cabindb_open_as_secondary_column_families(
    const cabindb_options_t* options, const char* name,
    const char* secondary_path, int num_column_families,
    const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** colummn_family_handles, char** errptr);

extern CABINDB_LIBRARY_API char** cabindb_list_column_families(
    const cabindb_options_t* options, const char* name, size_t* lencf,
    char** errptr);

extern CABINDB_LIBRARY_API void cabindb_list_column_families_destroy(
    char** list, size_t len);

extern CABINDB_LIBRARY_API cabindb_column_family_handle_t*
cabindb_create_column_family(cabindb_t* db,
                             const cabindb_options_t* column_family_options,
                             const char* column_family_name, char** errptr);

extern CABINDB_LIBRARY_API cabindb_column_family_handle_t*
cabindb_create_column_family_with_ttl(
    cabindb_t* db, const cabindb_options_t* column_family_options,
    const char* column_family_name, int ttl, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_drop_column_family(
    cabindb_t* db, cabindb_column_family_handle_t* handle, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_column_family_handle_destroy(
    cabindb_column_family_handle_t*);

extern CABINDB_LIBRARY_API void cabindb_close(cabindb_t* db);

extern CABINDB_LIBRARY_API void cabindb_put(
    cabindb_t* db, const cabindb_writeoptions_t* options, const char* key,
    size_t keylen, const char* val, size_t vallen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_put_cf(
    cabindb_t* db, const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, const char* val, size_t vallen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_delete(
    cabindb_t* db, const cabindb_writeoptions_t* options, const char* key,
    size_t keylen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_delete_cf(
    cabindb_t* db, const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_delete_range_cf(
    cabindb_t* db, const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* start_key,
    size_t start_key_len, const char* end_key, size_t end_key_len,
    char** errptr);

extern CABINDB_LIBRARY_API void cabindb_merge(
    cabindb_t* db, const cabindb_writeoptions_t* options, const char* key,
    size_t keylen, const char* val, size_t vallen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_merge_cf(
    cabindb_t* db, const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, const char* val, size_t vallen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_write(
    cabindb_t* db, const cabindb_writeoptions_t* options,
    cabindb_writebatch_t* batch, char** errptr);

/* Returns NULL if not found.  A malloc()ed array otherwise.
   Stores the length of the array in *vallen. */
extern CABINDB_LIBRARY_API char* cabindb_get(
    cabindb_t* db, const cabindb_readoptions_t* options, const char* key,
    size_t keylen, size_t* vallen, char** errptr);

extern CABINDB_LIBRARY_API char* cabindb_get_cf(
    cabindb_t* db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** errptr);

// if values_list[i] == NULL and errs[i] == NULL,
// then we got status.IsNotFound(), which we will not return.
// all errors except status status.ok() and status.IsNotFound() are returned.
//
// errs, values_list and values_list_sizes must be num_keys in length,
// allocated by the caller.
// errs is a list of strings as opposed to the conventional one error,
// where errs[i] is the status for retrieval of keys_list[i].
// each non-NULL errs entry is a malloc()ed, null terminated string.
// each non-NULL values_list entry is a malloc()ed array, with
// the length for each stored in values_list_sizes[i].
extern CABINDB_LIBRARY_API void cabindb_multi_get(
    cabindb_t* db, const cabindb_readoptions_t* options, size_t num_keys,
    const char* const* keys_list, const size_t* keys_list_sizes,
    char** values_list, size_t* values_list_sizes, char** errs);

extern CABINDB_LIBRARY_API void cabindb_multi_get_cf(
    cabindb_t* db, const cabindb_readoptions_t* options,
    const cabindb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** errs);

// The value is only allocated (using malloc) and returned if it is found and
// value_found isn't NULL. In that case the user is responsible for freeing it.
extern CABINDB_LIBRARY_API unsigned char cabindb_key_may_exist(
    cabindb_t* db, const cabindb_readoptions_t* options, const char* key,
    size_t key_len, char** value, size_t* val_len, const char* timestamp,
    size_t timestamp_len, unsigned char* value_found);

// The value is only allocated (using malloc) and returned if it is found and
// value_found isn't NULL. In that case the user is responsible for freeing it.
extern CABINDB_LIBRARY_API unsigned char cabindb_key_may_exist_cf(
    cabindb_t* db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t key_len, char** value, size_t* val_len, const char* timestamp,
    size_t timestamp_len, unsigned char* value_found);

extern CABINDB_LIBRARY_API cabindb_iterator_t* cabindb_create_iterator(
    cabindb_t* db, const cabindb_readoptions_t* options);

extern CABINDB_LIBRARY_API cabindb_wal_iterator_t* cabindb_get_updates_since(
        cabindb_t* db, uint64_t seq_number,
        const cabindb_wal_readoptions_t* options,
        char** errptr
);

extern CABINDB_LIBRARY_API cabindb_iterator_t* cabindb_create_iterator_cf(
    cabindb_t* db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family);

extern CABINDB_LIBRARY_API void cabindb_create_iterators(
    cabindb_t *db, cabindb_readoptions_t* opts,
    cabindb_column_family_handle_t** column_families,
    cabindb_iterator_t** iterators, size_t size, char** errptr);

extern CABINDB_LIBRARY_API const cabindb_snapshot_t* cabindb_create_snapshot(
    cabindb_t* db);

extern CABINDB_LIBRARY_API void cabindb_release_snapshot(
    cabindb_t* db, const cabindb_snapshot_t* snapshot);

/* Returns NULL if property name is unknown.
   Else returns a pointer to a malloc()-ed null-terminated value. */
extern CABINDB_LIBRARY_API char* cabindb_property_value(cabindb_t* db,
                                                        const char* propname);
/* returns 0 on success, -1 otherwise */
int cabindb_property_int(
    cabindb_t* db,
    const char* propname, uint64_t *out_val);

/* returns 0 on success, -1 otherwise */
int cabindb_property_int_cf(
    cabindb_t* db, cabindb_column_family_handle_t* column_family,
    const char* propname, uint64_t *out_val);

extern CABINDB_LIBRARY_API char* cabindb_property_value_cf(
    cabindb_t* db, cabindb_column_family_handle_t* column_family,
    const char* propname);

extern CABINDB_LIBRARY_API void cabindb_approximate_sizes(
    cabindb_t* db, int num_ranges, const char* const* range_start_key,
    const size_t* range_start_key_len, const char* const* range_limit_key,
    const size_t* range_limit_key_len, uint64_t* sizes);

extern CABINDB_LIBRARY_API void cabindb_approximate_sizes_cf(
    cabindb_t* db, cabindb_column_family_handle_t* column_family,
    int num_ranges, const char* const* range_start_key,
    const size_t* range_start_key_len, const char* const* range_limit_key,
    const size_t* range_limit_key_len, uint64_t* sizes);

extern CABINDB_LIBRARY_API void cabindb_compact_range(cabindb_t* db,
                                                      const char* start_key,
                                                      size_t start_key_len,
                                                      const char* limit_key,
                                                      size_t limit_key_len);

extern CABINDB_LIBRARY_API void cabindb_compact_range_cf(
    cabindb_t* db, cabindb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len);

extern CABINDB_LIBRARY_API void cabindb_compact_range_opt(
    cabindb_t* db, cabindb_compactoptions_t* opt, const char* start_key,
    size_t start_key_len, const char* limit_key, size_t limit_key_len);

extern CABINDB_LIBRARY_API void cabindb_compact_range_cf_opt(
    cabindb_t* db, cabindb_column_family_handle_t* column_family,
    cabindb_compactoptions_t* opt, const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len);

extern CABINDB_LIBRARY_API void cabindb_delete_file(cabindb_t* db,
                                                    const char* name);

extern CABINDB_LIBRARY_API const cabindb_livefiles_t* cabindb_livefiles(
    cabindb_t* db);

extern CABINDB_LIBRARY_API void cabindb_flush(
    cabindb_t* db, const cabindb_flushoptions_t* options, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_flush_cf(
    cabindb_t* db, const cabindb_flushoptions_t* options,
    cabindb_column_family_handle_t* column_family, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_disable_file_deletions(cabindb_t* db,
                                                               char** errptr);

extern CABINDB_LIBRARY_API void cabindb_enable_file_deletions(
    cabindb_t* db, unsigned char force, char** errptr);

/* Management operations */

extern CABINDB_LIBRARY_API void cabindb_destroy_db(
    const cabindb_options_t* options, const char* name, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_repair_db(
    const cabindb_options_t* options, const char* name, char** errptr);

/* Iterator */

extern CABINDB_LIBRARY_API void cabindb_iter_destroy(cabindb_iterator_t*);
extern CABINDB_LIBRARY_API unsigned char cabindb_iter_valid(
    const cabindb_iterator_t*);
extern CABINDB_LIBRARY_API void cabindb_iter_seek_to_first(cabindb_iterator_t*);
extern CABINDB_LIBRARY_API void cabindb_iter_seek_to_last(cabindb_iterator_t*);
extern CABINDB_LIBRARY_API void cabindb_iter_seek(cabindb_iterator_t*,
                                                  const char* k, size_t klen);
extern CABINDB_LIBRARY_API void cabindb_iter_seek_for_prev(cabindb_iterator_t*,
                                                           const char* k,
                                                           size_t klen);
extern CABINDB_LIBRARY_API void cabindb_iter_next(cabindb_iterator_t*);
extern CABINDB_LIBRARY_API void cabindb_iter_prev(cabindb_iterator_t*);
extern CABINDB_LIBRARY_API const char* cabindb_iter_key(
    const cabindb_iterator_t*, size_t* klen);
extern CABINDB_LIBRARY_API const char* cabindb_iter_value(
    const cabindb_iterator_t*, size_t* vlen);
extern CABINDB_LIBRARY_API void cabindb_iter_get_error(
    const cabindb_iterator_t*, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_wal_iter_next(cabindb_wal_iterator_t* iter);
extern CABINDB_LIBRARY_API unsigned char cabindb_wal_iter_valid(
        const cabindb_wal_iterator_t*);
extern CABINDB_LIBRARY_API void cabindb_wal_iter_status (const cabindb_wal_iterator_t* iter, char** errptr) ;
extern CABINDB_LIBRARY_API cabindb_writebatch_t* cabindb_wal_iter_get_batch (const cabindb_wal_iterator_t* iter, uint64_t* seq) ;
extern CABINDB_LIBRARY_API uint64_t cabindb_get_latest_sequence_number (cabindb_t *db);
extern CABINDB_LIBRARY_API void cabindb_wal_iter_destroy (const cabindb_wal_iterator_t* iter) ;

/* Write batch */

extern CABINDB_LIBRARY_API cabindb_writebatch_t* cabindb_writebatch_create();
extern CABINDB_LIBRARY_API cabindb_writebatch_t* cabindb_writebatch_create_from(
    const char* rep, size_t size);
extern CABINDB_LIBRARY_API void cabindb_writebatch_destroy(
    cabindb_writebatch_t*);
extern CABINDB_LIBRARY_API void cabindb_writebatch_clear(cabindb_writebatch_t*);
extern CABINDB_LIBRARY_API int cabindb_writebatch_count(cabindb_writebatch_t*);
extern CABINDB_LIBRARY_API void cabindb_writebatch_put(cabindb_writebatch_t*,
                                                       const char* key,
                                                       size_t klen,
                                                       const char* val,
                                                       size_t vlen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_put_cf(
    cabindb_writebatch_t*, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen, const char* val, size_t vlen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_putv(
    cabindb_writebatch_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, int num_values,
    const char* const* values_list, const size_t* values_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_putv_cf(
    cabindb_writebatch_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_merge(cabindb_writebatch_t*,
                                                         const char* key,
                                                         size_t klen,
                                                         const char* val,
                                                         size_t vlen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_merge_cf(
    cabindb_writebatch_t*, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen, const char* val, size_t vlen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_mergev(
    cabindb_writebatch_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, int num_values,
    const char* const* values_list, const size_t* values_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_mergev_cf(
    cabindb_writebatch_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_delete(cabindb_writebatch_t*,
                                                          const char* key,
                                                          size_t klen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_singledelete(
    cabindb_writebatch_t* b, const char* key, size_t klen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_delete_cf(
    cabindb_writebatch_t*, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_singledelete_cf(
    cabindb_writebatch_t* b, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_deletev(
    cabindb_writebatch_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_deletev_cf(
    cabindb_writebatch_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_delete_range(
    cabindb_writebatch_t* b, const char* start_key, size_t start_key_len,
    const char* end_key, size_t end_key_len);
extern CABINDB_LIBRARY_API void cabindb_writebatch_delete_range_cf(
    cabindb_writebatch_t* b, cabindb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* end_key,
    size_t end_key_len);
extern CABINDB_LIBRARY_API void cabindb_writebatch_delete_rangev(
    cabindb_writebatch_t* b, int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_delete_rangev_cf(
    cabindb_writebatch_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_put_log_data(
    cabindb_writebatch_t*, const char* blob, size_t len);
extern CABINDB_LIBRARY_API void cabindb_writebatch_iterate(
    cabindb_writebatch_t*, void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen));
extern CABINDB_LIBRARY_API const char* cabindb_writebatch_data(
    cabindb_writebatch_t*, size_t* size);
extern CABINDB_LIBRARY_API void cabindb_writebatch_set_save_point(
    cabindb_writebatch_t*);
extern CABINDB_LIBRARY_API void cabindb_writebatch_rollback_to_save_point(
    cabindb_writebatch_t*, char** errptr);
extern CABINDB_LIBRARY_API void cabindb_writebatch_pop_save_point(
    cabindb_writebatch_t*, char** errptr);

/* Write batch with index */

extern CABINDB_LIBRARY_API cabindb_writebatch_wi_t* cabindb_writebatch_wi_create(
                                                       size_t reserved_bytes,
                                                       unsigned char overwrite_keys);
extern CABINDB_LIBRARY_API cabindb_writebatch_wi_t* cabindb_writebatch_wi_create_from(
    const char* rep, size_t size);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_destroy(
    cabindb_writebatch_wi_t*);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_clear(cabindb_writebatch_wi_t*);
extern CABINDB_LIBRARY_API int cabindb_writebatch_wi_count(cabindb_writebatch_wi_t* b);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_put(cabindb_writebatch_wi_t*,
                                                       const char* key,
                                                       size_t klen,
                                                       const char* val,
                                                       size_t vlen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_put_cf(
    cabindb_writebatch_wi_t*, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen, const char* val, size_t vlen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_putv(
    cabindb_writebatch_wi_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, int num_values,
    const char* const* values_list, const size_t* values_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_putv_cf(
    cabindb_writebatch_wi_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_merge(cabindb_writebatch_wi_t*,
                                                         const char* key,
                                                         size_t klen,
                                                         const char* val,
                                                         size_t vlen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_merge_cf(
    cabindb_writebatch_wi_t*, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen, const char* val, size_t vlen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_mergev(
    cabindb_writebatch_wi_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, int num_values,
    const char* const* values_list, const size_t* values_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_mergev_cf(
    cabindb_writebatch_wi_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_delete(cabindb_writebatch_wi_t*,
                                                          const char* key,
                                                          size_t klen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_singledelete(
    cabindb_writebatch_wi_t*, const char* key, size_t klen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_delete_cf(
    cabindb_writebatch_wi_t*, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_singledelete_cf(
    cabindb_writebatch_wi_t*, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_deletev(
    cabindb_writebatch_wi_t* b, int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_deletev_cf(
    cabindb_writebatch_wi_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes);
// DO NOT USE - cabindb_writebatch_wi_delete_range is not yet supported
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_delete_range(
    cabindb_writebatch_wi_t* b, const char* start_key, size_t start_key_len,
    const char* end_key, size_t end_key_len);
// DO NOT USE - cabindb_writebatch_wi_delete_range_cf is not yet supported
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_delete_range_cf(
    cabindb_writebatch_wi_t* b, cabindb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* end_key,
    size_t end_key_len);
// DO NOT USE - cabindb_writebatch_wi_delete_rangev is not yet supported
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_delete_rangev(
    cabindb_writebatch_wi_t* b, int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes);
// DO NOT USE - cabindb_writebatch_wi_delete_rangev_cf is not yet supported
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_delete_rangev_cf(
    cabindb_writebatch_wi_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_put_log_data(
    cabindb_writebatch_wi_t*, const char* blob, size_t len);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_iterate(
    cabindb_writebatch_wi_t* b,
    void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen));
extern CABINDB_LIBRARY_API const char* cabindb_writebatch_wi_data(
    cabindb_writebatch_wi_t* b,
    size_t* size);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_set_save_point(
    cabindb_writebatch_wi_t*);
extern CABINDB_LIBRARY_API void cabindb_writebatch_wi_rollback_to_save_point(
    cabindb_writebatch_wi_t*, char** errptr);
extern CABINDB_LIBRARY_API char* cabindb_writebatch_wi_get_from_batch(
    cabindb_writebatch_wi_t* wbwi,
    const cabindb_options_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr);
extern CABINDB_LIBRARY_API char* cabindb_writebatch_wi_get_from_batch_cf(
    cabindb_writebatch_wi_t* wbwi,
    const cabindb_options_t* options,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr);
extern CABINDB_LIBRARY_API char* cabindb_writebatch_wi_get_from_batch_and_db(
    cabindb_writebatch_wi_t* wbwi,
    cabindb_t* db,
    const cabindb_readoptions_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr);
extern CABINDB_LIBRARY_API char* cabindb_writebatch_wi_get_from_batch_and_db_cf(
    cabindb_writebatch_wi_t* wbwi,
    cabindb_t* db,
    const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr);
extern CABINDB_LIBRARY_API void cabindb_write_writebatch_wi(
    cabindb_t* db,
    const cabindb_writeoptions_t* options,
    cabindb_writebatch_wi_t* wbwi,
    char** errptr);
extern CABINDB_LIBRARY_API cabindb_iterator_t* cabindb_writebatch_wi_create_iterator_with_base(
    cabindb_writebatch_wi_t* wbwi,
    cabindb_iterator_t* base_iterator);
extern CABINDB_LIBRARY_API cabindb_iterator_t* cabindb_writebatch_wi_create_iterator_with_base_cf(
    cabindb_writebatch_wi_t* wbwi,
    cabindb_iterator_t* base_iterator,
    cabindb_column_family_handle_t* cf);

/* Block based table options */

extern CABINDB_LIBRARY_API cabindb_block_based_table_options_t*
cabindb_block_based_options_create();
extern CABINDB_LIBRARY_API void cabindb_block_based_options_destroy(
    cabindb_block_based_table_options_t* options);
extern CABINDB_LIBRARY_API void cabindb_block_based_options_set_block_size(
    cabindb_block_based_table_options_t* options, size_t block_size);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_block_size_deviation(
    cabindb_block_based_table_options_t* options, int block_size_deviation);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_block_restart_interval(
    cabindb_block_based_table_options_t* options, int block_restart_interval);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_index_block_restart_interval(
    cabindb_block_based_table_options_t* options, int index_block_restart_interval);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_metadata_block_size(
    cabindb_block_based_table_options_t* options, uint64_t metadata_block_size);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_partition_filters(
    cabindb_block_based_table_options_t* options, unsigned char partition_filters);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_use_delta_encoding(
    cabindb_block_based_table_options_t* options, unsigned char use_delta_encoding);
extern CABINDB_LIBRARY_API void cabindb_block_based_options_set_filter_policy(
    cabindb_block_based_table_options_t* options,
    cabindb_filterpolicy_t* filter_policy);
extern CABINDB_LIBRARY_API void cabindb_block_based_options_set_no_block_cache(
    cabindb_block_based_table_options_t* options, unsigned char no_block_cache);
extern CABINDB_LIBRARY_API void cabindb_block_based_options_set_block_cache(
    cabindb_block_based_table_options_t* options, cabindb_cache_t* block_cache);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_block_cache_compressed(
    cabindb_block_based_table_options_t* options,
    cabindb_cache_t* block_cache_compressed);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_whole_key_filtering(
    cabindb_block_based_table_options_t*, unsigned char);
extern CABINDB_LIBRARY_API void cabindb_block_based_options_set_format_version(
    cabindb_block_based_table_options_t*, int);
enum {
  cabindb_block_based_table_index_type_binary_search = 0,
  cabindb_block_based_table_index_type_hash_search = 1,
  cabindb_block_based_table_index_type_two_level_index_search = 2,
};
extern CABINDB_LIBRARY_API void cabindb_block_based_options_set_index_type(
    cabindb_block_based_table_options_t*, int);  // uses one of the above enums
enum {
  cabindb_block_based_table_data_block_index_type_binary_search = 0,
  cabindb_block_based_table_data_block_index_type_binary_search_and_hash = 1,
};
extern CABINDB_LIBRARY_API void cabindb_block_based_options_set_data_block_index_type(
    cabindb_block_based_table_options_t*, int);  // uses one of the above enums
extern CABINDB_LIBRARY_API void cabindb_block_based_options_set_data_block_hash_ratio(
    cabindb_block_based_table_options_t* options, double v);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_hash_index_allow_collision(
    cabindb_block_based_table_options_t*, unsigned char);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_cache_index_and_filter_blocks(
    cabindb_block_based_table_options_t*, unsigned char);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(
    cabindb_block_based_table_options_t*, unsigned char);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(
    cabindb_block_based_table_options_t*, unsigned char);
extern CABINDB_LIBRARY_API void
cabindb_block_based_options_set_pin_top_level_index_and_filter(
    cabindb_block_based_table_options_t*, unsigned char);
extern CABINDB_LIBRARY_API void cabindb_options_set_block_based_table_factory(
    cabindb_options_t* opt, cabindb_block_based_table_options_t* table_options);

/* Cuckoo table options */

extern CABINDB_LIBRARY_API cabindb_cuckoo_table_options_t*
cabindb_cuckoo_options_create();
extern CABINDB_LIBRARY_API void cabindb_cuckoo_options_destroy(
    cabindb_cuckoo_table_options_t* options);
extern CABINDB_LIBRARY_API void cabindb_cuckoo_options_set_hash_ratio(
    cabindb_cuckoo_table_options_t* options, double v);
extern CABINDB_LIBRARY_API void cabindb_cuckoo_options_set_max_search_depth(
    cabindb_cuckoo_table_options_t* options, uint32_t v);
extern CABINDB_LIBRARY_API void cabindb_cuckoo_options_set_cuckoo_block_size(
    cabindb_cuckoo_table_options_t* options, uint32_t v);
extern CABINDB_LIBRARY_API void
cabindb_cuckoo_options_set_identity_as_first_hash(
    cabindb_cuckoo_table_options_t* options, unsigned char v);
extern CABINDB_LIBRARY_API void cabindb_cuckoo_options_set_use_module_hash(
    cabindb_cuckoo_table_options_t* options, unsigned char v);
extern CABINDB_LIBRARY_API void cabindb_options_set_cuckoo_table_factory(
    cabindb_options_t* opt, cabindb_cuckoo_table_options_t* table_options);

/* Options */
extern CABINDB_LIBRARY_API void cabindb_set_options(
    cabindb_t* db, int count, const char* const keys[], const char* const values[], char** errptr);

extern CABINDB_LIBRARY_API void cabindb_set_options_cf(
    cabindb_t* db, cabindb_column_family_handle_t* handle, int count, const char* const keys[], const char* const values[], char** errptr);

extern CABINDB_LIBRARY_API cabindb_options_t* cabindb_options_create();
extern CABINDB_LIBRARY_API void cabindb_options_destroy(cabindb_options_t*);
extern CABINDB_LIBRARY_API cabindb_options_t* cabindb_options_create_copy(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_increase_parallelism(
    cabindb_options_t* opt, int total_threads);
extern CABINDB_LIBRARY_API void cabindb_options_optimize_for_point_lookup(
    cabindb_options_t* opt, uint64_t block_cache_size_mb);
extern CABINDB_LIBRARY_API void cabindb_options_optimize_level_style_compaction(
    cabindb_options_t* opt, uint64_t memtable_memory_budget);
extern CABINDB_LIBRARY_API void
cabindb_options_optimize_universal_style_compaction(
    cabindb_options_t* opt, uint64_t memtable_memory_budget);
extern CABINDB_LIBRARY_API void
cabindb_options_set_allow_ingest_behind(cabindb_options_t*,
                                                   unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_allow_ingest_behind(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_compaction_filter(
    cabindb_options_t*, cabindb_compactionfilter_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_compaction_filter_factory(
    cabindb_options_t*, cabindb_compactionfilterfactory_t*);
extern CABINDB_LIBRARY_API void cabindb_options_compaction_readahead_size(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_compaction_readahead_size(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_comparator(
    cabindb_options_t*, cabindb_comparator_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_merge_operator(
    cabindb_options_t*, cabindb_mergeoperator_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_uint64add_merge_operator(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_compression_per_level(
    cabindb_options_t* opt, int* level_values, size_t num_levels);
extern CABINDB_LIBRARY_API void cabindb_options_set_create_if_missing(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_create_if_missing(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_create_missing_column_families(cabindb_options_t*,
                                                   unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_create_missing_column_families(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_error_if_exists(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_error_if_exists(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_paranoid_checks(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_paranoid_checks(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_db_paths(cabindb_options_t*,
                                                             const cabindb_dbpath_t** path_values,
                                                             size_t num_paths);
extern CABINDB_LIBRARY_API void cabindb_options_set_env(cabindb_options_t*,
                                                        cabindb_env_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_info_log(cabindb_options_t*,
                                                             cabindb_logger_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_info_log_level(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_info_log_level(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_write_buffer_size(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_write_buffer_size(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_db_write_buffer_size(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_db_write_buffer_size(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_open_files(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_max_open_files(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_file_opening_threads(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_max_file_opening_threads(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_total_wal_size(
    cabindb_options_t* opt, uint64_t n);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_max_total_wal_size(cabindb_options_t* opt);
extern CABINDB_LIBRARY_API void cabindb_options_set_compression_options(
    cabindb_options_t*, int, int, int, int);
extern CABINDB_LIBRARY_API void
cabindb_options_set_compression_options_zstd_max_train_bytes(cabindb_options_t*,
                                                             int);
extern CABINDB_LIBRARY_API void
cabindb_options_set_bottommost_compression_options(cabindb_options_t*, int, int,
                                                   int, int, unsigned char);
extern CABINDB_LIBRARY_API void
cabindb_options_set_bottommost_compression_options_zstd_max_train_bytes(
    cabindb_options_t*, int, unsigned char);
extern CABINDB_LIBRARY_API void cabindb_options_set_prefix_extractor(
    cabindb_options_t*, cabindb_slicetransform_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_num_levels(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_num_levels(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_level0_file_num_compaction_trigger(cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_options_get_level0_file_num_compaction_trigger(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_level0_slowdown_writes_trigger(cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_options_get_level0_slowdown_writes_trigger(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_level0_stop_writes_trigger(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_level0_stop_writes_trigger(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_mem_compaction_level(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API void cabindb_options_set_target_file_size_base(
    cabindb_options_t*, uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_target_file_size_base(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_target_file_size_multiplier(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_target_file_size_multiplier(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_bytes_for_level_base(
    cabindb_options_t*, uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_max_bytes_for_level_base(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_level_compaction_dynamic_level_bytes(cabindb_options_t*,
                                                         unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_level_compaction_dynamic_level_bytes(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_max_bytes_for_level_multiplier(cabindb_options_t*, double);
extern CABINDB_LIBRARY_API double
cabindb_options_get_max_bytes_for_level_multiplier(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_max_bytes_for_level_multiplier_additional(
    cabindb_options_t*, int* level_values, size_t num_levels);
extern CABINDB_LIBRARY_API void cabindb_options_enable_statistics(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_skip_stats_update_on_db_open(cabindb_options_t* opt,
                                                 unsigned char val);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_skip_stats_update_on_db_open(cabindb_options_t* opt);
extern CABINDB_LIBRARY_API void
cabindb_options_set_skip_checking_sst_file_sizes_on_db_open(
    cabindb_options_t* opt, unsigned char val);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_skip_checking_sst_file_sizes_on_db_open(
    cabindb_options_t* opt);

/* returns a pointer to a malloc()-ed, null terminated string */
extern CABINDB_LIBRARY_API char* cabindb_options_statistics_get_string(
    cabindb_options_t* opt);

extern CABINDB_LIBRARY_API void cabindb_options_set_max_write_buffer_number(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_max_write_buffer_number(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_min_write_buffer_number_to_merge(cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_options_get_min_write_buffer_number_to_merge(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_max_write_buffer_number_to_maintain(cabindb_options_t*,
                                                        int);
extern CABINDB_LIBRARY_API int
cabindb_options_get_max_write_buffer_number_to_maintain(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_max_write_buffer_size_to_maintain(cabindb_options_t*,
                                                      int64_t);
extern CABINDB_LIBRARY_API int64_t
cabindb_options_get_max_write_buffer_size_to_maintain(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_enable_pipelined_write(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_enable_pipelined_write(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_unordered_write(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_unordered_write(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_subcompactions(
    cabindb_options_t*, uint32_t);
extern CABINDB_LIBRARY_API uint32_t
cabindb_options_get_max_subcompactions(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_background_jobs(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_max_background_jobs(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_background_compactions(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_max_background_compactions(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_base_background_compactions(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_base_background_compactions(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_background_flushes(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_max_background_flushes(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_log_file_size(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_max_log_file_size(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_log_file_time_to_roll(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_log_file_time_to_roll(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_keep_log_file_num(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_keep_log_file_num(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_recycle_log_file_num(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_recycle_log_file_num(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_soft_rate_limit(
    cabindb_options_t*, double);
extern CABINDB_LIBRARY_API double cabindb_options_get_soft_rate_limit(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_hard_rate_limit(
    cabindb_options_t*, double);
extern CABINDB_LIBRARY_API double cabindb_options_get_hard_rate_limit(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_soft_pending_compaction_bytes_limit(
    cabindb_options_t* opt, size_t v);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_soft_pending_compaction_bytes_limit(cabindb_options_t* opt);
extern CABINDB_LIBRARY_API void cabindb_options_set_hard_pending_compaction_bytes_limit(
    cabindb_options_t* opt, size_t v);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_hard_pending_compaction_bytes_limit(cabindb_options_t* opt);
extern CABINDB_LIBRARY_API void
cabindb_options_set_rate_limit_delay_max_milliseconds(cabindb_options_t*,
                                                      unsigned int);
extern CABINDB_LIBRARY_API unsigned int
cabindb_options_get_rate_limit_delay_max_milliseconds(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_manifest_file_size(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_max_manifest_file_size(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_table_cache_numshardbits(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_table_cache_numshardbits(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_table_cache_remove_scan_count_limit(cabindb_options_t*,
                                                        int);
extern CABINDB_LIBRARY_API void cabindb_options_set_arena_block_size(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_arena_block_size(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_use_fsync(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_use_fsync(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_db_log_dir(
    cabindb_options_t*, const char*);
extern CABINDB_LIBRARY_API void cabindb_options_set_wal_dir(cabindb_options_t*,
                                                            const char*);
extern CABINDB_LIBRARY_API void cabindb_options_set_WAL_ttl_seconds(
    cabindb_options_t*, uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_WAL_ttl_seconds(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_WAL_size_limit_MB(
    cabindb_options_t*, uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_WAL_size_limit_MB(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_manifest_preallocation_size(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_manifest_preallocation_size(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_purge_redundant_kvs_while_flush(cabindb_options_t*,
                                                    unsigned char);
extern CABINDB_LIBRARY_API void cabindb_options_set_allow_mmap_reads(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_allow_mmap_reads(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_allow_mmap_writes(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_allow_mmap_writes(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_use_direct_reads(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_use_direct_reads(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_use_direct_io_for_flush_and_compaction(cabindb_options_t*,
                                                           unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_use_direct_io_for_flush_and_compaction(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_is_fd_close_on_exec(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_is_fd_close_on_exec(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_skip_log_error_on_recovery(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_skip_log_error_on_recovery(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_stats_dump_period_sec(
    cabindb_options_t*, unsigned int);
extern CABINDB_LIBRARY_API unsigned int
cabindb_options_get_stats_dump_period_sec(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_stats_persist_period_sec(
    cabindb_options_t*, unsigned int);
extern CABINDB_LIBRARY_API unsigned int
cabindb_options_get_stats_persist_period_sec(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_advise_random_on_open(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_advise_random_on_open(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_access_hint_on_compaction_start(cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_options_get_access_hint_on_compaction_start(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_use_adaptive_mutex(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_use_adaptive_mutex(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_bytes_per_sync(
    cabindb_options_t*, uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_bytes_per_sync(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_wal_bytes_per_sync(
        cabindb_options_t*, uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_wal_bytes_per_sync(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_writable_file_max_buffer_size(cabindb_options_t*, uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_writable_file_max_buffer_size(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_allow_concurrent_memtable_write(cabindb_options_t*,
                                                    unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_allow_concurrent_memtable_write(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_enable_write_thread_adaptive_yield(cabindb_options_t*,
                                                       unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_enable_write_thread_adaptive_yield(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_max_sequential_skip_in_iterations(cabindb_options_t*,
                                                      uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_max_sequential_skip_in_iterations(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_disable_auto_compactions(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_disable_auto_compactions(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_optimize_filters_for_hits(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_optimize_filters_for_hits(cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_delete_obsolete_files_period_micros(cabindb_options_t*,
                                                        uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_delete_obsolete_files_period_micros(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_prepare_for_bulk_load(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_memtable_vector_rep(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_memtable_prefix_bloom_size_ratio(
    cabindb_options_t*, double);
extern CABINDB_LIBRARY_API double
cabindb_options_get_memtable_prefix_bloom_size_ratio(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_max_compaction_bytes(
    cabindb_options_t*, uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_options_get_max_compaction_bytes(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_hash_skip_list_rep(
    cabindb_options_t*, size_t, int32_t, int32_t);
extern CABINDB_LIBRARY_API void cabindb_options_set_hash_link_list_rep(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API void cabindb_options_set_plain_table_factory(
    cabindb_options_t*, uint32_t, int, double, size_t);

extern CABINDB_LIBRARY_API void cabindb_options_set_min_level_to_compress(
    cabindb_options_t* opt, int level);

extern CABINDB_LIBRARY_API void cabindb_options_set_memtable_huge_page_size(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_memtable_huge_page_size(cabindb_options_t*);

extern CABINDB_LIBRARY_API void cabindb_options_set_max_successive_merges(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_max_successive_merges(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_bloom_locality(
    cabindb_options_t*, uint32_t);
extern CABINDB_LIBRARY_API uint32_t
cabindb_options_get_bloom_locality(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_inplace_update_support(
    cabindb_options_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_options_get_inplace_update_support(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_inplace_update_num_locks(
    cabindb_options_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_options_get_inplace_update_num_locks(cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_report_bg_io_stats(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_report_bg_io_stats(
    cabindb_options_t*);

enum {
  cabindb_tolerate_corrupted_tail_records_recovery = 0,
  cabindb_absolute_consistency_recovery = 1,
  cabindb_point_in_time_recovery = 2,
  cabindb_skip_any_corrupted_records_recovery = 3
};
extern CABINDB_LIBRARY_API void cabindb_options_set_wal_recovery_mode(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_wal_recovery_mode(
    cabindb_options_t*);

enum {
  cabindb_no_compression = 0,
  cabindb_snappy_compression = 1,
  cabindb_zlib_compression = 2,
  cabindb_bz2_compression = 3,
  cabindb_lz4_compression = 4,
  cabindb_lz4hc_compression = 5,
  cabindb_xpress_compression = 6,
  cabindb_zstd_compression = 7
};
extern CABINDB_LIBRARY_API void cabindb_options_set_compression(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_compression(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_bottommost_compression(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_bottommost_compression(
    cabindb_options_t*);

enum {
  cabindb_level_compaction = 0,
  cabindb_universal_compaction = 1,
  cabindb_fifo_compaction = 2
};
extern CABINDB_LIBRARY_API void cabindb_options_set_compaction_style(
    cabindb_options_t*, int);
extern CABINDB_LIBRARY_API int cabindb_options_get_compaction_style(
    cabindb_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_options_set_universal_compaction_options(
    cabindb_options_t*, cabindb_universal_compaction_options_t*);
extern CABINDB_LIBRARY_API void cabindb_options_set_fifo_compaction_options(
    cabindb_options_t* opt, cabindb_fifo_compaction_options_t* fifo);
extern CABINDB_LIBRARY_API void cabindb_options_set_ratelimiter(
    cabindb_options_t* opt, cabindb_ratelimiter_t* limiter);
extern CABINDB_LIBRARY_API void cabindb_options_set_atomic_flush(
    cabindb_options_t* opt, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_options_get_atomic_flush(
    cabindb_options_t* opt);

extern CABINDB_LIBRARY_API void cabindb_options_set_row_cache(
    cabindb_options_t* opt, cabindb_cache_t* cache
);

/* RateLimiter */
extern CABINDB_LIBRARY_API cabindb_ratelimiter_t* cabindb_ratelimiter_create(
    int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness);
extern CABINDB_LIBRARY_API void cabindb_ratelimiter_destroy(cabindb_ratelimiter_t*);

/* PerfContext */
enum {
  cabindb_uninitialized = 0,
  cabindb_disable = 1,
  cabindb_enable_count = 2,
  cabindb_enable_time_except_for_mutex = 3,
  cabindb_enable_time = 4,
  cabindb_out_of_bounds = 5
};

enum {
  cabindb_user_key_comparison_count = 0,
  cabindb_block_cache_hit_count,
  cabindb_block_read_count,
  cabindb_block_read_byte,
  cabindb_block_read_time,
  cabindb_block_checksum_time,
  cabindb_block_decompress_time,
  cabindb_get_read_bytes,
  cabindb_multiget_read_bytes,
  cabindb_iter_read_bytes,
  cabindb_internal_key_skipped_count,
  cabindb_internal_delete_skipped_count,
  cabindb_internal_recent_skipped_count,
  cabindb_internal_merge_count,
  cabindb_get_snapshot_time,
  cabindb_get_from_memtable_time,
  cabindb_get_from_memtable_count,
  cabindb_get_post_process_time,
  cabindb_get_from_output_files_time,
  cabindb_seek_on_memtable_time,
  cabindb_seek_on_memtable_count,
  cabindb_next_on_memtable_count,
  cabindb_prev_on_memtable_count,
  cabindb_seek_child_seek_time,
  cabindb_seek_child_seek_count,
  cabindb_seek_min_heap_time,
  cabindb_seek_max_heap_time,
  cabindb_seek_internal_seek_time,
  cabindb_find_next_user_entry_time,
  cabindb_write_wal_time,
  cabindb_write_memtable_time,
  cabindb_write_delay_time,
  cabindb_write_pre_and_post_process_time,
  cabindb_db_mutex_lock_nanos,
  cabindb_db_condition_wait_nanos,
  cabindb_merge_operator_time_nanos,
  cabindb_read_index_block_nanos,
  cabindb_read_filter_block_nanos,
  cabindb_new_table_block_iter_nanos,
  cabindb_new_table_iterator_nanos,
  cabindb_block_seek_nanos,
  cabindb_find_table_nanos,
  cabindb_bloom_memtable_hit_count,
  cabindb_bloom_memtable_miss_count,
  cabindb_bloom_sst_hit_count,
  cabindb_bloom_sst_miss_count,
  cabindb_key_lock_wait_time,
  cabindb_key_lock_wait_count,
  cabindb_env_new_sequential_file_nanos,
  cabindb_env_new_random_access_file_nanos,
  cabindb_env_new_writable_file_nanos,
  cabindb_env_reuse_writable_file_nanos,
  cabindb_env_new_random_rw_file_nanos,
  cabindb_env_new_directory_nanos,
  cabindb_env_file_exists_nanos,
  cabindb_env_get_children_nanos,
  cabindb_env_get_children_file_attributes_nanos,
  cabindb_env_delete_file_nanos,
  cabindb_env_create_dir_nanos,
  cabindb_env_create_dir_if_missing_nanos,
  cabindb_env_delete_dir_nanos,
  cabindb_env_get_file_size_nanos,
  cabindb_env_get_file_modification_time_nanos,
  cabindb_env_rename_file_nanos,
  cabindb_env_link_file_nanos,
  cabindb_env_lock_file_nanos,
  cabindb_env_unlock_file_nanos,
  cabindb_env_new_logger_nanos,
  cabindb_total_metric_count = 68
};

extern CABINDB_LIBRARY_API void cabindb_set_perf_level(int);
extern CABINDB_LIBRARY_API cabindb_perfcontext_t* cabindb_perfcontext_create();
extern CABINDB_LIBRARY_API void cabindb_perfcontext_reset(
    cabindb_perfcontext_t* context);
extern CABINDB_LIBRARY_API char* cabindb_perfcontext_report(
    cabindb_perfcontext_t* context, unsigned char exclude_zero_counters);
extern CABINDB_LIBRARY_API uint64_t cabindb_perfcontext_metric(
    cabindb_perfcontext_t* context, int metric);
extern CABINDB_LIBRARY_API void cabindb_perfcontext_destroy(
    cabindb_perfcontext_t* context);

/* Compaction Filter */

extern CABINDB_LIBRARY_API cabindb_compactionfilter_t*
cabindb_compactionfilter_create(
    void* state, void (*destructor)(void*),
    unsigned char (*filter)(void*, int level, const char* key,
                            size_t key_length, const char* existing_value,
                            size_t value_length, char** new_value,
                            size_t* new_value_length,
                            unsigned char* value_changed),
    const char* (*name)(void*));
extern CABINDB_LIBRARY_API void cabindb_compactionfilter_set_ignore_snapshots(
    cabindb_compactionfilter_t*, unsigned char);
extern CABINDB_LIBRARY_API void cabindb_compactionfilter_destroy(
    cabindb_compactionfilter_t*);

/* Compaction Filter Context */

extern CABINDB_LIBRARY_API unsigned char
cabindb_compactionfiltercontext_is_full_compaction(
    cabindb_compactionfiltercontext_t* context);

extern CABINDB_LIBRARY_API unsigned char
cabindb_compactionfiltercontext_is_manual_compaction(
    cabindb_compactionfiltercontext_t* context);

/* Compaction Filter Factory */

extern CABINDB_LIBRARY_API cabindb_compactionfilterfactory_t*
cabindb_compactionfilterfactory_create(
    void* state, void (*destructor)(void*),
    cabindb_compactionfilter_t* (*create_compaction_filter)(
        void*, cabindb_compactionfiltercontext_t* context),
    const char* (*name)(void*));
extern CABINDB_LIBRARY_API void cabindb_compactionfilterfactory_destroy(
    cabindb_compactionfilterfactory_t*);

/* Comparator */

extern CABINDB_LIBRARY_API cabindb_comparator_t* cabindb_comparator_create(
    void* state, void (*destructor)(void*),
    int (*compare)(void*, const char* a, size_t alen, const char* b,
                   size_t blen),
    const char* (*name)(void*));
extern CABINDB_LIBRARY_API void cabindb_comparator_destroy(
    cabindb_comparator_t*);

/* Filter policy */

extern CABINDB_LIBRARY_API cabindb_filterpolicy_t* cabindb_filterpolicy_create(
    void* state, void (*destructor)(void*),
    char* (*create_filter)(void*, const char* const* key_array,
                           const size_t* key_length_array, int num_keys,
                           size_t* filter_length),
    unsigned char (*key_may_match)(void*, const char* key, size_t length,
                                   const char* filter, size_t filter_length),
    void (*delete_filter)(void*, const char* filter, size_t filter_length),
    const char* (*name)(void*));
extern CABINDB_LIBRARY_API void cabindb_filterpolicy_destroy(
    cabindb_filterpolicy_t*);

extern CABINDB_LIBRARY_API cabindb_filterpolicy_t*
cabindb_filterpolicy_create_bloom(int bits_per_key);
extern CABINDB_LIBRARY_API cabindb_filterpolicy_t*
cabindb_filterpolicy_create_bloom_full(int bits_per_key);

/* Merge Operator */

extern CABINDB_LIBRARY_API cabindb_mergeoperator_t*
cabindb_mergeoperator_create(
    void* state, void (*destructor)(void*),
    char* (*full_merge)(void*, const char* key, size_t key_length,
                        const char* existing_value,
                        size_t existing_value_length,
                        const char* const* operands_list,
                        const size_t* operands_list_length, int num_operands,
                        unsigned char* success, size_t* new_value_length),
    char* (*partial_merge)(void*, const char* key, size_t key_length,
                           const char* const* operands_list,
                           const size_t* operands_list_length, int num_operands,
                           unsigned char* success, size_t* new_value_length),
    void (*delete_value)(void*, const char* value, size_t value_length),
    const char* (*name)(void*));
extern CABINDB_LIBRARY_API void cabindb_mergeoperator_destroy(
    cabindb_mergeoperator_t*);

/* Read options */

extern CABINDB_LIBRARY_API cabindb_readoptions_t* cabindb_readoptions_create();
extern CABINDB_LIBRARY_API void cabindb_readoptions_destroy(
    cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_verify_checksums(
    cabindb_readoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_readoptions_get_verify_checksums(cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_fill_cache(
    cabindb_readoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_readoptions_get_fill_cache(
    cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_snapshot(
    cabindb_readoptions_t*, const cabindb_snapshot_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_iterate_upper_bound(
    cabindb_readoptions_t*, const char* key, size_t keylen);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_iterate_lower_bound(
    cabindb_readoptions_t*, const char* key, size_t keylen);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_read_tier(
    cabindb_readoptions_t*, int);
extern CABINDB_LIBRARY_API int cabindb_readoptions_get_read_tier(
    cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_tailing(
    cabindb_readoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_readoptions_get_tailing(
    cabindb_readoptions_t*);
// The functionality that this option controlled has been removed.
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_managed(
    cabindb_readoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_readahead_size(
    cabindb_readoptions_t*, size_t);
extern CABINDB_LIBRARY_API size_t
cabindb_readoptions_get_readahead_size(cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_prefix_same_as_start(
    cabindb_readoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_readoptions_get_prefix_same_as_start(cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_pin_data(
    cabindb_readoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_readoptions_get_pin_data(
    cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_total_order_seek(
    cabindb_readoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_readoptions_get_total_order_seek(cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_max_skippable_internal_keys(
    cabindb_readoptions_t*, uint64_t);
extern CABINDB_LIBRARY_API uint64_t
cabindb_readoptions_get_max_skippable_internal_keys(cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_background_purge_on_iterator_cleanup(
    cabindb_readoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_readoptions_get_background_purge_on_iterator_cleanup(
    cabindb_readoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_readoptions_set_ignore_range_deletions(
    cabindb_readoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_readoptions_get_ignore_range_deletions(cabindb_readoptions_t*);

/* Write options */

extern CABINDB_LIBRARY_API cabindb_writeoptions_t*
cabindb_writeoptions_create();
extern CABINDB_LIBRARY_API void cabindb_writeoptions_destroy(
    cabindb_writeoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_writeoptions_set_sync(
    cabindb_writeoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_writeoptions_get_sync(
    cabindb_writeoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_writeoptions_disable_WAL(
    cabindb_writeoptions_t* opt, int disable);
extern CABINDB_LIBRARY_API unsigned char cabindb_writeoptions_get_disable_WAL(
    cabindb_writeoptions_t* opt);
extern CABINDB_LIBRARY_API void cabindb_writeoptions_set_ignore_missing_column_families(
    cabindb_writeoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_writeoptions_get_ignore_missing_column_families(
    cabindb_writeoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_writeoptions_set_no_slowdown(
    cabindb_writeoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_writeoptions_get_no_slowdown(
    cabindb_writeoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_writeoptions_set_low_pri(
    cabindb_writeoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_writeoptions_get_low_pri(
    cabindb_writeoptions_t*);
extern CABINDB_LIBRARY_API void
cabindb_writeoptions_set_memtable_insert_hint_per_batch(cabindb_writeoptions_t*,
                                                        unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_writeoptions_get_memtable_insert_hint_per_batch(
    cabindb_writeoptions_t*);

/* Compact range options */

extern CABINDB_LIBRARY_API cabindb_compactoptions_t*
cabindb_compactoptions_create();
extern CABINDB_LIBRARY_API void cabindb_compactoptions_destroy(
    cabindb_compactoptions_t*);
extern CABINDB_LIBRARY_API void
cabindb_compactoptions_set_exclusive_manual_compaction(
    cabindb_compactoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_compactoptions_get_exclusive_manual_compaction(
    cabindb_compactoptions_t*);
extern CABINDB_LIBRARY_API void
cabindb_compactoptions_set_bottommost_level_compaction(
    cabindb_compactoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_compactoptions_get_bottommost_level_compaction(
    cabindb_compactoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_compactoptions_set_change_level(
    cabindb_compactoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char
cabindb_compactoptions_get_change_level(cabindb_compactoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_compactoptions_set_target_level(
    cabindb_compactoptions_t*, int);
extern CABINDB_LIBRARY_API int cabindb_compactoptions_get_target_level(
    cabindb_compactoptions_t*);

/* Flush options */

extern CABINDB_LIBRARY_API cabindb_flushoptions_t*
cabindb_flushoptions_create();
extern CABINDB_LIBRARY_API void cabindb_flushoptions_destroy(
    cabindb_flushoptions_t*);
extern CABINDB_LIBRARY_API void cabindb_flushoptions_set_wait(
    cabindb_flushoptions_t*, unsigned char);
extern CABINDB_LIBRARY_API unsigned char cabindb_flushoptions_get_wait(
    cabindb_flushoptions_t*);

/* Cache */

extern CABINDB_LIBRARY_API cabindb_cache_t* cabindb_cache_create_lru(
    size_t capacity);
extern CABINDB_LIBRARY_API void cabindb_cache_destroy(cabindb_cache_t* cache);
extern CABINDB_LIBRARY_API void cabindb_cache_set_capacity(
    cabindb_cache_t* cache, size_t capacity);
extern CABINDB_LIBRARY_API size_t
cabindb_cache_get_capacity(cabindb_cache_t* cache);
extern CABINDB_LIBRARY_API size_t
cabindb_cache_get_usage(cabindb_cache_t* cache);
extern CABINDB_LIBRARY_API size_t
cabindb_cache_get_pinned_usage(cabindb_cache_t* cache);

/* DBPath */

extern CABINDB_LIBRARY_API cabindb_dbpath_t* cabindb_dbpath_create(const char* path, uint64_t target_size);
extern CABINDB_LIBRARY_API void cabindb_dbpath_destroy(cabindb_dbpath_t*);

/* Env */

extern CABINDB_LIBRARY_API cabindb_env_t* cabindb_create_default_env();
extern CABINDB_LIBRARY_API cabindb_env_t* cabindb_create_mem_env();
extern CABINDB_LIBRARY_API void cabindb_env_set_background_threads(
    cabindb_env_t* env, int n);
extern CABINDB_LIBRARY_API int cabindb_env_get_background_threads(
    cabindb_env_t* env);
extern CABINDB_LIBRARY_API void
cabindb_env_set_high_priority_background_threads(cabindb_env_t* env, int n);
extern CABINDB_LIBRARY_API int cabindb_env_get_high_priority_background_threads(
    cabindb_env_t* env);
extern CABINDB_LIBRARY_API void cabindb_env_set_low_priority_background_threads(
    cabindb_env_t* env, int n);
extern CABINDB_LIBRARY_API int cabindb_env_get_low_priority_background_threads(
    cabindb_env_t* env);
extern CABINDB_LIBRARY_API void
cabindb_env_set_bottom_priority_background_threads(cabindb_env_t* env, int n);
extern CABINDB_LIBRARY_API int
cabindb_env_get_bottom_priority_background_threads(cabindb_env_t* env);
extern CABINDB_LIBRARY_API void cabindb_env_join_all_threads(
    cabindb_env_t* env);
extern CABINDB_LIBRARY_API void cabindb_env_lower_thread_pool_io_priority(cabindb_env_t* env);
extern CABINDB_LIBRARY_API void cabindb_env_lower_high_priority_thread_pool_io_priority(cabindb_env_t* env);
extern CABINDB_LIBRARY_API void cabindb_env_lower_thread_pool_cpu_priority(cabindb_env_t* env);
extern CABINDB_LIBRARY_API void cabindb_env_lower_high_priority_thread_pool_cpu_priority(cabindb_env_t* env);

extern CABINDB_LIBRARY_API void cabindb_env_destroy(cabindb_env_t*);

extern CABINDB_LIBRARY_API cabindb_envoptions_t* cabindb_envoptions_create();
extern CABINDB_LIBRARY_API void cabindb_envoptions_destroy(
    cabindb_envoptions_t* opt);

/* SstFile */

extern CABINDB_LIBRARY_API cabindb_sstfilewriter_t*
cabindb_sstfilewriter_create(const cabindb_envoptions_t* env,
                             const cabindb_options_t* io_options);
extern CABINDB_LIBRARY_API cabindb_sstfilewriter_t*
cabindb_sstfilewriter_create_with_comparator(
    const cabindb_envoptions_t* env, const cabindb_options_t* io_options,
    const cabindb_comparator_t* comparator);
extern CABINDB_LIBRARY_API void cabindb_sstfilewriter_open(
    cabindb_sstfilewriter_t* writer, const char* name, char** errptr);
extern CABINDB_LIBRARY_API void cabindb_sstfilewriter_add(
    cabindb_sstfilewriter_t* writer, const char* key, size_t keylen,
    const char* val, size_t vallen, char** errptr);
extern CABINDB_LIBRARY_API void cabindb_sstfilewriter_put(
    cabindb_sstfilewriter_t* writer, const char* key, size_t keylen,
    const char* val, size_t vallen, char** errptr);
extern CABINDB_LIBRARY_API void cabindb_sstfilewriter_merge(
    cabindb_sstfilewriter_t* writer, const char* key, size_t keylen,
    const char* val, size_t vallen, char** errptr);
extern CABINDB_LIBRARY_API void cabindb_sstfilewriter_delete(
    cabindb_sstfilewriter_t* writer, const char* key, size_t keylen,
    char** errptr);
extern CABINDB_LIBRARY_API void cabindb_sstfilewriter_finish(
    cabindb_sstfilewriter_t* writer, char** errptr);
extern CABINDB_LIBRARY_API void cabindb_sstfilewriter_file_size(
    cabindb_sstfilewriter_t* writer, uint64_t* file_size);
extern CABINDB_LIBRARY_API void cabindb_sstfilewriter_destroy(
    cabindb_sstfilewriter_t* writer);

extern CABINDB_LIBRARY_API cabindb_ingestexternalfileoptions_t*
cabindb_ingestexternalfileoptions_create();
extern CABINDB_LIBRARY_API void
cabindb_ingestexternalfileoptions_set_move_files(
    cabindb_ingestexternalfileoptions_t* opt, unsigned char move_files);
extern CABINDB_LIBRARY_API void
cabindb_ingestexternalfileoptions_set_snapshot_consistency(
    cabindb_ingestexternalfileoptions_t* opt,
    unsigned char snapshot_consistency);
extern CABINDB_LIBRARY_API void
cabindb_ingestexternalfileoptions_set_allow_global_seqno(
    cabindb_ingestexternalfileoptions_t* opt, unsigned char allow_global_seqno);
extern CABINDB_LIBRARY_API void
cabindb_ingestexternalfileoptions_set_allow_blocking_flush(
    cabindb_ingestexternalfileoptions_t* opt,
    unsigned char allow_blocking_flush);
extern CABINDB_LIBRARY_API void
cabindb_ingestexternalfileoptions_set_ingest_behind(
    cabindb_ingestexternalfileoptions_t* opt,
    unsigned char ingest_behind);
extern CABINDB_LIBRARY_API void cabindb_ingestexternalfileoptions_destroy(
    cabindb_ingestexternalfileoptions_t* opt);

extern CABINDB_LIBRARY_API void cabindb_ingest_external_file(
    cabindb_t* db, const char* const* file_list, const size_t list_len,
    const cabindb_ingestexternalfileoptions_t* opt, char** errptr);
extern CABINDB_LIBRARY_API void cabindb_ingest_external_file_cf(
    cabindb_t* db, cabindb_column_family_handle_t* handle,
    const char* const* file_list, const size_t list_len,
    const cabindb_ingestexternalfileoptions_t* opt, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_try_catch_up_with_primary(
    cabindb_t* db, char** errptr);

/* SliceTransform */

extern CABINDB_LIBRARY_API cabindb_slicetransform_t*
cabindb_slicetransform_create(
    void* state, void (*destructor)(void*),
    char* (*transform)(void*, const char* key, size_t length,
                       size_t* dst_length),
    unsigned char (*in_domain)(void*, const char* key, size_t length),
    unsigned char (*in_range)(void*, const char* key, size_t length),
    const char* (*name)(void*));
extern CABINDB_LIBRARY_API cabindb_slicetransform_t*
    cabindb_slicetransform_create_fixed_prefix(size_t);
extern CABINDB_LIBRARY_API cabindb_slicetransform_t*
cabindb_slicetransform_create_noop();
extern CABINDB_LIBRARY_API void cabindb_slicetransform_destroy(
    cabindb_slicetransform_t*);

/* Universal Compaction options */

enum {
  cabindb_similar_size_compaction_stop_style = 0,
  cabindb_total_size_compaction_stop_style = 1
};

extern CABINDB_LIBRARY_API cabindb_universal_compaction_options_t*
cabindb_universal_compaction_options_create();
extern CABINDB_LIBRARY_API void
cabindb_universal_compaction_options_set_size_ratio(
    cabindb_universal_compaction_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_universal_compaction_options_get_size_ratio(
    cabindb_universal_compaction_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_universal_compaction_options_set_min_merge_width(
    cabindb_universal_compaction_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_universal_compaction_options_get_min_merge_width(
    cabindb_universal_compaction_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_universal_compaction_options_set_max_merge_width(
    cabindb_universal_compaction_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_universal_compaction_options_get_max_merge_width(
    cabindb_universal_compaction_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_universal_compaction_options_set_max_size_amplification_percent(
    cabindb_universal_compaction_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_universal_compaction_options_get_max_size_amplification_percent(
    cabindb_universal_compaction_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_universal_compaction_options_set_compression_size_percent(
    cabindb_universal_compaction_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_universal_compaction_options_get_compression_size_percent(
    cabindb_universal_compaction_options_t*);
extern CABINDB_LIBRARY_API void
cabindb_universal_compaction_options_set_stop_style(
    cabindb_universal_compaction_options_t*, int);
extern CABINDB_LIBRARY_API int
cabindb_universal_compaction_options_get_stop_style(
    cabindb_universal_compaction_options_t*);
extern CABINDB_LIBRARY_API void cabindb_universal_compaction_options_destroy(
    cabindb_universal_compaction_options_t*);

extern CABINDB_LIBRARY_API cabindb_fifo_compaction_options_t*
cabindb_fifo_compaction_options_create();
extern CABINDB_LIBRARY_API void
cabindb_fifo_compaction_options_set_max_table_files_size(
    cabindb_fifo_compaction_options_t* fifo_opts, uint64_t size);
extern CABINDB_LIBRARY_API uint64_t
cabindb_fifo_compaction_options_get_max_table_files_size(
    cabindb_fifo_compaction_options_t* fifo_opts);
extern CABINDB_LIBRARY_API void cabindb_fifo_compaction_options_destroy(
    cabindb_fifo_compaction_options_t* fifo_opts);

extern CABINDB_LIBRARY_API int cabindb_livefiles_count(
    const cabindb_livefiles_t*);
extern CABINDB_LIBRARY_API const char* cabindb_livefiles_name(
    const cabindb_livefiles_t*, int index);
extern CABINDB_LIBRARY_API int cabindb_livefiles_level(
    const cabindb_livefiles_t*, int index);
extern CABINDB_LIBRARY_API size_t
cabindb_livefiles_size(const cabindb_livefiles_t*, int index);
extern CABINDB_LIBRARY_API const char* cabindb_livefiles_smallestkey(
    const cabindb_livefiles_t*, int index, size_t* size);
extern CABINDB_LIBRARY_API const char* cabindb_livefiles_largestkey(
    const cabindb_livefiles_t*, int index, size_t* size);
extern CABINDB_LIBRARY_API uint64_t cabindb_livefiles_entries(
    const cabindb_livefiles_t*, int index);
extern CABINDB_LIBRARY_API uint64_t cabindb_livefiles_deletions(
    const cabindb_livefiles_t*, int index);
extern CABINDB_LIBRARY_API void cabindb_livefiles_destroy(
    const cabindb_livefiles_t*);

/* Utility Helpers */

extern CABINDB_LIBRARY_API void cabindb_get_options_from_string(
    const cabindb_options_t* base_options, const char* opts_str,
    cabindb_options_t* new_options, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_delete_file_in_range(
    cabindb_t* db, const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_delete_file_in_range_cf(
    cabindb_t* db, cabindb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len, char** errptr);

/* Transactions */

extern CABINDB_LIBRARY_API cabindb_column_family_handle_t*
cabindb_transactiondb_create_column_family(
    cabindb_transactiondb_t* txn_db,
    const cabindb_options_t* column_family_options,
    const char* column_family_name, char** errptr);

extern CABINDB_LIBRARY_API cabindb_transactiondb_t* cabindb_transactiondb_open(
    const cabindb_options_t* options,
    const cabindb_transactiondb_options_t* txn_db_options, const char* name,
    char** errptr);

cabindb_transactiondb_t* cabindb_transactiondb_open_column_families(
    const cabindb_options_t* options,
    const cabindb_transactiondb_options_t* txn_db_options, const char* name,
    int num_column_families, const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles, char** errptr);

extern CABINDB_LIBRARY_API const cabindb_snapshot_t*
cabindb_transactiondb_create_snapshot(cabindb_transactiondb_t* txn_db);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_release_snapshot(
    cabindb_transactiondb_t* txn_db, const cabindb_snapshot_t* snapshot);

extern CABINDB_LIBRARY_API cabindb_transaction_t* cabindb_transaction_begin(
    cabindb_transactiondb_t* txn_db,
    const cabindb_writeoptions_t* write_options,
    const cabindb_transaction_options_t* txn_options,
    cabindb_transaction_t* old_txn);

extern CABINDB_LIBRARY_API void cabindb_transaction_commit(
    cabindb_transaction_t* txn, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transaction_rollback(
    cabindb_transaction_t* txn, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transaction_set_savepoint(
    cabindb_transaction_t* txn);

extern CABINDB_LIBRARY_API void cabindb_transaction_rollback_to_savepoint(
    cabindb_transaction_t* txn, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transaction_destroy(
    cabindb_transaction_t* txn);

// This snapshot should be freed using cabindb_free
extern CABINDB_LIBRARY_API const cabindb_snapshot_t*
cabindb_transaction_get_snapshot(cabindb_transaction_t* txn);

extern CABINDB_LIBRARY_API char* cabindb_transaction_get(
    cabindb_transaction_t* txn, const cabindb_readoptions_t* options,
    const char* key, size_t klen, size_t* vlen, char** errptr);

extern CABINDB_LIBRARY_API char* cabindb_transaction_get_cf(
    cabindb_transaction_t* txn, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key, size_t klen,
    size_t* vlen, char** errptr);

extern CABINDB_LIBRARY_API char* cabindb_transaction_get_for_update(
    cabindb_transaction_t* txn, const cabindb_readoptions_t* options,
    const char* key, size_t klen, size_t* vlen, unsigned char exclusive,
    char** errptr);

char* cabindb_transaction_get_for_update_cf(
    cabindb_transaction_t* txn, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key, size_t klen,
    size_t* vlen, unsigned char exclusive, char** errptr);

extern CABINDB_LIBRARY_API char* cabindb_transactiondb_get(
    cabindb_transactiondb_t* txn_db, const cabindb_readoptions_t* options,
    const char* key, size_t klen, size_t* vlen, char** errptr);

extern CABINDB_LIBRARY_API char* cabindb_transactiondb_get_cf(
    cabindb_transactiondb_t* txn_db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transaction_put(
    cabindb_transaction_t* txn, const char* key, size_t klen, const char* val,
    size_t vlen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transaction_put_cf(
    cabindb_transaction_t* txn, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen, const char* val, size_t vlen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_put(
    cabindb_transactiondb_t* txn_db, const cabindb_writeoptions_t* options,
    const char* key, size_t klen, const char* val, size_t vlen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_put_cf(
    cabindb_transactiondb_t* txn_db, const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, const char* val, size_t vallen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_write(
    cabindb_transactiondb_t* txn_db, const cabindb_writeoptions_t* options,
    cabindb_writebatch_t *batch, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transaction_merge(
    cabindb_transaction_t* txn, const char* key, size_t klen, const char* val,
    size_t vlen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transaction_merge_cf(
    cabindb_transaction_t* txn, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen, const char* val, size_t vlen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_merge(
    cabindb_transactiondb_t* txn_db, const cabindb_writeoptions_t* options,
    const char* key, size_t klen, const char* val, size_t vlen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_merge_cf(
    cabindb_transactiondb_t* txn_db, const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key, size_t klen,
    const char* val, size_t vlen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transaction_delete(
    cabindb_transaction_t* txn, const char* key, size_t klen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transaction_delete_cf(
    cabindb_transaction_t* txn, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_delete(
    cabindb_transactiondb_t* txn_db, const cabindb_writeoptions_t* options,
    const char* key, size_t klen, char** errptr);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_delete_cf(
    cabindb_transactiondb_t* txn_db, const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr);

extern CABINDB_LIBRARY_API cabindb_iterator_t*
cabindb_transaction_create_iterator(cabindb_transaction_t* txn,
                                    const cabindb_readoptions_t* options);

extern CABINDB_LIBRARY_API cabindb_iterator_t*
cabindb_transaction_create_iterator_cf(
    cabindb_transaction_t* txn, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family);

extern CABINDB_LIBRARY_API cabindb_iterator_t*
cabindb_transactiondb_create_iterator(cabindb_transactiondb_t* txn_db,
                                      const cabindb_readoptions_t* options);

extern CABINDB_LIBRARY_API cabindb_iterator_t*
cabindb_transactiondb_create_iterator_cf(
    cabindb_transactiondb_t* txn_db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_close(
    cabindb_transactiondb_t* txn_db);

extern CABINDB_LIBRARY_API cabindb_checkpoint_t*
cabindb_transactiondb_checkpoint_object_create(cabindb_transactiondb_t* txn_db,
                                               char** errptr);

extern CABINDB_LIBRARY_API cabindb_optimistictransactiondb_t*
cabindb_optimistictransactiondb_open(const cabindb_options_t* options,
                                     const char* name, char** errptr);

extern CABINDB_LIBRARY_API cabindb_optimistictransactiondb_t*
cabindb_optimistictransactiondb_open_column_families(
    const cabindb_options_t* options, const char* name, int num_column_families,
    const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles, char** errptr);

extern CABINDB_LIBRARY_API cabindb_t*
cabindb_optimistictransactiondb_get_base_db(
    cabindb_optimistictransactiondb_t* otxn_db);

extern CABINDB_LIBRARY_API void cabindb_optimistictransactiondb_close_base_db(
    cabindb_t* base_db);

extern CABINDB_LIBRARY_API cabindb_transaction_t*
cabindb_optimistictransaction_begin(
    cabindb_optimistictransactiondb_t* otxn_db,
    const cabindb_writeoptions_t* write_options,
    const cabindb_optimistictransaction_options_t* otxn_options,
    cabindb_transaction_t* old_txn);

extern CABINDB_LIBRARY_API void cabindb_optimistictransactiondb_close(
    cabindb_optimistictransactiondb_t* otxn_db);

/* Transaction Options */

extern CABINDB_LIBRARY_API cabindb_transactiondb_options_t*
cabindb_transactiondb_options_create();

extern CABINDB_LIBRARY_API void cabindb_transactiondb_options_destroy(
    cabindb_transactiondb_options_t* opt);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_options_set_max_num_locks(
    cabindb_transactiondb_options_t* opt, int64_t max_num_locks);

extern CABINDB_LIBRARY_API void cabindb_transactiondb_options_set_num_stripes(
    cabindb_transactiondb_options_t* opt, size_t num_stripes);

extern CABINDB_LIBRARY_API void
cabindb_transactiondb_options_set_transaction_lock_timeout(
    cabindb_transactiondb_options_t* opt, int64_t txn_lock_timeout);

extern CABINDB_LIBRARY_API void
cabindb_transactiondb_options_set_default_lock_timeout(
    cabindb_transactiondb_options_t* opt, int64_t default_lock_timeout);

extern CABINDB_LIBRARY_API cabindb_transaction_options_t*
cabindb_transaction_options_create();

extern CABINDB_LIBRARY_API void cabindb_transaction_options_destroy(
    cabindb_transaction_options_t* opt);

extern CABINDB_LIBRARY_API void cabindb_transaction_options_set_set_snapshot(
    cabindb_transaction_options_t* opt, unsigned char v);

extern CABINDB_LIBRARY_API void cabindb_transaction_options_set_deadlock_detect(
    cabindb_transaction_options_t* opt, unsigned char v);

extern CABINDB_LIBRARY_API void cabindb_transaction_options_set_lock_timeout(
    cabindb_transaction_options_t* opt, int64_t lock_timeout);

extern CABINDB_LIBRARY_API void cabindb_transaction_options_set_expiration(
    cabindb_transaction_options_t* opt, int64_t expiration);

extern CABINDB_LIBRARY_API void
cabindb_transaction_options_set_deadlock_detect_depth(
    cabindb_transaction_options_t* opt, int64_t depth);

extern CABINDB_LIBRARY_API void
cabindb_transaction_options_set_max_write_batch_size(
    cabindb_transaction_options_t* opt, size_t size);

extern CABINDB_LIBRARY_API cabindb_optimistictransaction_options_t*
cabindb_optimistictransaction_options_create();

extern CABINDB_LIBRARY_API void cabindb_optimistictransaction_options_destroy(
    cabindb_optimistictransaction_options_t* opt);

extern CABINDB_LIBRARY_API void
cabindb_optimistictransaction_options_set_set_snapshot(
    cabindb_optimistictransaction_options_t* opt, unsigned char v);

// referring to convention (3), this should be used by client
// to free memory that was malloc()ed
extern CABINDB_LIBRARY_API void cabindb_free(void* ptr);

extern CABINDB_LIBRARY_API cabindb_pinnableslice_t* cabindb_get_pinned(
    cabindb_t* db, const cabindb_readoptions_t* options, const char* key,
    size_t keylen, char** errptr);
extern CABINDB_LIBRARY_API cabindb_pinnableslice_t* cabindb_get_pinned_cf(
    cabindb_t* db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr);
extern CABINDB_LIBRARY_API void cabindb_pinnableslice_destroy(
    cabindb_pinnableslice_t* v);
extern CABINDB_LIBRARY_API const char* cabindb_pinnableslice_value(
    const cabindb_pinnableslice_t* t, size_t* vlen);

extern CABINDB_LIBRARY_API cabindb_memory_consumers_t*
    cabindb_memory_consumers_create();
extern CABINDB_LIBRARY_API void cabindb_memory_consumers_add_db(
    cabindb_memory_consumers_t* consumers, cabindb_t* db);
extern CABINDB_LIBRARY_API void cabindb_memory_consumers_add_cache(
    cabindb_memory_consumers_t* consumers, cabindb_cache_t* cache);
extern CABINDB_LIBRARY_API void cabindb_memory_consumers_destroy(
    cabindb_memory_consumers_t* consumers);
extern CABINDB_LIBRARY_API cabindb_memory_usage_t*
cabindb_approximate_memory_usage_create(cabindb_memory_consumers_t* consumers,
                                       char** errptr);
extern CABINDB_LIBRARY_API void cabindb_approximate_memory_usage_destroy(
    cabindb_memory_usage_t* usage);

extern CABINDB_LIBRARY_API uint64_t
cabindb_approximate_memory_usage_get_mem_table_total(
    cabindb_memory_usage_t* memory_usage);
extern CABINDB_LIBRARY_API uint64_t
cabindb_approximate_memory_usage_get_mem_table_unflushed(
    cabindb_memory_usage_t* memory_usage);
extern CABINDB_LIBRARY_API uint64_t
cabindb_approximate_memory_usage_get_mem_table_readers_total(
    cabindb_memory_usage_t* memory_usage);
extern CABINDB_LIBRARY_API uint64_t
cabindb_approximate_memory_usage_get_cache_total(
    cabindb_memory_usage_t* memory_usage);

extern CABINDB_LIBRARY_API void cabindb_options_set_dump_malloc_stats(
    cabindb_options_t*, unsigned char);

extern CABINDB_LIBRARY_API void
cabindb_options_set_memtable_whole_key_filtering(cabindb_options_t*,
                                                 unsigned char);

extern CABINDB_LIBRARY_API void cabindb_cancel_all_background_work(
    cabindb_t* db, unsigned char wait);

#ifdef __cplusplus
}  /* end extern "C" */
#endif
