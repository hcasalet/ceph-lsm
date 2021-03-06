#include <openssl/md5.h>

#include <memory>

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_bloomfilter.h"

void lsm_bloomfilter_insert(std::vector<bool>& bloomfilter_store, const std::string& object)
{
    std::unique_ptr<unsigned char[]> MD5_hash_result_buffer = std::make_unique<unsigned char[]>(MD5_RESULT_SIZE_BYTES);

	lsm_bloomfilter_hash(object, &MD5_hash_result_buffer);
	const uint16_t* const object_hashes = reinterpret_cast<const uint16_t * const>(MD5_hash_result_buffer.get());

	for (size_t i = 0; i < HASH_FUNCTION_COUNT; i++)
	{
		const uint16_t index_to_set = object_hashes[i];
		bloomfilter_store[index_to_set] = true;
	}
}

void lsm_bloomfilter_insertAll(std::vector<bool>& bloomfilter_store, std::set<uint64_t>& keys)
{
	for (auto key : keys) {
		const std::string object = to_string(key);
		std::unique_ptr<unsigned char[]> MD5_hash_result_buffer = std::make_unique<unsigned char[]>(MD5_RESULT_SIZE_BYTES);

		lsm_bloomfilter_hash(object, &MD5_hash_result_buffer);
		const uint16_t* const object_hashes = reinterpret_cast<const uint16_t * const>(MD5_hash_result_buffer.get());

		for (size_t i = 0; i < HASH_FUNCTION_COUNT; i++)
		{
			const uint16_t index_to_set = object_hashes[i];
			bloomfilter_store[index_to_set] = true;
		}
	}
    
}

void lsm_bloomfilter_clear(std::vector<bool>& bloomfilter_store)
{
	std::fill(bloomfilter_store.begin(), bloomfilter_store.end(), 0);
}

void lsm_bloomfilter_clearall(std::vector<std::vector<bool> >& bloomfilter_stores)
{
	for (uint64_t i = 0; i < bloomfilter_stores.size(); i++) {
		std::fill(bloomfilter_stores[i].begin(), bloomfilter_stores[i].end(), 0);
	}
	
}

void lsm_bloomfilter_copy(std::vector<bool>& bloomfilter_store1, std::vector<bool>& bloomfilter_store2)
{
	for (uint64_t i = 0; i < bloomfilter_store2.size(); i++) {
		bloomfilter_store1[i] = bloomfilter_store2[i];
	}
}

void lsm_bloomfilter_compact(std::vector<std::vector<bool> >& bloomfilter_store_srcs, std::vector<bool>& bloomfilter_store_dest)
{
	lsm_bloomfilter_clear(bloomfilter_store_dest);

	for (uint64_t i = 0; i < bloomfilter_store_srcs.size(); i++) {
		for (uint64_t j = 0; j < bloomfilter_store_dest.size(); j++) {
			if (bloomfilter_store_srcs[i][j] == true) {
				bloomfilter_store_dest[j] = true;
			}
		}
	}
}

bool lsm_bloomfilter_contains(std::vector<bool>& bloomfilter_store, const std::string& object)
{
    std::unique_ptr<unsigned char[]> MD5_hash_result_buffer = std::make_unique<unsigned char[]>(MD5_RESULT_SIZE_BYTES);
	
	lsm_bloomfilter_hash(object, &MD5_hash_result_buffer);
	const uint16_t* const object_hashes = reinterpret_cast<const uint16_t * const>(MD5_hash_result_buffer.get());		

	for (size_t i = 0; i < HASH_FUNCTION_COUNT; i++)
	{
		const uint16_t index_to_get = object_hashes[i];
		if (!bloomfilter_store[index_to_get]) return false;
	}
	return true;
}

void lsm_bloomfilter_hash(const std::string& val, std::unique_ptr<unsigned char[]>* MD5_hash_result_buffer)
{
	const unsigned char* const md5_input_val = reinterpret_cast<const unsigned char*>(val.data());
	const size_t md5_input_length = val.length();
	MD5(md5_input_val, md5_input_length, (*MD5_hash_result_buffer).get());
}
