#include <openssl/md5.h>

#include <memory>

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_bloomfilter.h"

int lsm_bloomfilter_insert(std::vector<bool>& bloomfilter_store, const std::string& object)
{
    std::unique_ptr<unsigned char[]> MD5_hash_result_buffer = std::make_unique<unsigned char[]>(MD5_RESULT_SIZE_BYTES);

	lsm_bloomfilter_hash(object, &MD5_hash_result_buffer);
	const uint16_t* const object_hashes = reinterpret_cast<const uint16_t * const>(MD5_hash_result_buffer.get());

	for (size_t i = 0; i < HASH_FUNCTION_COUNT; i++)
	{
		const uint16_t index_to_set = object_hashes[i];
		bloomfilter_store[index_to_set] = true;
	}

    return 0;
}

int lsm_bloomfilter_clear(std::vector<bool>& bloomfilter_store)
{
	bloomfilter_store.clear();

    return 0;
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

int lsm_bloomfilter_hash(const std::string& val, std::unique_ptr<unsigned char[]>* MD5_hash_result_buffer)
{
	const unsigned char* const md5_input_val = reinterpret_cast<const unsigned char*>(val.data());
	const size_t md5_input_length = val.length();
	MD5(md5_input_val, md5_input_length, (*MD5_hash_result_buffer).get());

    return 0;
}
