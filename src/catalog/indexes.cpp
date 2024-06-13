#include "catalog/indexes.h"

IndexMetadata::IndexMetadata(const index_id_t index_id, const std::string& index_name, const table_id_t table_id,
	const std::vector<uint32_t>& key_map)
	: index_id_(index_id), index_name_(index_name), table_id_(table_id), key_map_(key_map) {}

IndexMetadata* IndexMetadata::Create(const index_id_t index_id, const string& index_name, const table_id_t table_id,
	const vector<uint32_t>& key_map) {
	return new IndexMetadata(index_id, index_name, table_id, key_map);
}

/**
 * @brief: the IndexMetadata object to a character buffer.
 * @param: buf A pointer to the character buffer where the serialized data will be written.
 * @return: The size of the serialized data in bytes.
 * @throws: An assertion error if the serialized size exceeds the page size.
 * @note: This function serializes the IndexMetadata object by writing its member variables to the character buffer.
 * @note: The serialized data includes the magic number, index ID, index name, table ID, key count, and key mapping in the table.
 * @note: The serialized data can be used to store the IndexMetadata object in a file or send it over a network.
 *
 * Example usage:
 * \code{cpp}
 * char buffer[PAGE_SIZE];
 * IndexMetadata metadata;
 * uint32_t serializedSize = metadata.SerializeTo(buffer);
 * \endcode
 */
uint32_t IndexMetadata::SerializeTo(char* buf) const {
	char* p = buf;
	uint32_t ofs = GetSerializedSize();
	ASSERT(ofs <= PAGE_SIZE, "Failed to serialize index info.");
	// magic num
	MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM);
	buf += 4;
	// index id
	MACH_WRITE_TO(index_id_t, buf, index_id_);
	buf += 4;
	// index name
	MACH_WRITE_UINT32(buf, index_name_.length());
	buf += 4;
	MACH_WRITE_STRING(buf, index_name_);
	buf += index_name_.length();
	// table id
	MACH_WRITE_TO(table_id_t, buf, table_id_);
	buf += 4;
	// key count
	MACH_WRITE_UINT32(buf, key_map_.size());
	buf += 4;
	// key mapping in table
	for (auto& col_index : key_map_) {
		MACH_WRITE_UINT32(buf, col_index);
		buf += 4;
	}
	ASSERT(buf - p == ofs, "Unexpected serialize size.");
	return ofs;
}

/**
 * TODO: Student Implement
 */
uint32_t IndexMetadata::GetSerializedSize() const {
	return
		4						// magic num
		+ 4						// index id
		+ 4						// index name length
		+ index_name_.length()	// index name
		+ 4						// table id
		+ 4						// key count
		+ key_map_.size() * sizeof(uint32_t);	// key mapping in table
}


/**
 * @brief Deserializes the index metadata from a buffer and creates an IndexMetadata object.
 *
 * This function takes a buffer containing serialized index metadata and creates an IndexMetadata object
 * by extracting the relevant information from the buffer. The deserialized index metadata includes the
 * magic number, index ID, index name, table ID, index key count, and key mapping in the table.
 *
 * @param buf A pointer to the buffer containing the serialized index metadata.
 * @param index_meta A reference to a pointer to IndexMetadata object. This pointer will be updated to
 *                   point to the newly created IndexMetadata object.
 * @return The number of bytes read from the buffer during deserialization.
 */
uint32_t IndexMetadata::DeserializeFrom(char* buf, IndexMetadata*& index_meta) {
	if (index_meta != nullptr) {
		LOG(WARNING) << "Pointer object index info is not null in table info deserialize." << std::endl;
	}
	char* p = buf;
	// magic num
	uint32_t magic_num = MACH_READ_UINT32(buf);
	buf += 4;
	ASSERT(magic_num == INDEX_METADATA_MAGIC_NUM, "Failed to deserialize index info.");
	// index id
	index_id_t index_id = MACH_READ_FROM(index_id_t, buf);
	buf += 4;
	// index name
	uint32_t len = MACH_READ_UINT32(buf);
	buf += 4;
	std::string index_name(buf, len);
	buf += len;
	// table id
	table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
	buf += 4;
	// index key count
	uint32_t index_key_count = MACH_READ_UINT32(buf);
	buf += 4;
	// key mapping in table
	std::vector<uint32_t> key_map;
	for (uint32_t i = 0; i < index_key_count; i++) {
		uint32_t key_index = MACH_READ_UINT32(buf);
		buf += 4;
		key_map.push_back(key_index);
	}
	// allocate space for index meta data
	index_meta = new IndexMetadata(index_id, index_name, table_id, key_map);
	return buf - p;
}

/**
 * @brief Creates an index based on the given index type.
 *
 * This function creates an index based on the specified index type. It calculates the maximum size required for the index based on the key schema and column types. The maximum size is determined by the number and types of columns in the key schema. If the index type is "bptree", the maximum size is adjusted to fit within certain limits. Finally, a new BPlusTreeIndex object is created with the specified metadata, key schema, maximum size, and buffer pool manager.
 *
 * @param buffer_pool_manager A pointer to the buffer pool manager.
 * @param index_type The type of index to create.
 * @return A pointer to the created index, or nullptr if the index type is not supported or the maximum size is too large.
 */
Index* IndexInfo::CreateIndex(BufferPoolManager* buffer_pool_manager, const string& index_type) {
	size_t max_size = 0;
	uint32_t column_cnt = key_schema_->GetColumns().size();
	size_t size_bitmap = (column_cnt % 8) ? column_cnt / 8 + 1 : column_cnt / 8;
	// column_cnt + bitmap
	max_size += 4 + sizeof(unsigned char) * size_bitmap;
	for (auto col : key_schema_->GetColumns()) {
		// length of char column
		if (col->GetType() == TypeId::kTypeChar)
			max_size += 4;
		max_size += col->GetLength();
	}

	if (index_type == "bptree") {
		if (max_size <= 8) max_size = 16;
		else if (max_size <= 24) max_size = 32;
		else if (max_size <= 56) max_size = 64;
		else if (max_size <= 120) max_size = 128;
		else if (max_size <= 248) max_size = 256;
		else {
			LOG(ERROR) << "GenericKey size is too large";
			return nullptr;
		}
	}
	else {
		return nullptr;
	}
	return new BPlusTreeIndex(meta_data_->index_id_, key_schema_, max_size, buffer_pool_manager);
}
