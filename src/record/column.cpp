#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
	: name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
	ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
	switch (type) {
		case TypeId::kTypeInt:
			len_ = sizeof(int32_t);
			break;
		case TypeId::kTypeFloat:
			len_ = sizeof(float_t);
			break;
		default:
			ASSERT(false, "Unsupported column type.");
	}
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
	: name_(std::move(column_name)), type_(type), len_(length), table_ind_(index), nullable_(nullable), unique_(unique) {
	ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column* other)
	: name_(other->name_), type_(other->type_), len_(other->len_), table_ind_(other->table_ind_),
	nullable_(other->nullable_), unique_(other->unique_) {}

/**
* TODO: Student Implement (finished)
* @brief: Serialize the column to a buffer
* @param buf: the buffer to serialize to
* @return: the number of bytes written to the buffer
*/
uint32_t Column::SerializeTo(char* buf) const {
	// replace with your code here
	uint32_t res = 0, len = name_.length();
	memcpy(buf + res, &COLUMN_MAGIC_NUM, sizeof(uint32_t)), res += sizeof(uint32_t);
	memcpy(buf + res, &len, sizeof(uint32_t)), res += sizeof(uint32_t);
	memcpy(buf + res, name_.c_str(), len), res += len;
	memcpy(buf + res, &type_, sizeof(TypeId)), res += sizeof(TypeId);
	memcpy(buf + res, &len_, sizeof(uint32_t)), res += sizeof(uint32_t);
	memcpy(buf + res, &table_ind_, sizeof(uint32_t)), res += sizeof(uint32_t);
	memcpy(buf + res, &nullable_, sizeof(bool)), res += sizeof(bool);
	memcpy(buf + res, &unique_, sizeof(bool)), res += sizeof(bool);
	return res;
}

/**
 * TODO: Student Implement (finished)
 * @brief: Get the size of the serialized column
 * @return: the size of the serialized column
 */
uint32_t Column::GetSerializedSize() const {
	// replace with your code here
	return name_.length()
		+ sizeof(TypeId)
		+ sizeof(uint32_t) * 4
		+ sizeof(bool) * 2;
}

/**
 * TODO: Student Implement (finished)
 * @brief: Deserialize the column from a buffer
 * @param buf: the buffer to deserialize from
 * @param column: the deserialized column
 * @return: the number of bytes read from the buffer
 * @note: the caller is responsible for freeing the column
 */
uint32_t Column::DeserializeFrom(char* buf, Column*& column) {
	// replace with your code here
	uint32_t res = 4, len;
	TypeId type;
	uint32_t col_len, table_ind;
	bool nullable, unique;
	if (column != nullptr) {
		LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
	}
	uint32_t magic_num = MACH_READ_UINT32(buf);
	// LOG(INFO) << "column_magic_num: " << magic_num;
	ASSERT(magic_num == COLUMN_MAGIC_NUM, "Failed to deserialize column.");
	memcpy(&len, buf + res, sizeof(uint32_t)), res += sizeof(uint32_t);
	std::string name(buf + res, len); res += len;
	memcpy(&type, buf + res, sizeof(TypeId)), res += sizeof(TypeId);
	memcpy(&col_len, buf + res, sizeof(uint32_t)), res += sizeof(uint32_t);
	memcpy(&table_ind, buf + res, sizeof(uint32_t)), res += sizeof(uint32_t);
	memcpy(&nullable, buf + res, sizeof(bool)), res += sizeof(bool);
	memcpy(&unique, buf + res, sizeof(bool)), res += sizeof(bool);
	if (type == TypeId::kTypeChar) {
		column = new Column(name, type, col_len, table_ind, nullable, unique);
	}
	else {
		column = new Column(name, type, table_ind, nullable, unique);
	}
	return res;
}
