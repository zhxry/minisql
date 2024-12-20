#include "record/row.h"

/**
 * TODO: Student Implement
 * @brief: Serialize the row to a buffer
 * @param buf: the buffer to serialize to
 * @param schema: the schema of the row
 * @return: the number of bytes written to the buffer
 */
uint32_t Row::SerializeTo(char* buf, Schema* schema) const {
	ASSERT(schema != nullptr, "Invalid schema before serialize.");
	ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
	uint32_t res = sizeof(uint32_t);
	uint32_t cnt = this->GetFieldCount();
	uint32_t len = (cnt + 31) / 32;
	res += len * sizeof(uint32_t);
	uint32_t* nulls = new uint32_t[len];
	memset(nulls, 0, len * sizeof(uint32_t));
	for (int i = 0; i < cnt; ++i) {
		if (fields_[i]->IsNull()) nulls[i / 32] |= 1u << (i % 32);
		else res += fields_[i]->SerializeTo(buf + res);
	}
	memcpy(buf, &cnt, sizeof(uint32_t));
	memcpy(buf + sizeof(uint32_t), nulls, len * sizeof(uint32_t));
	delete[] nulls;
	return res;
}

uint32_t Row::DeserializeFrom(char* buf, Schema* schema) {
	uint32_t res = sizeof(uint32_t), cnt = 0;
	memcpy(&cnt, buf, sizeof(uint32_t));
	uint32_t len = (cnt + 31) / 32;
	res += len * sizeof(uint32_t);
	uint32_t* nulls = new uint32_t[len];
	memcpy(nulls, buf + sizeof(uint32_t), len * sizeof(uint32_t));
	fields_.resize(cnt);
	for (int i = 0; i < cnt; ++i) {
		if (nulls[i / 32] & (1u << (i % 32))) {
			res += Field::DeserializeFrom(buf + res, schema->GetColumn(i)->GetType(), &fields_[i], true);
		}
		else {
			res += Field::DeserializeFrom(buf + res, schema->GetColumn(i)->GetType(), &fields_[i], false);
		}
	}
	delete[] nulls;
	return res;
}

uint32_t Row::GetSerializedSize(Schema* schema) const {
	// ASSERT(schema != nullptr, "Invalid schema before serialize.");
	// ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
	// replace with your code here
	uint32_t res = sizeof(uint32_t);
	int cnt = schema->GetColumnCount();
	res += (cnt + 31) / 32 * sizeof(uint32_t);
	for (int i = 0; i < cnt; ++i) {
		if (!fields_[i]->IsNull()) {
			res += fields_[i]->GetSerializedSize();
		}
	}
	return res;
}

void Row::GetKeyFromRow(const Schema* schema, const Schema* key_schema, Row& key_row) {
	auto columns = key_schema->GetColumns();
	std::vector<Field> fields;
	uint32_t idx;
	for (auto column : columns) {
		schema->GetColumnIndex(column->GetName(), idx);
		fields.emplace_back(*this->GetField(idx));
	}
	key_row = Row(fields);
	key_row.SetRowId(this->GetRowId());
}
