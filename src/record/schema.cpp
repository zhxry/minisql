#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
    uint32_t res = 0, siz = columns_.size();
    memcpy(buf + res, &siz, sizeof(uint32_t)), res += sizeof(uint32_t);
    for (Column* col : columns_) {
        res += col->SerializeTo(buf + res);
    }
    return res;
}

uint32_t Schema::GetSerializedSize() const {
    uint32_t res = sizeof(uint32_t);
    for (Column* col : columns_) {
        res += col->GetSerializedSize();
    }
    return res;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    uint32_t res = 0, siz;
    memcpy(&siz, buf + res, sizeof(uint32_t)), res += sizeof(uint32_t);
    std::vector<Column *> columns(siz);
    for (int i = 0; i < siz; ++i) {
        res += Column::DeserializeFrom(buf + res, columns[i]);
    }
    schema = new Schema(columns, true);
    return res;
}