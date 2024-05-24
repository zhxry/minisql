#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement (finished)
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if (LRU_list.empty()) return frame_id = nullptr, false;
    *frame_id = LRU_list.front();
    LRU_list.pop_front();
    LRU_map.erase(*frame_id);
    return true;
}

/**
 * TODO: Student Implement (finished)
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
    if (LRU_map.find(frame_id) != LRU_map.end()) {
        LRU_list.erase(LRU_map[frame_id]);
        LRU_map.erase(frame_id);
    }
}

/**
 * TODO: Student Implement (finished)
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
    if (LRU_map.find(frame_id) == LRU_map.end()) {
        LRU_list.push_back(frame_id);
        LRU_map[frame_id] = --LRU_list.end();
    }
}

/**
 * TODO: Student Implement (finished)
 */
size_t LRUReplacer::Size() {
    return LRU_map.size();
}