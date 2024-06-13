#include "buffer/clock_replacer.h"
#include "glog/logging.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages) {
    clock_status.resize(num_pages + 1);
    for (frame_id_t i = 1; i <= num_pages; i++) clock_status[i] = EMPTY;
    clock_hand = 1;
    size = 0;
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
    if (size == 0) return frame_id = nullptr, false;
    for (int i = 0; i <= capacity; ++i) {
        if (clock_status[clock_hand] == USED) {
            clock_status[clock_hand] = UNUSED;
        } else if (clock_status[clock_hand] == UNUSED) {
            clock_status[clock_hand] = EMPTY;
            *frame_id = clock_hand;
            clock_hand = clock_hand % capacity + 1;
            return --size, true;
        }
        clock_hand = clock_hand % capacity + 1;
    }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
    if (clock_status[frame_id] != EMPTY) {
        --size;
        clock_status[frame_id] = EMPTY;
    }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
    if (clock_status[frame_id] == EMPTY) {
        ++size;
        clock_status[frame_id] = USED;
    }
}

size_t CLOCKReplacer::Size() {
    return size;
}