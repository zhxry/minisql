#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement (finished)
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
	if (page_allocated_ >= GetMaxSupportedSize()) return false;
	bytes[next_free_page_ / 8] |= 1 << (next_free_page_ % 8);
	page_offset = next_free_page_;
	++page_allocated_;
	while (!IsPageFree(next_free_page_) && next_free_page_ < GetMaxSupportedSize() - 1) {
		++next_free_page_;
	}
  	return true;
}

/**
 * TODO: Student Implement (finished)
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
	if (page_offset >= GetMaxSupportedSize()) {
		LOG(ERROR) << "Invalid page_offset:" << page_offset << std::endl;
		return false;
	}
	if (IsPageFree(page_offset)) return false;
	bytes[page_offset / 8] &= ~(1 << (page_offset % 8));
	--page_allocated_;
	if (page_offset < next_free_page_) next_free_page_ = page_offset;
  	return true;
}

/**
 * TODO: Student Implement (finished)
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
	if (page_offset >= GetMaxSupportedSize()) {
		LOG(ERROR) << "Invalid page_offset:" << page_offset << std::endl;
		return false;
	}
  	return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
	if (byte_index >= MAX_CHARS) {
		LOG(ERROR) << "Invalid byte index:" << byte_index << std::endl;
  		return false;
	}
	return !(bytes[byte_index] & (1 << bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;