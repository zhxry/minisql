#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = { 0 };

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
	: pool_size_(pool_size), disk_manager_(disk_manager) {
	pages_ = new Page[pool_size_];
	replacer_ = new LRUReplacer(pool_size_);
	for (size_t i = 0; i < pool_size_; i++) {
		free_list_.emplace_back(i);
	}
}

BufferPoolManager::~BufferPoolManager() {
	for (auto page : page_table_) {
		FlushPage(page.first);
	}
	delete[] pages_;
	delete replacer_;
}

/**
 * TODO: Student Implement (finished)
 * @brief: Flushes all the pages in the buffer pool to disk
 * @note: This function is not thread safe
 * @note: This function should be called by the disk manager when a page is read from disk
 * @param page_id: the page id of the page that was read from disk
 */
Page* BufferPoolManager::FetchPage(page_id_t page_id) {
	// 1.    Search the page table for the requested page (P).
	// 1.1   If P exists, pin it and return it immediately.
	// 1.2   If P does not exist, find a replacement page (R) from either the free list or the replacer.
	//       Note that pages are always found from the free list first.
	// 2.    If R is dirty, write it back to the disk.
	// 3.    Delete R from the page table and insert P.
	// 4.    Update P's metadata, read in the page content from disk, and then return a pointer to P.
	if (page_table_.find(page_id) != page_table_.end()) {
		frame_id_t frame_id = page_table_[page_id];
		pages_[frame_id].pin_count_++;
		replacer_->Pin(frame_id);
		return &pages_[frame_id];
	}
	frame_id_t frame_id = INVALID_FRAME_ID;
	if (!free_list_.empty()) {
		frame_id = free_list_.front();
		free_list_.pop_front();
	}
	else {
		if (!replacer_->Victim(&frame_id)) return nullptr;
	}
	if (pages_[frame_id].IsDirty()) FlushPage(pages_[frame_id].GetPageId());
	page_table_.erase(pages_[frame_id].GetPageId());
	page_table_[page_id] = frame_id;
	pages_[frame_id].ResetMemory();
	pages_[frame_id].pin_count_ = 1;
	pages_[frame_id].is_dirty_ = false;
	pages_[frame_id].page_id_ = page_id;
	disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
	replacer_->Pin(frame_id);
	return &pages_[frame_id];
}

/**
 * TODO: Student Implement (finished)
 * @note: This function is not thread safe
 * @note: This function should be called by the disk manager when a page is written to disk
 * @param page_id: the page id of the page that was written to disk
 */
Page* BufferPoolManager::NewPage(page_id_t& page_id) {
	// 0.   Make sure you call AllocatePage!
	// 1.   If all the pages in the buffer pool are pinned, return nullptr.
	// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
	// 3.   Update P's metadata, zero out memory and add P to the page table.
	// 4.   Set the page ID output parameter. Return a pointer to P.
	if (free_list_.empty() && replacer_->Size() == 0) return nullptr;
	frame_id_t frame_id = INVALID_FRAME_ID;
	if (!free_list_.empty()) {
		frame_id = free_list_.front();
		free_list_.pop_front();
	}
	else {
		if (!replacer_->Victim(&frame_id)) return nullptr;
	}
	if (pages_[frame_id].IsDirty()) FlushPage(pages_[frame_id].GetPageId());
	page_table_.erase(pages_[frame_id].GetPageId());
	page_id = AllocatePage();
	page_table_[page_id] = frame_id;
	pages_[frame_id].ResetMemory();
	pages_[frame_id].pin_count_ = 1;
	pages_[frame_id].is_dirty_ = false;
	pages_[frame_id].page_id_ = page_id;
	replacer_->Pin(frame_id);
	return &pages_[frame_id];
}

/**
 * TODO: Student Implement (finished)
 * @brief: delete the page from the buffer pool and from the disk
 * @note: This function is not thread safe
 * @note: This function should be called by the disk manager when a page is written to disk
 * @param page_id: the page id of the page that was written to disk
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
	// 0.   Make sure you call DeallocatePage!
	// 1.   Search the page table for the requested page (P).
	// 1.   If P does not exist, return true.
	// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
	// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
	if (page_table_.find(page_id) == page_table_.end()) return true;
	frame_id_t frame_id = page_table_[page_id];
	if (pages_[frame_id].GetPinCount() != 0) return false;
	if (pages_[frame_id].IsDirty()) FlushPage(page_id);
	DeallocatePage(page_id);
	page_table_.erase(page_id);
	pages_[frame_id].ResetMemory();
	pages_[frame_id].pin_count_ = 0;
	pages_[frame_id].is_dirty_ = false;
	pages_[frame_id].page_id_ = INVALID_PAGE_ID;
	free_list_.push_back(frame_id);
	replacer_->Pin(frame_id);
	return true;
}

/**
 * TODO: Student Implement (finished)
 * @brief: Unpins a page from the buffer pool.
 *
 * This function unpins a page with the specified page ID from the buffer pool. 
 * If the page is not found in the page table, or if the page's pin count is already 0, 
 * the function returns false. Otherwise, the function decreases the pin count of the page 
 * and updates its dirty flag if specified. If the pin count becomes 0 after decrementing, 
 * the function notifies the replacer to unpin the corresponding frame.
 *
 * @param page_id The ID of the page to be unpinned.
 * @param is_dirty A flag indicating whether the page should be marked as dirty.
 * @return True if the page was successfully unpinned, false otherwise.
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
	if (page_table_.find(page_id) == page_table_.end()) return false;
	frame_id_t frame_id = page_table_[page_id];
	if (pages_[frame_id].GetPinCount() == 0) return false;
	if (is_dirty) pages_[frame_id].is_dirty_ = true;
	if (!--pages_[frame_id].pin_count_) replacer_->Unpin(frame_id);
	return true;
}

/**
 * TODO: Student Implement (finished)
 * @brief: flushes the page to disk
 * @note: This function is not thread safe
 * @param page_id: the page id of the page to flush
 * @return true if the operation is successful, false otherwise
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
	if (page_table_.find(page_id) == page_table_.end()) return false;
	frame_id_t frame_id = page_table_[page_id];
	disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
	pages_[frame_id].is_dirty_ = false;
	return true;
}

page_id_t BufferPoolManager::AllocatePage() {
	int next_page_id = disk_manager_->AllocatePage();
	return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
	disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
	return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
	bool res = true;
	for (size_t i = 0; i < pool_size_; i++) {
		if (pages_[i].pin_count_ != 0) {
			res = false;
			LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
		}
	}
	return res;
}