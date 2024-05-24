#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    SetLSN(INVALID_LSN);
    SetKeySize(key_size);
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
    return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
    memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
    return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
    *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
    for (int i = 0; i < GetSize(); ++i) {
        if (ValueAt(i) == value) return i;
    }
    return -1;
}

void *InternalPage::PairPtrAt(int index) {
    return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
    memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    int l = 1, r = GetSize() - 1;
    while (l <= r) {
        int mid = (l + r) / 2;
        if (KM.CompareKeys(KeyAt(mid), key) <= 0) l = mid + 1;
        else r = mid - 1;
    }
    return ValueAt(r);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    SetSize(2);
    SetKeyAt(1, new_key);
    SetValueAt(0, old_value);
    SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    int index = ValueIndex(old_value);
    if (index == -1) return GetSize();
    memmove(PairPtrAt(index + 2), PairPtrAt(index + 1), (GetSize() - index - 1) * pair_size);
    SetKeyAt(index + 1, new_key);
    SetValueAt(index + 1, new_value);
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize(), half = size / 2;
    recipient->CopyNFrom(pairs_off + half * pair_size, size - half, buffer_pool_manager);
    SetSize(half);
}

/* Copy entries into me, starting from {src} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    PairCopy(pairs_off + GetSize() * pair_size, src, size);
    IncreaseSize(size);
    for (int i = 0; i < size; ++i) {
        page_id_t child_page_id = ValueAt(GetSize() - size + i);
        if (child_page_id != INVALID_PAGE_ID) {
            auto *child_page = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(child_page_id)->GetData());
            child_page->SetParentPageId(GetPageId());
            buffer_pool_manager->UnpinPage(child_page_id, true);
        }
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    memmove(PairPtrAt(index), PairPtrAt(index + 1), (GetSize() - index - 1) * pair_size);
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    page_id_t value = ValueAt(0);
    SetValueAt(0, INVALID_PAGE_ID);
    IncreaseSize(-1);
    return value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
    recipient->CopyNFrom(pairs_off, GetSize(), buffer_pool_manager);
    recipient->SetKeyAt(recipient->GetSize() - GetSize(), middle_key);
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
    recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
    Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    ASSERT(GetSize() + 1 <= GetMaxSize(), "size exceed max size");
    SetKeyAt(GetSize(), key);
    SetValueAt(GetSize(), value);
    IncreaseSize(1);
    if (value != INVALID_PAGE_ID) {
        auto *child_page = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(value)->GetData());
        child_page->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(value, true);
    }
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    recipient->CopyFirstFrom(ValueAt(GetSize() - 1), buffer_pool_manager);
    recipient->SetKeyAt(0, KeyAt(GetSize() - 1));
    recipient->SetKeyAt(1, middle_key);
    Remove(GetSize() - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    ASSERT(GetSize() + 1 <= GetMaxSize(), "size exceed max size");
    memmove(PairPtrAt(1), PairPtrAt(0), GetSize() * pair_size);
    SetValueAt(0, value);
    IncreaseSize(1);
    if (value != INVALID_PAGE_ID) {
        auto *child_page = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(value)->GetData());
        child_page->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(value, true);
    }
}