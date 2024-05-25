#include "storage/table_heap.h"

/**
 * TODO: Student Implement (finished)
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
    uint32_t siz = row.GetSerializedSize(schema_);
    if (siz >= TablePage::SIZE_MAX_ROW) return false;
    if (last_visited_page_id_ == INVALID_PAGE_ID) {
        last_visited_page_id_ = first_page_id_;
    }
    page_id_t cur_pid = last_visited_page_id_, nxt_pid;
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_pid));
    while (!page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
        nxt_pid = page->GetNextPageId();
        if (nxt_pid == INVALID_PAGE_ID) {
            auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(nxt_pid));
            if (new_page == nullptr) return false;
            new_page->Init(nxt_pid, cur_pid, log_manager_, txn);
            new_page->SetNextPageId(INVALID_PAGE_ID);
            page->SetNextPageId(nxt_pid);
            buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
            page = new_page;
        } else {
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(nxt_pid));
        }
    }
    last_visited_page_id_ = page->GetPageId();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    // If the page could not be found, then abort the recovery.
    if (page == nullptr) {
        return false;
    }
    // Otherwise, mark the tuple as deleted.
    page->WLatch();
    page->MarkDelete(rid, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
}

/**
 * TODO: Student Implement (finished)
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) return false;
    Row* old_row = new Row(rid);
    page->RLatch();
    bool res = page->GetTuple(old_row, schema_, txn, lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    if (!res) return delete old_row, false;
    page->WLatch();
    int upd_res = page->UpdateTuple(row, old_row, schema_, txn, lock_manager_, log_manager_);
    delete old_row;
    if (upd_res == TablePage::TUPLE_UPDATED) {
        // Successfully updated
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
        return true;
    }
    if (upd_res == TablePage::NOT_ENOUGH_SPACE) {
        // Not enough space, delete and insert
        Row* new_row = new Row(row);
        bool insert_res = InsertTuple(*new_row, txn);
        delete new_row;
        if (!insert_res) {
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
            return false;
        }
        page->MarkDelete(rid, txn, lock_manager_, log_manager_);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
        return true;
    }
    // Invalid slot number or tuple is deleted
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
}

/**
 * TODO: Student Implement (finished)
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
    // Step1: Find the page which contains the tuple.
    // Step2: Delete the tuple from the page.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    last_visited_page_id_ = INVALID_PAGE_ID;
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    // Rollback to delete.
    page->WLatch();
    page->RollbackDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement (finished)
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    if (page == nullptr) return false;
    page->RLatch();
    bool res = page->GetTuple(row, schema_, txn, lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
    return res;
}

void TableHeap::DeleteTable(page_id_t page_id) {
    if (page_id != INVALID_PAGE_ID) {
        auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
        if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
        DeleteTable(temp_table_page->GetNextPageId());
        buffer_pool_manager_->UnpinPage(page_id, false);
        buffer_pool_manager_->DeletePage(page_id);
    } else {
        DeleteTable(first_page_id_);
    }
}

/**
 * TODO: Student Implement (finished)
 */
TableIterator TableHeap::Begin(Txn *txn) {
    page_id_t page_id = first_page_id_;
    while (page_id != INVALID_PAGE_ID) {
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        page->RLatch();
        RowId rid;
        if (page->GetFirstTupleRid(&rid)) {
            page->RUnlatch();
            buffer_pool_manager_->UnpinPage(page_id, false);
            return TableIterator(new Row(rid), this);
        }
        page_id = page->GetNextPageId();
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, false);
    }
    return this->End();
}

/**
 * TODO: Student Implement (finished)
 */
TableIterator TableHeap::End() {
    return TableIterator(new Row(INVALID_ROWID), this);
}
