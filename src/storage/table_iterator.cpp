#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement (finished)
 */
// TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {}
TableIterator::TableIterator() {}

TableIterator::TableIterator(Row *row, TableHeap *table_heap) {
    this->row_ = row;
    this->table_heap_ = table_heap;
}

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
    this->row_ = new Row(rid);
    this->table_heap_ = table_heap;
}

TableIterator::TableIterator(const TableIterator &other) {
    this->row_ = new Row(*other.row_);
    this->table_heap_ = other.table_heap_;
}

TableIterator::~TableIterator() {
    delete this->row_;
    this->table_heap_ = nullptr;
}

bool TableIterator::operator==(const TableIterator &itr) const {
    return this->row_->GetRowId() == itr.row_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    return !(this->row_->GetRowId() == itr.row_->GetRowId());
}

const Row &TableIterator::operator*() {
    return *row_;
}

Row *TableIterator::operator->() {
    return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    this->row_ = new Row(*itr.row_);
    this->table_heap_ = itr.table_heap_;
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
    RowId rid = this->row_->GetRowId(), new_rid;
    page_id_t page_id = rid.GetPageId();
    if (page_id == INVALID_PAGE_ID) return this->row_->SetRowId(INVALID_ROWID), *this;
    auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
    page->RLatch();
    if (page->GetNextTupleRid(rid, &new_rid)) {
        page->RUnlatch();
        this->row_->SetRowId(new_rid);
        table_heap_->GetTuple(this->row_, nullptr);
        table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
        return *this;
    }
    page_id_t next_page_id = page->GetNextPageId();
    while (next_page_id != INVALID_PAGE_ID) {
        page->RUnlatch();
        page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
        page->RLatch();
        if (page->GetFirstTupleRid(&new_rid)) {
            page->RUnlatch();
            this->row_->SetRowId(new_rid);
            table_heap_->GetTuple(this->row_, nullptr);
            table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
            return *this;
        }
        next_page_id = page->GetNextPageId();
        table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    page->RUnlatch();
    this->row_->SetRowId(INVALID_ROWID);
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator temp(*this);
    ++(*this);
    return (const TableIterator)temp;
}
