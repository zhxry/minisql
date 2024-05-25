#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
    root_page_id_ = INVALID_PAGE_ID;
    auto *index_roots_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
    if (!index_roots_page->GetRootId(index_id_, &root_page_id_)) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(1);
    }
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
    if (leaf_max_size_ == 0) {
        leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;
    }
    if (internal_max_size_ == 0) {
        internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t)) - 1;
    }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
    if (current_page_id == INVALID_PAGE_ID) current_page_id = root_page_id_;
    ASSERT(current_page_id != INVALID_PAGE_ID, "page_id invalid");
    auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
    if (page->IsRootPage()) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(0);
    }
    if (!page->IsLeafPage()) {
        auto *internal_page = reinterpret_cast<InternalPage *>(page);
        for (int i = 0; i < internal_page->GetSize(); i++) {
            Destroy(internal_page->ValueAt(i));
        }
    }
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    buffer_pool_manager_->DeletePage(current_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
    if (IsEmpty()) return false;
    auto *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID, false));
    if (IsEmpty() || leaf_page == nullptr) return false;
    RowId rid;
    bool ret = leaf_page->Lookup(key, rid, processor_);
    if (ret) result.push_back(rid);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
    if (IsEmpty()) return StartNewTree(key, value), true;
    return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
    Page *page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) throw "out of memory";
    auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    leaf_page->Init(page->GetPageId(), INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
    leaf_page->Insert(key, value, processor_);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
    auto *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID, false));
    if (leaf_page == nullptr) return false;
    RowId rid;
    if (leaf_page->Lookup(key, rid, processor_)) {
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
        return false;
    }
    if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
        leaf_page->Insert(key, value, processor_);
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
        return true;
    }
    auto *new_leaf_page = Split(leaf_page, transaction);
    if (processor_.CompareKeys(key, leaf_page->KeyAt(leaf_page->GetSize() - 1)) > 0) {
        new_leaf_page->Insert(key, value, processor_);
    } else {
        leaf_page->Insert(key, value, processor_);
    }
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
    page_id_t new_page_id;
    Page *page = buffer_pool_manager_->NewPage(new_page_id);
    if (page == nullptr) throw "out of memory";
    auto *new_internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    new_internal_page->Init(page->GetPageId(), node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
    node->MoveHalfTo(new_internal_page, buffer_pool_manager_);
    return new_internal_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
    page_id_t new_page_id;
    Page *page = buffer_pool_manager_->NewPage(new_page_id);
    if (page == nullptr) throw "out of memory";
    auto *new_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    new_leaf_page->Init(page->GetPageId(), node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
    node->MoveHalfTo(new_leaf_page);
    new_leaf_page->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(new_leaf_page->GetPageId());
    return new_leaf_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
    if (old_node->IsRootPage()) {
        // create a new root
        Page *page = buffer_pool_manager_->NewPage(root_page_id_);
        if (page == nullptr) throw "out of memory";
        auto *new_root_page = reinterpret_cast<InternalPage *>(page->GetData());
        new_root_page->Init(page->GetPageId(), INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
        new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);
        UpdateRootPageId(0);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        return;
    }
    auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
    if (parent->GetSize() < parent->GetMaxSize()) {
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        return;
    }
    auto *new_parent = Split(parent, transaction);
    if (processor_.CompareKeys(key, parent->KeyAt(parent->GetSize() - 1)) > 0) {
        new_parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(new_parent->GetPageId());
    } else {
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    }
    InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
    if (IsEmpty()) return;
    auto *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID, false));
    if (leaf_page == nullptr) return;
    leaf_page->RemoveAndDeleteRecord(key, processor_);
    if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
        return;
    }
    if (CoalesceOrRedistribute(leaf_page, transaction)) {
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
        buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    } else {
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
    if (node->IsRootPage()) return AdjustRoot(node);
    auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
    int index = parent->ValueIndex(node->GetPageId());
    page_id_t sibling_pid = index == 0 ? parent->ValueAt(1) : parent->ValueAt(index - 1);
    auto *sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(sibling_pid)->GetData());
    if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
        Redistribute(sibling, node, index);
        buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        return false;
    }
    if (Coalesce(sibling, node, parent, index, transaction)) {
        // parent size < min size
        bool ret = parent->IsRootPage() ? AdjustRoot(parent) : CoalesceOrRedistribute(parent, transaction);
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), !ret);
        if (ret) buffer_pool_manager_->DeletePage(parent->GetPageId());
    } else {
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
    return buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true), true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
    if (index == 0) {
        neighbor_node->MoveAllTo(node);
        parent->Remove(1);
    } else {
        node->MoveAllTo(neighbor_node);
        parent->Remove(index);
    }
    return parent->GetSize() < parent->GetMinSize();
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
    if (index == 0) {
        neighbor_node->MoveAllTo(node, parent->KeyAt(1), buffer_pool_manager_);
        parent->Remove(1);
    } else {
        node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
        parent->Remove(index);
    }
    return parent->GetSize() < parent->GetMinSize();
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @paran   index              0: neighbor on th right, >0: neighbor on the left
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
    auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
    if (index == 0) {
        neighbor_node->MoveFirstToEndOf(node);
        parent->SetKeyAt(1, neighbor_node->KeyAt(0));
    } else {
        neighbor_node->MoveLastToFrontOf(node);
        parent->SetKeyAt(index, node->KeyAt(0));
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
    auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
    if (index == 0) {
        node->MoveFirstToEndOf(neighbor_node, parent->KeyAt(1), buffer_pool_manager_);
        parent->SetKeyAt(1, neighbor_node->KeyAt(0));
    } else {
        node->MoveLastToFrontOf(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
        parent->SetKeyAt(index, node->KeyAt(0));
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
    if (old_root_node->IsLeafPage()) {
        if (old_root_node->GetSize() == 0) {
            // Deleted the last element in whole b+ tree
            root_page_id_ = INVALID_PAGE_ID;
            return UpdateRootPageId(0), true;
        }
        return false;
    }
    auto *root_node = reinterpret_cast<InternalPage *>(old_root_node);
    if (root_node->GetSize() == 1) {
        // Deleted the last element in root page, but still has one last child
        page_id_t new_root_pid = root_node->RemoveAndReturnOnlyChild();
        auto *new_root_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(new_root_pid)->GetData());
        new_root_node->SetParentPageId(INVALID_PAGE_ID);
        root_page_id_ = new_root_pid;
        UpdateRootPageId(0);
        buffer_pool_manager_->UnpinPage(new_root_pid, true);
        return true;
    }
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
    auto *page = FindLeafPage(nullptr, INVALID_PAGE_ID, true);
    if (page == nullptr) return End();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return IndexIterator(page->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
    auto *page = FindLeafPage(key, INVALID_PAGE_ID, false);
    if (page == nullptr) return End();
    int index = reinterpret_cast<LeafPage *>(page)->KeyIndex(key, processor_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return IndexIterator(page->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
    return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Find the leaf page that contains the input key from input page_id
 * if page_id == INVALID_PAGE_ID, start from root page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
    if (page_id == INVALID_PAGE_ID) page_id = root_page_id_;
    if (page_id == INVALID_PAGE_ID) return nullptr;
    auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
    while (!page->IsLeafPage()) {
        auto *internal_page = reinterpret_cast<InternalPage *>(page);
        if (leftMost) page_id = internal_page->ValueAt(0);
        else page_id = internal_page->Lookup(key, processor_);
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
        page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
    }
    // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return reinterpret_cast<Page *>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
    IndexRootsPage *index_roots_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
    if (insert_record) index_roots_page->Insert(index_id_, root_page_id_);
    else index_roots_page->Update(index_id_, root_page_id_);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
    std::string leaf_prefix("LEAF_");
    std::string internal_prefix("INT_");
    if (page->IsLeafPage()) {
        auto *leaf = reinterpret_cast<LeafPage *>(page);
        // Print node name
        out << leaf_prefix << leaf->GetPageId();
        // Print node properties
        out << "[shape=plain color=green ";
        // Print data of the node
        out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
        // Print data
        out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
                << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
        out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
                << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
                << "</TD></TR>\n";
        out << "<TR>";
        for (int i = 0; i < leaf->GetSize(); i++) {
            Row ans;
            processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
            out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
        }
        out << "</TR>";
        // Print table end
        out << "</TABLE>>];\n";
        // Print Leaf node link if there is a next page
        if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
            out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
            out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
        }

        // Print parent links if there is a parent
        if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
            out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
                    << leaf->GetPageId() << ";\n";
        }
    } else {
        auto *inner = reinterpret_cast<InternalPage *>(page);
        // Print node name
        out << internal_prefix << inner->GetPageId();
        // Print node properties
        out << "[shape=plain color=pink ";// why not?
        // Print data of the node
        out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
        // Print data
        out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
                << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
        out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
                << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
                << "</TD></TR>\n";
        out << "<TR>";
        for (int i = 0; i < inner->GetSize(); i++) {
            out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
            if (i > 0) {
                Row ans;
                processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
                out << ans.GetField(0)->toString();
            } else {
                out << " ";
            }
            out << "</TD>\n";
        }
        out << "</TR>";
        // Print table end
        out << "</TABLE>>];\n";
        // Print Parent link
        if (inner->GetParentPageId() != INVALID_PAGE_ID) {
            out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
                    << inner->GetPageId() << ";\n";
        }
        // Print leaves
        for (int i = 0; i < inner->GetSize(); i++) {
            auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
            ToGraph(child_page, bpm, out, schema);
            if (i > 0) {
                auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
                if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
                    out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
                            << child_page->GetPageId() << "};\n";
                }
                bpm->UnpinPage(sibling_page->GetPageId(), false);
            }
        }
    }
    bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
    if (page->IsLeafPage()) {
        auto *leaf = reinterpret_cast<LeafPage *>(page);
        std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
                            << " next: " << leaf->GetNextPageId() << std::endl;
        for (int i = 0; i < leaf->GetSize(); i++) {
            std::cout << leaf->KeyAt(i) << ",";
        }
        std::cout << std::endl;
        std::cout << std::endl;
    } else {
        auto *internal = reinterpret_cast<InternalPage *>(page);
        std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
        for (int i = 0; i < internal->GetSize(); i++) {
            std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
        }
        std::cout << std::endl;
        std::cout << std::endl;
        for (int i = 0; i < internal->GetSize(); i++) {
            ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
            bpm->UnpinPage(internal->ValueAt(i), false);
        }
    }
}

bool BPlusTree::Check() {
    bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
    if (!all_unpinned) {
        LOG(ERROR) << "problem in page unpin" << endl;
    }
    return all_unpinned;
}