#include "catalog/catalog.h"

/**
 * @brief: the catalog metadata to a buffer.
 *
 * This function writes the catalog metadata to a character buffer in a serialized format.
 * The serialized data includes the magic number, the number of table metadata pages,
 * the number of index metadata pages, and the table and index mappings.
 *
 * @param buf A pointer to the buffer where the serialized data will be written.
 *
 * @pre The size of the serialized data must not exceed the page size.
 *
 * @post The catalog metadata is serialized and written to the buffer.
 *
 * @remarks The serialized data format is as follows:
 *          - Magic number (4 bytes)
 *          - Number of table metadata pages (4 bytes)
 *          - Number of index metadata pages (4 bytes)
 *          - Table mappings (table_id_t and page_id_t pairs)
 *          - Index mappings (index_id_t and page_id_t pairs)
 *
 * @throws An assertion error if the size of the serialized data exceeds the page size.
 */
void CatalogMeta::SerializeTo(char* buf) const {
	ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
	MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
	buf += 4;
	MACH_WRITE_UINT32(buf, table_meta_pages_.size());
	buf += 4;
	MACH_WRITE_UINT32(buf, index_meta_pages_.size());
	buf += 4;
	for (auto iter : table_meta_pages_) {
		MACH_WRITE_TO(table_id_t, buf, iter.first);
		buf += 4;
		MACH_WRITE_TO(page_id_t, buf, iter.second);
		buf += 4;
	}
	for (auto iter : index_meta_pages_) {
		MACH_WRITE_TO(index_id_t, buf, iter.first);
		buf += 4;
		MACH_WRITE_TO(page_id_t, buf, iter.second);
		buf += 4;
	}
}

/**
 * @brief Deserialize catalog metadata from a buffer.
 *
 * @param buf The buffer containing the serialized metadata.
 * @return A pointer to the deserialized CatalogMeta object.
 */
CatalogMeta* CatalogMeta::DeserializeFrom(char* buf) {
	// check valid
	uint32_t magic_num = MACH_READ_UINT32(buf);
	buf += 4;
	ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
	// get table and index nums
	uint32_t table_nums = MACH_READ_UINT32(buf);
	buf += 4;
	uint32_t index_nums = MACH_READ_UINT32(buf);
	buf += 4;
	// create metadata and read value
	CatalogMeta* meta = new CatalogMeta();
	for (uint32_t i = 0; i < table_nums; i++) {
		auto table_id = MACH_READ_FROM(table_id_t, buf);
		buf += 4;
		auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
		buf += 4;
		meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
	}
	for (uint32_t i = 0; i < index_nums; i++) {
		auto index_id = MACH_READ_FROM(index_id_t, buf);
		buf += 4;
		auto index_page_id = MACH_READ_FROM(page_id_t, buf);
		buf += 4;
		meta->index_meta_pages_.emplace(index_id, index_page_id);
	}
	return meta;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief: Get the size of the serialized catalog metadata.
 * @return: The size of the serialized catalog metadata.
 * @note: The size of the serialized catalog metadata is the sum of
 * @note: the size of the magic number
 * @note: the size of the table meta pages
 * @note: and the size of the index meta pages.
 */
uint32_t CatalogMeta::GetSerializedSize() const {
	return
		4	// magic number
		+ 4	// table meta pages
		+ 4	// index meta pages
		+ 4 * table_meta_pages_.size() * 2
		+ 4 * index_meta_pages_.size() * 2;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Constructs a new instance of the CatalogManager class.
 *
 * @param buffer_pool_manager A pointer to the BufferPoolManager used for managing buffer pool pages.
 * @param lock_manager A pointer to the LockManager used for managing locks.
 * @param log_manager A pointer to the LogManager used for managing logs.
 * @param init A boolean value indicating whether to initialize a new catalog or load an existing one.
 */
CatalogManager::CatalogManager(
	BufferPoolManager* buffer_pool_manager,
	LockManager* lock_manager,
	LogManager* log_manager,
	bool init
) : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
	if (init) {		// initialize a new catalog
		catalog_meta_ = CatalogMeta::NewInstance();														// create a new catalog meta
		catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());	// serialize to disk
		buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);									// unpin the page
	}
	else {			// load an existing catalog
		catalog_meta_ = CatalogMeta::DeserializeFrom(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());	// deserialize from disk
		auto table_meta_pages = catalog_meta_->GetTableMetaPages();
		for (auto iter : *table_meta_pages)
			LoadTable(iter.first, iter.second);
		auto index_meta_pages = catalog_meta_->GetIndexMetaPages();
		for (auto iter : *index_meta_pages)
			LoadIndex(iter.first, iter.second);
		buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
	}
}

CatalogManager::~CatalogManager() {
	FlushCatalogMetaPage();
	delete catalog_meta_;
	for (auto iter : tables_) delete iter.second;
	for (auto iter : indexes_) delete iter.second;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief: a new table in the catalog with the given table name and schema.
 *
 * @param table_name The name of the table to be created.
 * @param schema A pointer to the schema of the table.
 * @param txn A pointer to the transaction object.
 * @param table_info A pointer to the table information object that will be populated with the created table's metadata.
 * @return A dberr_t value indicating the success or failure of the operation.
 *         - DB_SUCCESS: The table was created successfully.
 *         - DB_TABLE_ALREADY_EXIST: A table with the same name already exists in the catalog.
 */
dberr_t CatalogManager::CreateTable(
	const string& table_name,
	TableSchema* schema,
	Txn* txn,
	TableInfo*& table_info
) {
	// To ensure the table with the same name does not exist
	auto table = table_names_.find(table_name);
	if (table != table_names_.end())
		return DB_TABLE_ALREADY_EXIST;

	page_id_t page_id;
	table_id_t table_id = catalog_meta_->GetNextTableId();
	Schema* new_schema = Schema::DeepCopySchema(schema);
	TableHeap* table_heap = TableHeap::Create(buffer_pool_manager_, new_schema, txn, log_manager_, lock_manager_);
	TableMetadata* table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), new_schema);

	table_names_.emplace(table_name, table_id);
	table_meta->SerializeTo(buffer_pool_manager_->NewPage(page_id)->GetData());
	table_info = TableInfo::Create();
	table_info->Init(table_meta, table_heap);
	tables_.emplace(table_id, table_info);
	index_names_.emplace(table_name, std::unordered_map<string, index_id_t>());
	auto table_meta_pages = catalog_meta_->GetTableMetaPages();
	table_meta_pages->emplace(table_id, page_id);
	buffer_pool_manager_->UnpinPage(page_id, true);

	catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
	buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Retrieves the table information for the specified table name.
 * @param table_name The name of the table to retrieve.
 * @param table_info A pointer to a TableInfo object that will be updated with the retrieved table information.
 * @return A dberr_t value indicating the result of the operation. Returns DB_TABLE_NOT_EXIST if the table does not exist, or DB_SUCCESS if the operation was successful.
 * @note This function retrieves the table information for the specified table name and updates the table_info parameter with the retrieved information.
 */
dberr_t CatalogManager::GetTable(const string& table_name, TableInfo*& table_info) {
	auto table = table_names_.find(table_name);
	if (table == table_names_.end())
		return DB_TABLE_NOT_EXIST;
	table_info = tables_.find(table->second)->second;
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Retrieves all the tables in the catalog.
 * @param tables A vector of TableInfo pointers that will be populated with the retrieved table information.
 * @return A dberr_t value indicating the result of the operation. Returns DB_SUCCESS if the operation was successful.
 * @note This function retrieves all the tables in the catalog and populates the tables vector with the retrieved table information.
 */
dberr_t CatalogManager::GetTables(vector<TableInfo*>& tables) const {
	for (auto iter : tables_)
		tables.push_back(iter.second);
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Creates an index for the specified table.
 * @param table_name The name of the table.
 * @param index_name The name of the index to be created.
 * @param index_keys A vector of strings representing the column names to be indexed.
 * @param txn A pointer to the transaction object.
 * @param index_info A pointer to the index information object that will be populated with the created index's metadata.
 * @param index_type The type of the index.
 * @return A dberr_t value indicating the success or failure of the operation.
 *         - DB_SUCCESS: The index was created successfully.
 *         - DB_TABLE_NOT_EXIST: The specified table does not exist in the catalog.
 *         - DB_INDEX_ALREADY_EXIST: An index with the same name already exists for the specified table.
 *         - DB_COLUMN_NAME_NOT_EXIST: One or more column names in the index_keys vector do not exist in the table schema.
 * @note This function creates an index for the specified table with the given index name, index keys, and index type. It also updates the index_info parameter with the created index's metadata.
 */
dberr_t CatalogManager::CreateIndex(
	const std::string& table_name,
	const string& index_name,
	const std::vector<std::string>& index_keys,
	Txn* txn,
	IndexInfo*& index_info,
	const string& index_type
) {

	auto table = index_names_.find(table_name);
	if (table == index_names_.end())
		return DB_TABLE_NOT_EXIST;

	auto index = table->second.find(index_name);
	if (index != table->second.end())
		return DB_INDEX_ALREADY_EXIST;

	table_id_t table_id = table_names_.find(table_name)->second;
	index_id_t index_id = catalog_meta_->GetNextIndexId();
	TableInfo* table_info = tables_.find(table_id)->second;
	std::vector<uint32_t> key_map;
	std::vector<Column*> columns = table_info->GetSchema()->GetColumns();

	for (auto& index_key : index_keys) {
		bool found = false;
		for (size_t i = 0; i < columns.size(); i++) {
			if (columns[i]->GetName() == index_key) {
				key_map.push_back(i);
				found = true;
				break;
			}
		}
		if (!found) return DB_COLUMN_NAME_NOT_EXIST;
	}

	page_id_t page_id;
	IndexMetadata* index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map);
	index_info = IndexInfo::Create();
	index_info->Init(index_meta, table_info, buffer_pool_manager_);
	index_names_[table_name].emplace(index_name, index_id);
	indexes_.emplace(index_id, index_info);
	index_meta->SerializeTo(buffer_pool_manager_->NewPage(page_id)->GetData());
	auto index_meta_pages = catalog_meta_->GetIndexMetaPages();
	index_meta_pages->emplace(index_id, page_id);
	buffer_pool_manager_->UnpinPage(page_id, true);

	catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
	buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Retrieves the index information for the specified index name and table name.
 * @param table_name The name of the table.
 * @param index_name The name of the index to retrieve.
 * @param index_info A pointer to an IndexInfo object that will be updated with the retrieved index information.
 * @return A dberr_t value indicating the result of the operation. Returns DB_TABLE_NOT_EXIST if the specified table does not exist, DB_INDEX_NOT_FOUND if the specified index does not exist, or DB_SUCCESS if the operation was successful.
 * @note This function retrieves the index information for the specified index name and table name and updates the index_info parameter with the retrieved information.
 */
dberr_t CatalogManager::GetIndex(const std::string& table_name, const std::string& index_name,
	IndexInfo*& index_info) const {
	auto table = index_names_.find(table_name);
	if (table == index_names_.end())
		return DB_TABLE_NOT_EXIST;

	auto index = table->second.find(index_name);
	if (index == table->second.end())
		return DB_INDEX_NOT_FOUND;

	index_info = indexes_.find(index->second)->second;
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Retrieves all the indexes for the specified table.
 * @param table_name The name of the table.
 * @param indexes A vector of IndexInfo pointers that will be populated with the retrieved index information.
 * @return A dberr_t value indicating the result of the operation. Returns DB_TABLE_NOT_EXIST if the specified table does not exist, or DB_SUCCESS if the operation was successful.
 * @note This function retrieves all the indexes for the specified table and populates the indexes vector with the retrieved index information.
 */
dberr_t CatalogManager::GetTableIndexes(const std::string& table_name, std::vector<IndexInfo*>& indexes) const {
	auto table = index_names_.find(table_name);
	if (table == index_names_.end())
		return DB_TABLE_NOT_EXIST;

	for (auto& iter : table->second)
		indexes.push_back(indexes_.find(iter.second)->second);
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Drops the specified table from the catalog.
 * @param table_name The name of the table to drop.
 * @return A dberr_t value indicating the result of the operation. Returns DB_TABLE_NOT_EXIST if the specified table does not exist, or DB_SUCCESS if the operation was successful.
 * @note This function drops the specified table from the catalog, including all associated indexes and metadata.
 */
dberr_t CatalogManager::DropTable(const string& table_name) {
	auto table = table_names_.find(table_name);
	if (table == table_names_.end())
		return DB_TABLE_NOT_EXIST;

	table_id_t table_id = table->second;
	page_id_t page_id = tables_.find(table_id)->second->GetRootPageId();

	table_names_.erase(table);
	while (page_id != INVALID_PAGE_ID) {
		auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id)->GetData());
		page_id = page->GetNextPageId();
		buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
		buffer_pool_manager_->DeletePage(page_id);
	}
	tables_.erase(table_id);
	catalog_meta_->DeleteTableMetaPage(buffer_pool_manager_, table_id);

	catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
	buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 *
 * @brief Drops the specified index from the catalog.
 * @param table_name The name of the table.
 * @param index_name The name of the index to drop.
 * @return A dberr_t value indicating the result of the operation. Returns DB_TABLE_NOT_EXIST if the specified table does not exist, DB_INDEX_NOT_FOUND if the specified index does not exist, or DB_SUCCESS if the operation was successful.
 * @note This function drops the specified index from the catalog, including all associated metadata.
 */
dberr_t CatalogManager::DropIndex(const string& table_name, const string& index_name) {
	auto table = index_names_.find(table_name);
	if (table == index_names_.end())
		return DB_TABLE_NOT_EXIST;

	auto index = table->second.find(index_name);
	if (index == table->second.end())
		return DB_INDEX_NOT_FOUND;

	IndexInfo* index_info = nullptr;
	index_id_t index_id = index->second;
	dberr_t ret = GetIndex(table_name, index_name, index_info);
	if (ret != DB_SUCCESS) return ret;

	index_info->GetIndex()->Destroy();
	index_names_[table_name].erase(index);
	indexes_.erase(index_id);
	catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id);

	catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
	buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Flushes the catalog metadata page to disk.
 * @return A dberr_t value indicating the result of the operation. Returns DB_SUCCESS if the operation was successful.
 * @note This function flushes the catalog metadata page to disk, ensuring that any changes made to the catalog are persisted.
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
	catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
	buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
	buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Loads the table metadata from disk.
 * @param table_id The ID of the table.
 * @param page_id The page ID of the table metadata.
 * @return A dberr_t value indicating the result of the operation. Returns DB_SUCCESS if the operation was successful.
 * @note This function loads the table metadata from the page with the given page ID and creates a table heap and a table info object.
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
	Page* table_page = buffer_pool_manager_->FetchPage(page_id);
	if (table_page == nullptr)
		return DB_FAILED;

	char* buf = table_page->GetData();
	TableMetadata* table_meta = nullptr;
	TableMetadata::DeserializeFrom(buf, table_meta);
	table_names_.emplace(table_meta->GetTableName(), table_id);

	Schema* schema = Schema::DeepCopySchema(table_meta->GetSchema());
	TableHeap* table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), schema, log_manager_, lock_manager_);
	TableInfo* table_info = TableInfo::Create();
	table_info->Init(table_meta, table_heap);
	tables_.emplace(table_id, table_info);

	buffer_pool_manager_->UnpinPage(page_id, false);
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief Loads the index metadata from disk.
 * @param index_id The ID of the index.
 * @param page_id The page ID of the index metadata.
 * @return A dberr_t value indicating the result of the operation. Returns DB_SUCCESS if the operation was successful.
 * @note This function loads the index metadata from the page with the given page ID and creates an index info object.
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
	Page* index_page = buffer_pool_manager_->FetchPage(page_id);
	if (index_page == nullptr)
		return DB_FAILED;

	char* buf = index_page->GetData();
	IndexMetadata* index_meta = nullptr;
	IndexMetadata::DeserializeFrom(buf, index_meta);
	table_id_t table_id = index_meta->GetTableId();
	std::string table_name = tables_.find(table_id)->second->GetTableName();
	index_names_[table_name].emplace(index_meta->GetIndexName(), index_id);

	TableInfo* table_info = tables_.find(table_id)->second;
	IndexInfo* index_info = IndexInfo::Create();
	index_info->Init(index_meta, table_info, buffer_pool_manager_);
	indexes_.emplace(index_id, index_info);

	buffer_pool_manager_->UnpinPage(page_id, false);
	return DB_SUCCESS;
}


/**
 * TODO: Student Implement (finished)
 *
 * @brief Retrieves the table information for the specified table ID.
 * @param table_id The ID of the table to retrieve.
 * @param table_info A pointer to a TableInfo object that will be updated with the retrieved table information.
 * @return A dberr_t value indicating the result of the operation. Returns DB_TABLE_NOT_EXIST if the table does not exist, or DB_SUCCESS if the operation was successful.
 * @note This function retrieves the table information for the specified table ID and updates the table_info parameter with the retrieved information.
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo*& table_info) {
	auto table = tables_.find(table_id);
	if (table == tables_.end())
		return DB_TABLE_NOT_EXIST;
	table_info = table->second;
	return DB_SUCCESS;
}