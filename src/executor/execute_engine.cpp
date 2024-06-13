#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
	int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
    char path[] = "./databases";
    DIR *dir;
    if ((dir = opendir(path)) == nullptr) {
        mkdir("./databases", 0777);
        dir = opendir(path);
    }
    /** When you have completed all the code for
     *  the test, run it using main.cpp and uncomment
     *  this part of the code.
    **/
    // struct dirent *stdir;
    // while((stdir = readdir(dir)) != nullptr) {
    //     if( strcmp( stdir->d_name , "." ) == 0 ||
    //         strcmp( stdir->d_name , "..") == 0 ||
    //         stdir->d_name[0] == '.')
    //     continue;
    //     dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
    // }
    closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext* exec_ctx,
	const AbstractPlanNodeRef& plan) {
	switch (plan->GetType()) {
		// Create a new sequential scan executor
		case PlanType::SeqScan: {
				return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode*>(plan.get()));
			}
							  // Create a new index scan executor
		case PlanType::IndexScan: {
				return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode*>(plan.get()));
			}
								// Create a new update executor
		case PlanType::Update: {
				auto update_plan = dynamic_cast<const UpdatePlanNode*>(plan.get());
				auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
				return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
			}
							 // Create a new delete executor
		case PlanType::Delete: {
				auto delete_plan = dynamic_cast<const DeletePlanNode*>(plan.get());
				auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
				return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
			}
		case PlanType::Insert: {
				auto insert_plan = dynamic_cast<const InsertPlanNode*>(plan.get());
				auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
				return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
			}
		case PlanType::Values: {
				return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode*>(plan.get()));
			}
		default:
			throw std::logic_error("Unsupported plan type.");
	}
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef& plan, std::vector<Row>* result_set, Txn* txn,
	ExecuteContext* exec_ctx) {
	// Construct the executor for the abstract plan node
	auto executor = CreateExecutor(exec_ctx, plan);

	try {
		executor->Init();
		RowId rid{};
		Row row{};
		while (executor->Next(&row, &rid)) {
			if (result_set != nullptr) {
				result_set->push_back(row);
			}
		}
	}
	catch (const exception& ex) {
		std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
		if (result_set != nullptr) {
			result_set->clear();
		}
		return DB_FAILED;
	}
	return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
	if (ast == nullptr) {
		return DB_FAILED;
	}
	auto start_time = std::chrono::system_clock::now();
	unique_ptr<ExecuteContext> context(nullptr);
	if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
	switch (ast->type_) {
		case kNodeCreateDB:
			return ExecuteCreateDatabase(ast, context.get());
		case kNodeDropDB:
			return ExecuteDropDatabase(ast, context.get());
		case kNodeShowDB:
			return ExecuteShowDatabases(ast, context.get());
		case kNodeUseDB:
			return ExecuteUseDatabase(ast, context.get());
		case kNodeShowTables:
			return ExecuteShowTables(ast, context.get());
		case kNodeCreateTable:
			return ExecuteCreateTable(ast, context.get());
		case kNodeDropTable:
			return ExecuteDropTable(ast, context.get());
		case kNodeShowIndexes:
			return ExecuteShowIndexes(ast, context.get());
		case kNodeCreateIndex:
			return ExecuteCreateIndex(ast, context.get());
		case kNodeDropIndex:
			return ExecuteDropIndex(ast, context.get());
		case kNodeTrxBegin:
			return ExecuteTrxBegin(ast, context.get());
		case kNodeTrxCommit:
			return ExecuteTrxCommit(ast, context.get());
		case kNodeTrxRollback:
			return ExecuteTrxRollback(ast, context.get());
		case kNodeExecFile:
			return ExecuteExecfile(ast, context.get());
		case kNodeQuit:
			return ExecuteQuit(ast, context.get());
		default:
			break;
	}
	// Plan the query.
	Planner planner(context.get());
	std::vector<Row> result_set{};
	try {
		planner.PlanQuery(ast);
		// Execute the query.
		ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
	}
	catch (const exception& ex) {
		std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
		return DB_FAILED;
	}
	auto stop_time = std::chrono::system_clock::now();
	double duration_time =
		double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
	// Return the result set as string.
	std::stringstream ss;
	ResultWriter writer(ss);

	if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
		auto schema = planner.plan_->OutputSchema();
		auto num_of_columns = schema->GetColumnCount();
		if (!result_set.empty()) {
			// find the max width for each column
			vector<int> data_width(num_of_columns, 0);
			for (const auto& row : result_set) {
				for (uint32_t i = 0; i < num_of_columns; i++) {
					data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
				}
			}
			int k = 0;
			for (const auto& column : schema->GetColumns()) {
				data_width[k] = max(data_width[k], int(column->GetName().length()));
				k++;
			}
			// Generate header for the result set.
			writer.Divider(data_width);
			k = 0;
			writer.BeginRow();
			for (const auto& column : schema->GetColumns()) {
				writer.WriteHeaderCell(column->GetName(), data_width[k++]);
			}
			writer.EndRow();
			writer.Divider(data_width);

            // Transforming result set into strings.
            for (const auto &row : result_set) {
                writer.BeginRow();
                for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
                writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
                }
                writer.EndRow();
            }
            writer.Divider(data_width);
        }
        writer.EndInformation(result_set.size(), duration_time, true);
    } else {
        writer.EndInformation(result_set.size(), duration_time, false);
    }
    std::cout << writer.stream_.rdbuf();
    // todo:: use shared_ptr for schema
    if (ast->type_ == kNodeSelect)
        delete planner.plan_->OutputSchema();
    return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
	switch (result) {
		case DB_ALREADY_EXIST:
			cout << "Database already exists." << endl;
			break;
		case DB_NOT_EXIST:
			cout << "Database not exists." << endl;
			break;
		case DB_TABLE_ALREADY_EXIST:
			cout << "Table already exists." << endl;
			break;
		case DB_TABLE_NOT_EXIST:
			cout << "Table not exists." << endl;
			break;
		case DB_INDEX_ALREADY_EXIST:
			cout << "Index already exists." << endl;
			break;
		case DB_INDEX_NOT_FOUND:
			cout << "Index not exists." << endl;
			break;
		case DB_COLUMN_NAME_NOT_EXIST:
			cout << "Column not exists." << endl;
			break;
		case DB_KEY_NOT_FOUND:
			cout << "Key not exists." << endl;
			break;
		case DB_QUIT:
			cout << "Bye." << endl;
			break;
		default:
			break;
	}
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
	string db_name = ast->child_->val_;
	if (dbs_.find(db_name) != dbs_.end()) {
		return DB_ALREADY_EXIST;
	}
	dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
	return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
	string db_name = ast->child_->val_;
	if (dbs_.find(db_name) == dbs_.end()) {
		return DB_NOT_EXIST;
	}
	// remove(db_name.c_str());
	remove(("./databases/" + db_name).c_str());
	delete dbs_[db_name];
	dbs_.erase(db_name);
	if (db_name == current_db_) current_db_ = "";
	return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
	if (dbs_.empty()) {
		cout << "Empty set (0.00 sec)" << endl;
		return DB_SUCCESS;
	}
	int max_width = 8;
	for (const auto& itr : dbs_) {
		if (itr.first.length() > max_width) max_width = itr.first.length();
	}
	cout << "+" << setfill('-') << setw(max_width + 2) << ""
		<< "+" << endl;
	cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
		<< " |" << endl;
	cout << "+" << setfill('-') << setw(max_width + 2) << ""
		<< "+" << endl;
	for (const auto& itr : dbs_) {
		cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
	}
	cout << "+" << setfill('-') << setw(max_width + 2) << ""
		<< "+" << endl;
	return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
	string db_name = ast->child_->val_;
	if (dbs_.find(db_name) != dbs_.end()) {
		current_db_ = db_name;
		cout << "Database changed" << endl;
		return DB_SUCCESS;
	}
	return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
	if (current_db_.empty()) {
		cout << "No database selected" << endl;
		return DB_FAILED;
	}
	vector<TableInfo*> tables;
	if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
		cout << "Empty set (0.00 sec)" << endl;
		return DB_FAILED;
	}
	string table_in_db("Tables_in_" + current_db_);
	uint max_width = table_in_db.length();
	for (const auto& itr : tables) {
		if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
	}
	cout << "+" << setfill('-') << setw(max_width + 2) << ""
		<< "+" << endl;
	cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
	cout << "+" << setfill('-') << setw(max_width + 2) << ""
		<< "+" << endl;
	for (const auto& itr : tables) {
		cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
	}
	cout << "+" << setfill('-') << setw(max_width + 2) << ""
		<< "+" << endl;
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
	if (context == nullptr) {
		std::cout << "No database selected." << std::endl;
		return DB_FAILED;
	}
	auto st = std::chrono::high_resolution_clock::now();
	std::string table_name = ast->child_->val_;
	std::vector<std::string> col_names;
	std::vector<TypeId> col_types;
	std::vector<int> col_lengths;
	std::vector<bool> col_uniques;
	std::vector<std::string> primary_keys;
	pSyntaxNode col_def = ast->child_->next_->child_;
	while (col_def != nullptr) {
		if (col_def->val_ != nullptr && std::string(col_def->val_) == std::string("primary keys")) {
			for (pSyntaxNode key = col_def->child_; key != nullptr; key = key->next_) {
				primary_keys.push_back(key->val_);
			}
		}
		else {
			std::string col_name;
			TypeId type = kTypeInvalid;
			int length = 0;
			if (col_def->val_ != nullptr && std::string(col_def->val_) == std::string("unique")) {
				col_uniques.push_back(true);
			}
			else {
				col_uniques.push_back(false);
			}
			for (pSyntaxNode col_attr = col_def->child_; col_attr != nullptr; col_attr = col_attr->next_) {
				if (col_attr->type_ == kNodeIdentifier) {
					col_name = col_attr->val_;
				}
				else if (col_attr->type_ == kNodeColumnType) {
					if (col_attr->val_ != nullptr && std::string(col_attr->val_) == std::string("int")) {
						type = kTypeInt;
					}
					else if (col_attr->val_ != nullptr && std::string(col_attr->val_) == std::string("float")) {
						type = kTypeFloat;
					}
					else if (col_attr->val_ != nullptr && std::string(col_attr->val_) == std::string("char")) {
						type = kTypeChar;
						ASSERT(col_attr->child_ != nullptr && col_attr->child_->val_ != nullptr, "Invalid column length");
						try { // length < 0 or length is a floating number
							size_t pos;
							length = std::stoi(col_attr->child_->val_, &pos);
							if (pos != std::string(col_attr->child_->val_).length()) {
								std::cout << "Invalid column length" << std::endl;
								return DB_FAILED;
							}
						}
						catch (std::invalid_argument& e) {
							std::cout << "Invalid column length" << std::endl;
							return DB_FAILED;
						}
						if (length < 0) {
							std::cout << "Invalid column length" << std::endl;
							return DB_FAILED;
						}
					}
					else {
						std::cout << "Invalid column type" << std::endl;
						return DB_FAILED;
					}
				}
				else {
					std::cout << "Invalid column attribute" << std::endl;
					return DB_FAILED;
				}
			}
			col_names.push_back(col_name);
			col_types.push_back(type);
			col_lengths.push_back(length);
		}
		col_def = col_def->next_;
	}
	std::vector<Column*> columns;
	for (int i = 0; i < col_names.size(); i++) {
		bool nullable = true;
		if (std::find(primary_keys.begin(), primary_keys.end(), col_names[i]) != primary_keys.end()) {
			nullable = false;
		}
		if (col_types[i] == kTypeChar) {
			columns.push_back(new Column(col_names[i], col_types[i], col_lengths[i], i, nullable, col_uniques[i]));
		}
		else {
			columns.push_back(new Column(col_names[i], col_types[i], i, nullable, col_uniques[i]));
		}
	}
	TableSchema* schema = new TableSchema(columns);
	TableInfo* table_info = nullptr;
	dberr_t res = context->GetCatalog()->CreateTable(table_name, schema, nullptr, table_info);
	if (res != DB_SUCCESS) return res;
	// create index for unique columns
	for (int i = 0; i < col_names.size(); i++) {
		if (col_uniques[i]) {
			std::string index_name = table_name + "_" + col_names[i] + "_index";
			std::vector<std::string> index_keys{ col_names[i] };
			IndexInfo* index_info = nullptr;
			res = context->GetCatalog()->CreateIndex(table_name, index_name, index_keys, nullptr, index_info, "bptree");
			if (res != DB_SUCCESS) return res;
		}
	}
	// create index for primary keys
	if (primary_keys.size() > 0) {
		std::string index_name = table_name + "_primary_keys_index";
		IndexInfo* index_info = nullptr;
		res = context->GetCatalog()->CreateIndex(table_name, index_name, primary_keys, nullptr, index_info, "bptree");
		if (res != DB_SUCCESS) return res;
	}
	delete schema;
	auto ed = std::chrono::high_resolution_clock::now();
	std::cout << "Query OK, 0 rows affected (" << std::chrono::duration<double, std::milli>(ed - st).count() / 1000 << " sec)" << std::endl;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief: the DROP TABLE statement.
 *
 * This function drops a table from the current database. It first checks if a database is selected and if the execution context is valid. Then, it retrieves the table's index information from the catalog. If successful, it drops all the indexes associated with the table and finally drops the table itself from the catalog.
 *
 * @param ast The syntax node representing the DROP TABLE statement.
 * @param context The execution context.
 * @return Returns DB_SUCCESS if the table is dropped successfully, otherwise returns an appropriate error code.
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
	if (current_db_.empty() || context == nullptr) {
		cout << "No database selected" << endl;
		return DB_FAILED;
	}
	auto st = std::chrono::high_resolution_clock::now();
	CatalogManager* catalog = context->GetCatalog();

	string table_name = ast->child_->val_;
	std::vector<IndexInfo*> index_info;
	dberr_t res = catalog->GetTableIndexes(table_name, index_info);
	if (res != DB_SUCCESS) return res;

	for (auto& index : index_info) {
		res = catalog->DropIndex(table_name, index->GetIndexName());
		if (res != DB_SUCCESS) return res;
	}
	res = catalog->DropTable(table_name);
	if (res != DB_SUCCESS) return res;
	auto ed = std::chrono::high_resolution_clock::now();
	std::cout << "Drop table OK (" << std::chrono::duration<double, std::milli>(ed - st).count() / 1000 << " sec)" << std::endl;
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief: the SHOW INDEXES statement.
 *
 * This function retrieves the index information for all tables in the currently selected database
 * and displays it in a formatted table. If no database is selected or the execution context is
 * null, an error message is printed.
 *
 * @param ast The syntax node representing the SHOW INDEXES statement.
 * @param context The execution context.
 * @return The result of the execution. Returns DB_FAILED if no database is selected or the context
 *         is null. Returns DB_SUCCESS otherwise.
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
	if (current_db_.empty() || context == nullptr) {
		std::cout << "No database selected" << endl;
		return DB_FAILED;
	}
	auto st = std::chrono::high_resolution_clock::now();
	CatalogManager* catalog = context->GetCatalog();

	std::vector<TableInfo*> table_info;
	dberr_t res = catalog->GetTables(table_info);
	if (res != DB_SUCCESS) return res;

	std::vector<IndexInfo*> index_info;
	for (auto& table : table_info) {
		res = catalog->GetTableIndexes(table->GetTableName(), index_info);
		if (res != DB_SUCCESS) return res;
	}

	std::vector<int> data_width = { 5 };
	std::vector<std::string> index_names;
	for (auto& index_info : index_info) {
		index_names.push_back(index_info->GetIndexName());
		data_width[0] = std::max(data_width[0], int(index_info->GetIndexName().size()));
	}
	if (!index_names.empty()) {
		std::stringstream ss;
		ResultWriter writer(ss);
		writer.Divider(data_width);
		writer.BeginRow();
		writer.WriteHeaderCell("Index", data_width[0]);
		writer.EndRow();
		writer.Divider(data_width);
		for (auto& index_name : index_names) {
			writer.BeginRow();
			writer.WriteCell(index_name, data_width[0]);
			writer.EndRow();
		}
		writer.Divider(data_width);
		std::cout << ss.rdbuf();

		auto ed = std::chrono::high_resolution_clock::now();
		std::cout << "OK, " << index_names.size() << " rows in set (" << std::chrono::duration<double, std::milli>(ed - st).count() / 1000 << " sec)" << std::endl;
	}
	else {
		auto ed = std::chrono::high_resolution_clock::now();
		std::cout << "Empty set (" << std::chrono::duration<double, std::milli>(ed - st).count() / 1000 << " sec)" << std::endl;
	}
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief: the CREATE INDEX statement.
 *
 * This function creates an index on a specified table and column(s).
 *
 * @param ast The syntax tree node representing the CREATE INDEX statement.
 * @param context The execution context.
 * @return The result of the execution. Returns DB_SUCCESS if the index is created successfully, otherwise returns an error code.
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
	if (current_db_.empty() || context == nullptr) {
		std::cout << "No database selected" << endl;
		return DB_FAILED;
	}
	auto st = std::chrono::high_resolution_clock::now();
	CatalogManager* catalog = context->GetCatalog();
	std::string index_name = ast->child_->val_;
	std::string table_name = ast->child_->next_->val_;
	pSyntaxNode column = ast->child_->next_->next_->child_;
	std::vector<std::string> column_names;
	while (column != nullptr) {
		column_names.push_back(column->val_);
		column = column->next_;
	}

	// create index
	IndexInfo* index_info = nullptr;
	dberr_t res = catalog->CreateIndex(table_name, index_name, column_names, context->GetTransaction(), index_info, "bptree");
	if (res != DB_SUCCESS) return res;

	// create index file
	TableInfo* table_info = nullptr;
	res = catalog->GetTable(table_name, table_info);
	if (res != DB_SUCCESS) return res;
	TableHeap* table_heap = table_info->GetTableHeap();
	std::vector<uint32_t> index_cols;
	column = ast->child_->next_->next_->child_;
	while (column != nullptr) {
		uint32_t index;
		table_info->GetSchema()->GetColumnIndex(column->val_, index);
		index_cols.push_back(index);
		column = column->next_;
	}
	for (auto it = table_heap->Begin(nullptr); it != table_heap->End(); ++it) {
		std::vector<Field> fields;
		for (auto& index : index_cols) {
			fields.push_back(*(it->GetField(index)));
		}
		Row row(fields);
		if (index_info->GetIndex()->InsertEntry(row, it->GetRowId(), nullptr) == DB_FAILED) {
			std::cout << "Duplicate entry!!!" << std::endl;
			context->GetCatalog()->DropIndex(table_name, index_name);
			return DB_FAILED;
		}
	}

	auto ed = std::chrono::high_resolution_clock::now();
	std::cout << "Create index OK (" << std::chrono::duration<double, std::milli>(ed - st).count() / 1000 << " sec)" << std::endl;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief: Executes the DROP INDEX statement.
 *
 * This function is responsible for executing the DROP INDEX statement in the SQL query.
 * It logs the execution of the DROP INDEX statement if the ENABLE_EXECUTE_DEBUG macro is defined.
 * It checks if the current database is selected and if the execution context is valid.
 * It retrieves the index name from the syntax node and searches for the corresponding table.
 * If the index is found, it retrieves the table name and drops the index using the catalog manager.
 * Finally, it prints the execution time and a success message.
 *
 * @param ast The syntax node representing the DROP INDEX statement.
 * @param context The execution context.
 * @return A dberr_t value indicating the result of the execution. In this case, it returns DB_INDEX_NOT_FOUND if the index is not found, or DB_SUCCESS otherwise.
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
	if (current_db_.empty() || context == nullptr) {
		std::cout << "No database selected" << endl;
		return DB_FAILED;
	}

	auto st = std::chrono::high_resolution_clock::now();
	CatalogManager* catalog = context->GetCatalog();
	std::string index_name = ast->child_->val_;
	std::string table_name;
	std::vector<TableInfo*> table_info;
	dberr_t res = catalog->GetTables(table_info);
	if (res != DB_SUCCESS) return res;

	for (auto& table : table_info) {
		std::vector<IndexInfo*> index_info;
		std::vector<std::string> index_names;
		res = catalog->GetTableIndexes(table->GetTableName(), index_info);
		if (res != DB_SUCCESS) return res;

		for (auto& index : index_info) {
			index_names.push_back(index->GetIndexName());
		}
		if (std::find(index_names.begin(), index_names.end(), index_name) != index_names.end()) {
			table_name = table->GetTableName();
			break;
		}
	}
	if (table_name.empty())
		return DB_INDEX_NOT_FOUND;
	res = catalog->DropIndex(table_name, index_name);
	if (res != DB_SUCCESS)
		return res;

	auto ed = std::chrono::high_resolution_clock::now();
	std::cout << "Drop index OK (" << std::chrono::duration<double, std::milli>(ed - st).count() / 1000 << " sec)" << std::endl;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
	return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
	return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
	return DB_FAILED;
}

/**
 * TODO: Student Implement (finished)
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
	auto st = std::chrono::high_resolution_clock::now();
	std::string file_name = ast->child_->val_;
	std::ifstream file(file_name);
	if (!file.is_open())
		return DB_FAILED;

	constexpr int buf_size = 1024;
	char cmd[buf_size];
	while (!file.eof()) {
		memset(cmd, 0, buf_size);
		file.getline(cmd, buf_size, ';');
		cmd[strlen(cmd)] = ';';
		while (cmd[0] == '\n') memmove(cmd, cmd + 1, strlen(cmd));
		if (strlen(cmd) == 1) continue;
		std::cout << "minisql > " << cmd << std::endl;
		YY_BUFFER_STATE bp = yy_scan_string(cmd);
		if (bp == nullptr) {
			LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
			exit(1);
		}
		yy_switch_to_buffer(bp);
		MinisqlParserInit(); // init parser module
		yyparse(); // parse

		// parse result handle
		if (MinisqlParserGetError()) {
			// error
			printf("%s\n", MinisqlParserGetErrorMessage());
		}

		auto result = Execute(MinisqlGetParserRootNode());

		// clean memory after parse
		MinisqlParserFinish();
		yy_delete_buffer(bp);
		yylex_destroy();

		// quit condition
		ExecuteInformation(result);
		if (result == DB_QUIT) {
			auto ed = std::chrono::high_resolution_clock::now();
			std::cout << "Execute file \"" << file_name << "\" OK (" << std::chrono::duration<double, std::milli>(ed - st).count() / 1000 << " sec)" << std::endl;
			return DB_QUIT;
		}
	}
	auto ed = std::chrono::high_resolution_clock::now();
	std::cout << "Execute file \"" << file_name << "\" OK (" << std::chrono::duration<double, std::milli>(ed - st).count() / 1000 << " sec)" << std::endl;
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement (finished)
 *
 * @brief: the QUIT statement.
 *
 * This function is responsible for executing the QUIT statement in the SQL query.
 * It logs the execution of the QUIT statement if the ENABLE_EXECUTE_DEBUG macro is defined.
 *
 * @param ast The syntax node representing the QUIT statement.
 * @param context The execution context.
 * @return A dberr_t value indicating the result of the execution. In this case, it always returns DB_QUIT.
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext* context) {
#ifdef ENABLE_EXECUTE_DEBUG
	LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
	return DB_QUIT;
}
