#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement (finished)
 */
struct LogRec {
    LogRec() = default;
    LogRec(LogRecType type, lsn_t lsn, txn_id_t txn_id, lsn_t prev_lsn)
        : type_(type), lsn_(lsn), txn_id_(txn_id), prev_lsn_(prev_lsn) {}

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};
    std::pair<KeyType, ValType> ins_{}, del_{}, old_{}, new_{};

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

static lsn_t GetPrevLSN(txn_id_t txn_id, lsn_t cur) {
    if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
        lsn_t ret = LogRec::prev_lsn_map_[txn_id];
        LogRec::prev_lsn_map_[txn_id] = cur;
        return ret;
    } else {
        LogRec::prev_lsn_map_.emplace(txn_id, cur);
        return INVALID_LSN;
    }
}

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement (finished)
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    lsn_t lsn = LogRec::next_lsn_++;
    auto log = std::make_shared<LogRec>(LogRecType::kInsert, lsn, txn_id, GetPrevLSN(txn_id, lsn));
    log->ins_ = std::make_pair(std::move(ins_key), ins_val);
    return log;
}

/**
 * TODO: Student Implement (finished)
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    lsn_t lsn = LogRec::next_lsn_++;
    auto log = std::make_shared<LogRec>(LogRecType::kDelete, lsn, txn_id, GetPrevLSN(txn_id, lsn));
    log->del_ = std::make_pair(std::move(del_key), del_val);
    return log;
}

/**
 * TODO: Student Implement (finished)
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    lsn_t lsn = LogRec::next_lsn_++;
    auto log = std::make_shared<LogRec>(LogRecType::kUpdate, lsn, txn_id, GetPrevLSN(txn_id, lsn));
    log->old_ = std::make_pair(std::move(old_key), old_val);
    log->new_ = std::make_pair(std::move(new_key), new_val);
    return log;
}

/**
 * TODO: Student Implement (finished)
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    lsn_t lsn = LogRec::next_lsn_++;
    return std::make_shared<LogRec>(LogRecType::kBegin, lsn, txn_id, GetPrevLSN(txn_id, lsn));
}

/**
 * TODO: Student Implement (finished)
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    lsn_t lsn = LogRec::next_lsn_++;
    return std::make_shared<LogRec>(LogRecType::kCommit, lsn, txn_id, GetPrevLSN(txn_id, lsn));
}

/**
 * TODO: Student Implement (finished)
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    lsn_t lsn = LogRec::next_lsn_++;
    return std::make_shared<LogRec>(LogRecType::kAbort, lsn, txn_id, GetPrevLSN(txn_id, lsn));
}

#endif  // MINISQL_LOG_REC_H
