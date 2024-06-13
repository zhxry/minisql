#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * TODO: Student Implement (finished)
    */
    void Init(CheckPoint &last_checkpoint) {
        persist_lsn_ = last_checkpoint.checkpoint_lsn_;
        active_txns_ = std::move(last_checkpoint.active_txns_);
        data_ = std::move(last_checkpoint.persist_data_);
    }

    /**
    * TODO: Student Implement (finished)
    */
    void RedoPhase() {
        for (auto pos = log_recs_.lower_bound(persist_lsn_); pos != log_recs_.end(); ++pos) {
            LogRecPtr log_rec = pos->second;
            active_txns_[log_rec->txn_id_] = log_rec->lsn_;
            switch (log_rec->type_) {
                case LogRecType::kInsert:
                    data_[log_rec->ins_.first] = log_rec->ins_.second;
                    break;
                case LogRecType::kDelete:
                    data_.erase(log_rec->del_.first);
                    break;
                case LogRecType::kUpdate:
                    data_.erase(log_rec->old_.first);
                    data_[log_rec->new_.first] = log_rec->new_.second;
                    break;
                case LogRecType::kCommit:
                    active_txns_.erase(log_rec->txn_id_);
                    break;
                case LogRecType::kAbort:
                    Rollback(log_rec->txn_id_);
                    active_txns_.erase(log_rec->txn_id_);
                default: break;
            }
        }
    }

    /**
    * TODO: Student Implement (finished)
    */
    void UndoPhase() {
        for (const auto &txn : active_txns_) {
            Rollback(txn.first);
        }
        active_txns_.clear();
    }

    void Rollback(txn_id_t txn_id) {
        for (auto cur = active_txns_[txn_id]; cur != INVALID_LSN; cur = log_recs_[cur]->prev_lsn_) {
            if (log_recs_.find(cur) == log_recs_.end()) break;
            LogRecPtr log_rec = log_recs_[cur];
            switch (log_rec->type_) {
                case LogRecType::kInsert:
                    data_.erase(log_rec->ins_.first);
                    break;
                case LogRecType::kDelete:
                    data_[log_rec->del_.first] = log_rec->del_.second;
                    break;
                case LogRecType::kUpdate:
                    data_.erase(log_rec->new_.first);
                    data_[log_rec->old_.first] = log_rec->old_.second;
                    break;
                default: break;
            }
        }
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

 private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
