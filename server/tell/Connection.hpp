/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once
#include <util/Protocol.hpp>

#include <array>
#include <telldb/TellDB.hpp>
#include <boost/asio.hpp>

namespace mbench {

class Transaction {
private: // Types
    using GetFuture = tell::db::Future<tell::db::Tuple>;
public: // types
    using UpdateOp = std::array<std::pair<unsigned, tell::db::Field>, 5>;
    using Tuple = std::vector<tell::db::Field>;
    using Field = tell::db::Field;
private: // members
    tell::db::Transaction& mTx;
    std::vector<tell::db::Tuple::id_t> mFieldIds;
    std::vector<uint64_t> mDelete;
    std::vector<uint64_t> mGet;
    std::vector<std::pair<uint64_t, UpdateOp>> mUpdate;
    std::vector<std::pair<uint64_t, Field>>    mFieldUpdates;
    bool mTableIdSet = false;
    tell::db::table_t mTableId;
public:
    tell::db::table_t tableId() {
        if (!mTableIdSet) {
            auto resF = mTx.openTable("maintable");
            mTableId = resF.get();
        }
        return mTableId;
    }
    tell::db::Tuple::id_t idOfPos(unsigned pos) {
        if (mFieldIds.empty()) initFieldIds();
        return mFieldIds[pos];
    }
    crossbow::string nameOfCol(unsigned col) {
        char name = 'A' + (col % 10);
        crossbow::string colName(&name, 1);
        colName += crossbow::to_string(col / 10 + 1);
        return colName;
    }
    tell::db::Transaction& transaction() {
        return mTx;
    }
private:
    void initFieldIds() {
        auto& schema = mTx.getSchema(tableId());
        auto& fixedSized = schema.fixedSizeFields();
        auto& varSized = schema.varSizeFields();
        std::array<unsigned, 10> occ;
        for (auto& o : occ) {
            o = 0;
        }
        id_t i = 0;
        mFieldIds.resize(fixedSized.size() + varSized.size());
        for (; i < fixedSized.size(); ++i) {
            auto& field = fixedSized[i];
            switch (field.name()[0]) {
            case 'A':
                mFieldIds[0 + 10*occ[0]++] = i;
                break;
            case 'B':
                mFieldIds[1 + 10*occ[1]++] = i;
                break;
            case 'C':
                mFieldIds[2 + 10*occ[2]++] = i;
                break;
            case 'D':
                mFieldIds[3 + 10*occ[3]++] = i;
                break;
            case 'E':
                mFieldIds[4 + 10*occ[4]++] = i;
                break;
            case 'F':
                mFieldIds[5 + 10*occ[5]++] = i;
                break;
            case 'G':
                mFieldIds[6 + 10*occ[6]++] = i;
                break;
            case 'H':
                mFieldIds[7 + 10*occ[7]++] = i;
                break;
            default:
                throw std::runtime_error((boost::format("Unexpected field %1%") % field.name()).str().c_str());
            }
        }
        for (; i - fixedSized.size() < varSized.size(); ++i) {
            auto& field = varSized[i - fixedSized.size()];
            switch (field.name()[0]) {
            case 'I':
                mFieldIds[8 + 10*occ[8]++] = i;
                break;
            case 'J':
                mFieldIds[9 + 10*occ[9]++] = i;
                break;
            default:
                throw std::runtime_error((boost::format("Unexpected field %1%") % field.name()).str().c_str());
            }
        }
    }
    void execGets() {
        auto tId = tableId();
        std::vector<GetFuture> getsF;
        getsF.reserve(mGet.size());
        for (auto key : mGet) {
            getsF.emplace_back(mTx.get(tId, tell::db::key_t{key}));
        }
        for (auto iter = getsF.rbegin(); iter != getsF.rend(); ++iter) {
            iter->wait();
        }
        mGet.clear();
    }
    void execDeletions() {
        auto tId = tableId();
        std::vector<GetFuture> getsF;
        getsF.reserve(mDelete.size());
        for (auto key : mDelete) {
            getsF.emplace_back(mTx.get(tId, tell::db::key_t{key}));
        }
        for (auto i = getsF.size(); i > 0; --i) {
            auto t = getsF[i - 1].get();
            mTx.remove(tId, tell::db::key_t{mDelete[i - 1]}, t);
        }
        mDelete.clear();
    }
    void execUpdates() {
        assert(mUpdate.empty() || mFieldUpdates.empty());
        auto tId = tableId();
        std::vector<GetFuture> getsF;
        getsF.reserve(mUpdate.size() + mFieldUpdates.size());
        for (auto& p : mUpdate) {
            getsF.emplace_back(mTx.get(tId, tell::db::key_t{p.first}));
        }
        for (auto& p : mFieldUpdates) {
            getsF.emplace_back(mTx.get(tId, tell::db::key_t{p.first}));
        }
        for (unsigned i = 0; i < mUpdate.size(); ++i) {
            auto tuple = getsF[i].get();
            auto old = tuple;
            auto& arr = mUpdate[i].second;
            for (auto& p : arr) {
                auto& field = tuple[idOfPos(p.first)];
                assert(field.type() == p.second.type());
                tuple[idOfPos(p.first)] = p.second;
            }
            mTx.update(tId, tell::db::key_t{mUpdate[i].first}, old, tuple);
        }
        for (unsigned i = 0; i < mFieldUpdates.size(); ++i) {
            auto tuple = getsF[i].get();
            auto old = tuple;
            tuple[0] = mFieldUpdates[i].second;
            mTx.update(tId, tell::db::key_t{mFieldUpdates[i].first}, old, tuple);
        }
        mUpdate.clear();
        mFieldUpdates.clear();
    }
public:
    Transaction(tell::db::Transaction& tx)
        : mTx(tx)
    {
        mUpdate.reserve(100);
        mFieldUpdates.reserve(100);
        mDelete.reserve(100);
    }

    void commit() {
        execDeletions();
        execUpdates();
        execGets();
        mTx.commit();
    }

    static Tuple newTuple(unsigned n) {
        return std::vector<tell::db::Field>(n);
    }

    void remove(uint64_t key) {
        mDelete.push_back(key);
    }

    void get(uint64_t key) {
        mGet.push_back(key);
    }

    void update(uint64_t key, const UpdateOp& up) {
        mUpdate.emplace_back(key, up);
    }

    void insert(uint64_t key, Tuple value) {
        auto insTuple = mTx.newTuple(tableId());
        for (unsigned i = 0; i < value.size(); ++i) {
            insTuple[idOfPos(i)] = value[i];
        }
#ifndef NDEBUG
        for (id_t i = 0; i < insTuple.count(); ++i) {
            assert(!insTuple[i].null());
        }
#endif
        mTx.insert(tableId(), tell::db::key_t{key}, insTuple);
    }

    void createSchema(unsigned numCols, unsigned sf) {
        tell::store::Schema schema(tell::store::TableType::TRANSACTIONAL);
        for (unsigned i = 0; i < numCols; ++i) {
            crossbow::string colName = nameOfCol(i);
            tell::store::FieldType type;
            switch (i % 10) {
            case 0:
                type = tell::store::FieldType::DOUBLE;
                break;
            case 1:
                type = tell::store::FieldType::INT;
                break;
            case 2:
                type = tell::store::FieldType::INT;
                break;
            case 3:
                type = tell::store::FieldType::SMALLINT;
                break;
            case 4:
                type = tell::store::FieldType::SMALLINT;
                break;
            case 5:
                type = tell::store::FieldType::BIGINT;
                break;
            case 6:
                type = tell::store::FieldType::BIGINT;
                break;
            case 7:
                type = tell::store::FieldType::DOUBLE;
                break;
            case 8:
                type = tell::store::FieldType::TEXT;
                break;
            case 9:
                type = tell::store::FieldType::TEXT;
            }
            schema.addField(type, colName, true);
        }
        mTx.createTable("maintable", schema);
    }
};

struct TransactionRunner {
    std::function<void(Transaction&)> callback;
    boost::asio::io_service& service;
    std::unique_ptr<tell::db::TransactionFiber<void>> fiber;
    template<class Fun>
    TransactionRunner(Fun&& callback, boost::asio::io_service& service)
        : callback(callback)
        , service(service)
    {}

    void operator() (tell::db::Transaction& tx) {
        Transaction t(tx);
        callback(t);
        service.post([this]() {
            fiber->wait();
            delete this;
        });
    }
};

extern std::unique_ptr<tell::store::ScanMemoryManager> scanMemoryManager;
extern std::mutex memoryManagerMutex;

class Connection {
    tell::db::ClientManager<void> mClientManager;
    boost::asio::io_service& mService;
    size_t mNumStorages;
public: // types
    using string_type = crossbow::string;
public:
    Connection(tell::store::ClientConfig& config, boost::asio::io_service& service, unsigned sf)
        : mClientManager(config)
        , mService(service)
        , mNumStorages(config.tellStore.size())
    {
        if (!scanMemoryManager) {
            std::unique_lock<std::mutex> _(memoryManagerMutex);
            if (!scanMemoryManager) {
                size_t scanSize = size_t(sf)<<20;
                scanSize *= 70;
                size_t numStorages = storageCount();
                auto n = mClientManager.newScanMemoryManager(numStorages, scanSize/numStorages);
                scanMemoryManager.swap(n);
            }
        }
    }

    template<class Callback>
    void startTx(mbench::TxType txType, const Callback& callback) {
        tell::store::TransactionType type;
        switch (txType) {
        case mbench::TxType::RW:
            type = tell::store::TransactionType::READ_WRITE;
            break;
        case mbench::TxType::RO:
            type = tell::store::TransactionType::READ_ONLY;
            break;
        case mbench::TxType::A:
            type = tell::store::TransactionType::ANALYTICAL;
        }
        auto tx = new TransactionRunner(callback, mService);
        tx->fiber.reset(new tell::db::TransactionFiber<void>(mClientManager.startTransaction(
                        [tx](tell::db::Transaction& t) { (*tx)(t); }, type)));
    }

    std::unique_ptr<tell::store::ScanMemoryManager> newScanMemoryManager(size_t chunkCount, size_t chunkSize) {
        return mClientManager.newScanMemoryManager(chunkCount, chunkSize);
    }

    size_t storageCount() const {
        return mNumStorages;
    }
};

} // namespace mbench

