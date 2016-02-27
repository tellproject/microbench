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
#define TELL
#include <server/Server.hpp>
#include <server/main.hpp>

#include <crossbow/allocator.hpp>
#include <telldb/TellDB.hpp>

#include <thread>

class Transaction {
private: // Types
    using UpdateOp = std::array<std::pair<unsigned, tell::db::Field>, 5>;
    using GetFuture = tell::db::Future<tell::db::Tuple>;
public: // types
    using Tuple = std::vector<tell::db::Field>;
    using Field = tell::db::Field;
private: // members
    tell::db::Transaction& mTx;
    tell::db::table_t tableId() {
        auto resF = mTx.openTable("maintable");
        return resF.get();
    }
    std::vector<uint64_t> mDelete;
    std::vector<uint64_t> mGet;
    std::vector<std::pair<uint64_t, UpdateOp>> mUpdate;
    std::vector<std::pair<uint64_t, Field>>    mFieldUpdates;
private:
    crossbow::string nameOfCol(unsigned col) {
        char name = 'A' + (col % 10);
        crossbow::string colName(&name, 1);
        colName += crossbow::to_string(col / 10 + 1);
        return colName;
    }
    void execGets() {
        auto tId = tableId();
        std::vector<GetFuture> getsF;
        getsF.reserve(mGet.size());
        for (auto key : mGet) {
            getsF.emplace_back(mTx.get(tId, tell::db::key_t{key}));
        }
        for (unsigned i = 0; i < mGet.size(); ++i) {
            auto t = getsF[i].get();
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
        for (unsigned i = 0; i < mDelete.size(); ++i) {
            auto t = getsF[i].get();
            mTx.remove(tId, tell::db::key_t{mDelete[i]}, t);
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
                tuple[nameOfCol(p.first)] = p.second;
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

    Tuple newTuple(unsigned n) {
        return std::vector<tell::db::Field>(n);
    }

    void remove(uint64_t key) {
        mDelete.push_back(key);
    }

    void get(uint64_t key) {
        mGet.push_back(key);
    }

    template<class S>
    void update(uint64_t key, unsigned n, S& server) {
        if (n == 1) {
            mFieldUpdates.push_back(std::make_pair(key, server.template rand<0>()));
        } else {
            mUpdate.push_back(std::make_pair(key, server.rndUpdate()));
        }
    }

    void insert(uint64_t key, Tuple value) {
        std::unordered_map<crossbow::string, tell::db::Field> map;
        map.reserve(value.size());
        for (unsigned i = 0; i < value.size(); ++i) {
            map.emplace(nameOfCol(i), value[i]);
        }
        mTx.insert(tableId(), tell::db::key_t{key}, map);
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

class Connection {
    tell::db::ClientManager<void> mClientManager;
    boost::asio::io_service& mService;
public: // types
    using string_type = crossbow::string;
public:
    Connection(tell::store::ClientConfig& config, boost::asio::io_service& service)
        : mClientManager(config)
        , mService(service)
    {}

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
};

int main(int argc, const char* argv[]) {
    crossbow::allocator::init();

    namespace po = boost::program_options;
    return mbench::mainFun<Connection, Transaction, tell::store::ClientConfig>(argc, argv,
            [](po::options_description& desc, tell::store::ClientConfig& config) {
                desc.add_options()
                    ("commit-manager,c", po::value<std::string>()->required(), "Commit manager to bind to")
                    ("storage", po::value<std::string>()->required(), "List of storage nodes")
                    ("network-threads", po::value<unsigned>()->required()->default_value(3),
                        "Number of Infinio threads")
                    ;
            }, [](po::variables_map& vm, tell::store::ClientConfig& config) {
                auto commitManager = vm["commit-manager"].as<std::string>();
                auto storageNodes = vm["storage"].as<std::string>();
                config.commitManager = tell::store::ClientConfig::parseCommitManager(commitManager);
                config.tellStore = tell::store::ClientConfig::parseTellStore(storageNodes);
                config.numNetworkThreads = vm["network-threads"].as<unsigned>();
            });
}

