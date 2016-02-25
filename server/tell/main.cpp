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
    tell::db::Transaction& mTx;
    tell::db::table_t tableId() {
        auto resF = mTx.openTable("maintable");
        return resF.get();
    }
public: // types
    using Tuple = std::vector<tell::db::Field>;
public:
    Transaction(tell::db::Transaction& tx)
        : mTx(tx)
    {}

    void commit() {
        std::cout << "Commit\n";
        mTx.commit();
        std::cout << "Commit done\n";
        std::cout.flush();
    }

    Tuple newTuple(unsigned n) {
        return std::vector<tell::db::Field>(n);
    }

    void insert(uint64_t key, Tuple value) {
        std::unordered_map<crossbow::string, tell::db::Field> map;
        map.reserve(value.size());
        for (unsigned i = 0; i < value.size(); ++i) {
            char name = 'A' + (i % 10);
            crossbow::string colName(&name, 1);
            map.emplace(colName, value[i]);
        }
        mTx.insert(tableId(), tell::db::key_t{key}, map);
    }

    void createSchema(unsigned numCols, unsigned sf) {
        tell::store::Schema schema(tell::store::TableType::TRANSACTIONAL);
        for (unsigned i = 0; i < numCols; ++i) {
            char name = 'A' + (i % 10);
            crossbow::string colName(&name, 1);
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

class Connection {
    tell::db::ClientManager<void> mClientManager;
public: // types
    using string_type = crossbow::string;
public:
    Connection(tell::store::ClientConfig& config)
        : mClientManager(config)
    {}

    ~Connection() {
        std::cout << "Delete connection\n";
        std::terminate();
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
        mClientManager.startTransaction([callback](tell::db::Transaction& tx){
            Transaction transaction(tx);
            callback(transaction);
        }, type);
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

