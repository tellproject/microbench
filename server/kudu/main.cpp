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
#include <server/Server.hpp>
#include <server/main.hpp>
#include <server/Queries.hpp>

#include <kudu/client/client.h>
#include <kudu/client/row_result.h>

#include <boost/format.hpp>
#include <boost/variant.hpp>
#include <mutex>

#define assertOk(s) kuduAssert(s, __FILE__, __LINE__)

// KUDU helper functions

void kuduAssert(kudu::Status status, const char* file, unsigned line) {
    if (!status.ok()) {
        std::string msg = (boost::format("Error (%1%:%2%): %3%")
                % file % line % status.message().ToString().c_str()).str();
        throw std::runtime_error(msg);
    }
}

using namespace kudu;
using namespace kudu::client;

using ScannerList = std::vector<std::unique_ptr<KuduScanner>>;

template<class T>
struct is_string {
    constexpr static bool value = false;
};

template<size_t S>
struct is_string<char[S]> {
    constexpr static bool value = true;
};

template<>
struct is_string<const char*> {
    constexpr static bool value = true;
};

template<>
struct is_string<std::string> {
    constexpr static bool value = true;
};

template<class T>
struct CreateValue {
    template<class U = T>
    typename std::enable_if<std::is_integral<U>::value, KuduValue*>::type
    create(U v) {
        return KuduValue::FromInt(v);
    }

    template<class U = T>
    typename std::enable_if<std::is_floating_point<U>::value, KuduValue*>::type
    create(U v) {
        return KuduValue::FromDouble(v);
    }

    template<class U = T>
    typename std::enable_if<is_string<U>::value, KuduValue*>::type
    create(const U& v) {
        return KuduValue::CopyString(v);
    }
};


template<class... P>
struct PredicateAdder;

template<class Key, class Value, class CompOp, class... Tail>
struct PredicateAdder<Key, Value, CompOp, Tail...> {
    PredicateAdder<Tail...> tail;
    CreateValue<Value> creator;

    void operator() (KuduTable& table, KuduScanner& scanner, const Key& key,
            const Value& value, const CompOp& predicate, const Tail&... rest) {
        assertOk(scanner.AddConjunctPredicate(table.NewComparisonPredicate(key, predicate, creator.template create<Value>(value))));
        tail(table, scanner, rest...);
    }
};

template<>
struct PredicateAdder<> {
    void operator() (KuduTable& table, KuduScanner& scanner) {
    }
};

template<class... Args>
void addPredicates(KuduTable& table, KuduScanner& scanner, const Args&... args) {
    PredicateAdder<Args...> adder;
    adder(table, scanner, args...);
}

// opens a scan an puts it in the scanner list, use an empty projection-vector for getting everything
template<class... Args>
KuduScanner &openScan(KuduTable& table, ScannerList& scanners, const std::vector<std::string> &projectionColumns, const Args&... args) {
    scanners.emplace_back(new KuduScanner(&table));
    auto& scanner = *scanners.back();
    addPredicates(table, scanner, args...);
    if (projectionColumns.size() > 0) {
        assertOk(scanner.SetProjectedColumnNames(projectionColumns));
    }
    assertOk(scanner.Open());
    return scanner;
}

void getField(KuduRowResult &row,  int columnIdx, int16_t &result) {
    assertOk(row.GetInt16(columnIdx, &result));
}

void getField(KuduRowResult &row,  int columnIdx, int32_t &result) {
    assertOk(row.GetInt32(columnIdx, &result));
}

void getField(KuduRowResult &row,  int columnIdx, int64_t &result) {
    assertOk(row.GetInt64(columnIdx, &result));
}

void getField(KuduRowResult &row,  int columnIdx, double &result) {
    assertOk(row.GetDouble(columnIdx, &result));
}

namespace mbench {

static std::string nameOfCol(unsigned col) {
    unsigned colNum = col + 1;
    char name = 'A' + (col % 10);
    return name + std::to_string(colNum);
}

class Transaction {
    kudu::client::KuduClient& mClient;
    std::tr1::shared_ptr<kudu::client::KuduSession> mSession;
    static std::tr1::shared_ptr<kudu::client::KuduTable> mTable;
    static std::mutex mMutex;
private:
    kudu::client::KuduSession& session() {
        if (mSession) {
            return *mSession;
        }
        mSession = mClient.NewSession();
        assertOk(mSession->SetFlushMode(kudu::client::KuduSession::MANUAL_FLUSH));
        mSession->SetTimeoutMillis(60000);
        return *mSession;
    }
    
public: // types
    using Field = boost::variant<int16_t, int32_t, int64_t, float, double, std::string>;
    using Tuple = std::vector<Field>;
    using UpdateOp = std::array<std::pair<unsigned, Field>, 5>;
    class SetVisitor : public boost::static_visitor<> {
        kudu::KuduPartialRow& row;
        int idx;
    public:
        SetVisitor(kudu::KuduPartialRow& row) : row(row) {}
        void setIdx(int i) { idx = i; }

        void operator() (int16_t v) {
            assertOk(row.SetInt16(idx, v));
        }
        void operator() (int32_t v) {
            assertOk(row.SetInt32(idx, v));
        }
        void operator() (int64_t v) {
            assertOk(row.SetInt64(idx, v));
        }
        void operator() (float v) {
            assertOk(row.SetFloat(idx, v));
        }
        void operator() (double v) {
            assertOk(row.SetDouble(idx, v));
        }
        void operator() (const std::string& v) {
            assertOk(row.SetStringCopy(idx, v));
        }
    };
public:
    Transaction(kudu::client::KuduClient& client)
        : mClient(client)
    {}

    static Tuple newTuple(unsigned n) {
        return std::vector<Field>(n);
    }

    kudu::client::KuduTable& table() {
        if (!mTable) {
            std::lock_guard<std::mutex> _(mMutex);
            if (!mTable) {
                assertOk(mClient.OpenTable("maintable", &mTable));
            }
        }
        return *mTable;
    }

    void update(uint64_t key, const UpdateOp& up) {
        auto& table = this->table();
        std::unique_ptr<kudu::client::KuduUpdate> upd(table.NewUpdate());
        auto row = upd->mutable_row();
        assertOk(row->SetInt64(0, key));
        SetVisitor visitor(*row);
        for (auto& p : up) {
            visitor.setIdx(p.first);
            boost::apply_visitor(visitor, p.second);
        }
        auto& session = this->session();
        assertOk(session.Apply(upd.release()));
    }

    void get(uint64_t key) {
        auto& table = this->table();
        kudu::client::KuduScanner scanner(&table);
        auto pre = table.NewComparisonPredicate(0, kudu::client::KuduPredicate::EQUAL, kudu::client::KuduValue::FromInt(int64_t(key)));
        assertOk(scanner.AddConjunctPredicate(pre));
        assertOk(scanner.Open());
        if (scanner.HasMoreRows()) {
            std::vector<kudu::client::KuduRowResult> res;
            scanner.NextBatch(&res);
            assert(res.size() == 1);
        }
    }

    void remove(uint64_t key) {
        auto& table = this->table();
        std::unique_ptr<kudu::client::KuduDelete> del(table.NewDelete());
        assertOk(del->mutable_row()->SetInt64(0, key));
        auto& session = this->session();
        assertOk(session.Apply(del.release()));
    }

    void insert(uint64_t key, const Tuple &value) {
        auto& table = this->table();
        std::unique_ptr<kudu::client::KuduInsert> ins(table.NewInsert());
        auto row = ins->mutable_row();
        SetVisitor visitor(*row);
        assertOk(row->SetInt64(0, key));
        for (uint i = 0; i < value.size(); ++i) {
            visitor.setIdx(i + 1);
            boost::apply_visitor(visitor, value[i]);
        }
        auto& session = this->session();
        assertOk(session.Apply(ins.release()));
    }

    void createSchema(unsigned numCols, unsigned sf) {
        std::unique_ptr<kudu::client::KuduTableCreator> creator(mClient.NewTableCreator());
        creator->table_name("maintable");
        kudu::client::KuduSchema schema;
        kudu::client::KuduSchemaBuilder schemaBuilder;


        // Add primary
        auto col = schemaBuilder.AddColumn("P");
        col->NotNull()->Type(kudu::client::KuduColumnSchema::INT64);
        col->PrimaryKey();

        for (unsigned i = 0; i < numCols; ++i) {
            std::string colName= nameOfCol(i);
            kudu::client::KuduColumnSchema::DataType type;
            switch (i % 10) {
            case 0:
                type = kudu::client::KuduColumnSchema::DOUBLE;
                break;
            case 1:
                type = kudu::client::KuduColumnSchema::INT32;
                break;
            case 2:
                type = kudu::client::KuduColumnSchema::INT32;
                break;
            case 3:
                type = kudu::client::KuduColumnSchema::INT16;
                break;
            case 4:
                type = kudu::client::KuduColumnSchema::INT16;
                break;
            case 5:
                type = kudu::client::KuduColumnSchema::INT64;
                break;
            case 6:
                type = kudu::client::KuduColumnSchema::INT64;
                break;
            case 7:
                type = kudu::client::KuduColumnSchema::DOUBLE;
                break;
            case 8:
                type = kudu::client::KuduColumnSchema::STRING;
                break;
            case 9:
                type = kudu::client::KuduColumnSchema::STRING;
            }
            schemaBuilder.AddColumn(colName)->NotNull()->Type(type);
        }

        schemaBuilder.Build(&schema);
        creator->schema(&schema);
        creator->num_replicas(1);
        // add splits
        std::vector<kudu::client::KuduTabletServer*> servers;
        assertOk(mClient.ListTabletServers(&servers));
        auto numTablets = 8*servers.size();
        std::vector<const kudu::KuduPartialRow*> splits;
        auto increment = numTuples(sf) * 2 / numTablets; // For now create a domain that is 2 times bigger than population.
        for (uint64_t i = 0; i < numTablets; ++i) {
            auto row = schema.NewRow();
            assertOk(row->SetInt64(0, i*increment));
            splits.emplace_back(row);
        }
        creator->split_rows(splits);

        assertOk(creator->Create());
        creator.reset(nullptr);
    }

    void commit() {
        if (mSession.get()) {
            assertOk(mSession->Flush());
        }
    }
};

std::tr1::shared_ptr<kudu::client::KuduTable> Transaction::mTable;
std::mutex Transaction::mMutex;

struct ConnectionConfig {
    std::string master;
};

class Connection {
    std::tr1::shared_ptr<kudu::client::KuduClient> mClient;
public: // types
    using string_type = std::string;
public:
    Connection(const ConnectionConfig& config, boost::asio::io_service&, unsigned) {
        kudu::client::KuduClientBuilder builder;
        builder.add_master_server_addr(config.master);
        builder.Build(&mClient);
    }

    template<class Callback>
    void startTx(mbench::TxType txType, const Callback& callback) {
        Transaction tx ((*mClient));
        callback(tx);
    }
};

template<template <class, class> class Server>
struct ScanContext<Server, Connection, Transaction>
: public ContextBase<ScanContext<Server, Connection, Transaction>, unsigned, crossbow::string> {
    using string = crossbow::string;
    using AggregationFunction = unsigned;

    Server<Connection, Transaction>& server;

    ScanContext(Server<Connection, Transaction>& server)
        : server(server)
    {}

    // dummy implemenations, need to be changed if necessary!!
    constexpr unsigned sum() const {
        return 0;
    }

    constexpr unsigned count() const {
        return 0; 
    }

    constexpr unsigned min() const {
        return 0; 
    }

    constexpr unsigned max() const {
        return 0; 
    }

    std::mt19937_64& rnd() {
        return server.mRnd;
    }

    unsigned N() const {
        return server.N();
    }

    const string& rndSyllable() {
        return server.rndSyllable();
    }
};

template<template <class, class> class Server>
struct Q1<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q1(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        std::vector<std::string> projection;
        int maxColIdx = projection.size();
        projection.push_back(nameOfCol(0));

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(tx.table(), scanners, projection);
        double max = 0, val;
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
            for (KuduRowResult row : resultBatch) {
                getField(row, maxColIdx, val);
                max = std::max(max, val);
            }
        }
        tx.commit();
    }
};

template<template <class, class> class Server>
struct Q2<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q2(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        unsigned iU, jU;
        std::tie(iU,jU) = scanContext.template rndTwoCols<'A'>();
        std::vector<std::string> projection;
        int maxColIdx = projection.size();
        projection.push_back(nameOfCol(0));
        std::string filterColName = nameOfCol(7);

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(tx.table(), scanners, projection,
                filterColName, double(0.0), KuduPredicate::GREATER_EQUAL,
                filterColName, double(0.5), KuduPredicate::LESS_EQUAL);
        double max = 0, val;
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
            for (KuduRowResult row : resultBatch) {
                getField(row, maxColIdx, val);
                max = std::max(max, val);
            }
        }
        tx.commit();
    }
};

template<template <class, class> class Server>
struct Q3<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q3(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        std::vector<std::string> projection;
        std::string filterColName = nameOfCol(4);

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(tx.table(), scanners, projection,
                filterColName, int16_t(0), KuduPredicate::GREATER_EQUAL,
                filterColName, int16_t(128), KuduPredicate::LESS_EQUAL);
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
        }
        tx.commit();
    }
};

} // namespace mbench

int main(int argc, const char* argv[]) {
    namespace po = boost::program_options;
    return mbench::mainFun<mbench::Connection, mbench::Transaction, mbench::ConnectionConfig>(argc, argv,
            [](po::options_description& desc, mbench::ConnectionConfig& config) {
                desc.add_options()
                    ("master,c",
                        po::value<std::string>(&config.master)->required(),
                        "Address to kudu master")
                    ;
            }, [](po::variables_map& vm, mbench::ConnectionConfig& config) {
            });
}
