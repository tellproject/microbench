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

#include <kudu/client/client.h>
#include <kudu/client/row_result.h>

#include <boost/format.hpp>
#include <boost/variant.hpp>
#include <mutex>

#define assertOk(s) kuduAssert(s, __FILE__, __LINE__)

void kuduAssert(kudu::Status status, const char* file, unsigned line) {
    if (!status.ok()) {

        std::string msg = (boost::format("Error (%1%:%2%): ")
                % file % line % status.message().ToString().c_str()).str();
        throw std::runtime_error(msg);
    }
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
        return *mSession;
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
public: // types
    using Field = boost::variant<int16_t, int32_t, int64_t, float, double, std::string>;
    using Tuple = std::vector<Field>;
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

    template<class S>
    void update(uint64_t key, unsigned n, S& server) {
        auto& table = this->table();
        std::unique_ptr<kudu::client::KuduUpdate> upd(table.NewUpdate());
        auto row = upd->mutable_row();
        assertOk(row->SetInt64(0, key));
        if (n == 1) {
            assertOk(row->SetDouble(1, server.template rand<0>()));
        } else {
            auto cols = server.rndUpdate();
            SetVisitor visitor(*row);
            for (auto& p : cols) {
                visitor.setIdx(p.first);
                boost::apply_visitor(visitor, p.second);
            }
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

    void insert(uint64_t key, Tuple& value) {
        auto& table = this->table();
        std::unique_ptr<kudu::client::KuduInsert> ins(table.NewInsert());
        auto row = ins->mutable_row();
        SetVisitor visitor(*row);
        assertOk(row->SetInt64(0, key));
        for (int i = 0; i < value.size(); ++i) {
            visitor.setIdx(i + 1);
            boost::apply_visitor(visitor, value[i + 1]);
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
            unsigned colNum = i + 1;
            char name = 'A' + (i % 10);
            std::string colName = name + std::to_string(colNum);
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
        auto increment = (sf * 1024*1024) / numTablets;
        for (int64_t i = 0; i < numTablets; ++i) {
            auto row = schema.NewRow();
            assertOk(row->SetInt64(0, i*increment));
            splits.emplace_back(row);
        }
        creator->split_rows(splits);

        assertOk(creator->Create());
    }

    void commit() {
        if (mSession) {
            assertOk(mSession->Flush());
        }
    }
};

struct ConnectionConfig {
    std::string master;
};

class Connection {
    std::tr1::shared_ptr<kudu::client::KuduClient> mClient;
public: // types
    using string_type = std::string;
public:
    Connection(const ConnectionConfig& config, boost::asio::io_service&) {
        kudu::client::KuduClientBuilder builder;
        builder.add_master_server_addr(config.master);
        builder.Build(&mClient);
    }

    template<class Callback>
    void startTx(mbench::TxType txType, const Callback& callback) {
    }
};

int main(int argc, const char* argv[]) {
    namespace po = boost::program_options;
    return mbench::mainFun<Connection, Transaction, ConnectionConfig>(argc, argv,
            [](po::options_description& desc, ConnectionConfig& config) {
                desc.add_options()
                    ("master,c",
                        po::value<std::string>(&config.master)->required(),
                        "Address to kudu master")
                    ;
            }, [](po::variables_map& vm, ConnectionConfig& config) {
            });
}
