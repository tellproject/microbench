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
#include <boost/format.hpp>

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
public:
    Transaction(kudu::client::KuduClient& client)
        : mClient(client)
    {}

    void createSchema(unsigned numCols) {
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
        assertOk(creator->Create());
    }

    void commit() {}
};

struct ConnectionConfig {
    std::string master;
};

class Connection {
    std::tr1::shared_ptr<kudu::client::KuduClient> mClient;
public:
    Connection(const ConnectionConfig& config) {
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
                        po::value<std::string>(&config.master),
                        "Address to kudu master")
                    ;
            }, [](po::variables_map& vm, ConnectionConfig& config) {
            });
}
