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
#include "Client.hpp"
#include <util/Protocol.hpp>
#include "sqlite3.h"

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

#include <vector>
#include <thread>

namespace mbench {

std::string cmdString(Commands cmd) {
    switch (cmd) {
    case mbench::Commands::CreateSchema:
        return "CreateSchema";
    case mbench::Commands::Populate:
        return "Populate";
    case mbench::Commands::BatchOp:
        return "BatchOp";
    case mbench::Commands::Q1:
        return "Q1";
    case mbench::Commands::Q2:
        return "Q2";
    case mbench::Commands::Q3:
        return "Q3";
    throw std::runtime_error("Invalid command");
    }
    throw std::runtime_error("This has to be dead code");
}

} // namespace mbench

#define sqlOk(code) assertSql(code, __FILE__, __LINE__)

void assertSql(int code, const char* file, int line) {
    if (code != SQLITE_OK) {
        auto msg = (boost::format("ERROR (%1%:%2%): %3%") % file % line % sqlite3_errstr(code)).str();
        throw std::runtime_error(msg.c_str());
    }
}

int main(int argc, const char* argv[]) {
    namespace po = boost::program_options;
    unsigned numThreads = 1;
    unsigned sf;
    unsigned oltpClients;
    bool populate = false;
    unsigned time = 5;
    unsigned numAnalytical;
    unsigned numOps;
    double insProb, delProb, updProb;
    bool noWarmup;
    bool onlyQ1;
    std::string hostStr;
    std::string dbFile("out.db");
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        ("hosts,H", po::value<std::string>(&hostStr)->required(), "Adress to servers")
        ("scaling-factor,s", po::value<unsigned>(&sf)->required(), "Scaling factor")
        ("clients,c", po::value<unsigned>(&oltpClients)->default_value(29), "Number of get/put clients (in total)")
        ("analytical,a", po::value<unsigned>(&numAnalytical)->default_value(0), "Number of analytical clients (in total)")
        ("threads,t", po::value<unsigned>(&numThreads), "Number of network threads")
        ("populate,P", "Run population instead of benchmark")
        ("db,o", po::value<std::string>(&dbFile), "Output to write to")
        ("time", po::value<unsigned>(&time), "Number of minutes to run")
        ("batch-size,b", po::value<unsigned>(&numOps)->default_value(100), "Number of operations per batch")
        ("inserts,i", po::value<double>(&insProb)->default_value(0.166), "Fraction of insert operations")
        ("deletes,d", po::value<double>(&delProb)->default_value(0.166), "Fraction of delete operations")
        ("update,u", po::value<double>(&updProb)->default_value(0.166), "Fraction of update operations")
        ("no-warmup", po::bool_switch(&noWarmup)->default_value(false), "No warm up time")
        ("only-q1,q", po::bool_switch(&onlyQ1)->default_value(false), "Execute only q1")
        ;

    po::variables_map vm;
    store(parse_command_line(argc, argv, desc), vm);
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (vm.count("populate")) {
        populate = true;
    }
    notify(vm);
    if (insProb + updProb + delProb > 1.0) {
        std::cerr << "sum(insert,delete,update) > 1.0\n";
        return 1;
    }

    if (oltpClients == 0 && populate) {
        std::cerr << "can not populate without oltp clients\n";
        return 1;
    }

    if (!noWarmup)
        time += 2;

    std::vector<std::string> hosts;
    boost::split(hosts, hostStr, boost::is_any_of(";,"), boost::token_compress_on);

    boost::asio::io_service service;
    boost::asio::io_service::strand ioStrand(service);
    std::vector<std::unique_ptr<mbench::Client>> clients;
    clients.reserve(oltpClients + numAnalytical);
    boost::asio::ip::tcp::resolver resolver(service);
    std::vector<std::string> hostPort;
    hostPort.reserve(2);

    bool allAnalytical = oltpClients == 0;
    auto hostIter = hosts.begin();
    if (populate) numAnalytical = 0;
    for (unsigned i = 0; i < numAnalytical; ++i) {
        if (hostIter == hosts.end()) hostIter = hosts.begin();
        clients.emplace_back(new mbench::Client(service, ioStrand, sf,
                    oltpClients, 0, true, numOps, insProb, delProb,
                    updProb, onlyQ1));
        auto& client = *clients.back();
        boost::split(hostPort, *hostIter, boost::is_any_of(":"), boost::token_compress_on);
        if (hostPort.size() == 1) {
            boost::asio::connect(client.socket(), resolver.resolve({hostPort[0]}));
        } else if (hostPort.size() == 2) {
            boost::asio::connect(client.socket(), resolver.resolve({hostPort[0], hostPort[1]}));
        } else {
            std::cerr << "Format error while parsing hosts" << std::endl;
            return 1;
        }
        ++hostIter;
    }
    for (unsigned clientId = 0; clientId < oltpClients; ++clientId) {
        if (hostIter == hosts.end()) hostIter = hosts.begin();
        clients.emplace_back(new mbench::Client(service, ioStrand, sf,
                    oltpClients, clientId, false, numOps, insProb,
                    delProb, updProb, onlyQ1));
        auto& client = *clients.back();
        boost::split(hostPort, *hostIter, boost::is_any_of(":"), boost::token_compress_on);
        if (hostPort.size() == 1) {
            boost::asio::connect(client.socket(), resolver.resolve({hostPort[0]}));
        } else if (hostPort.size() == 2) {
            boost::asio::connect(client.socket(), resolver.resolve({hostPort[0], hostPort[1]}));
        } else {
            std::cerr << "Format error while parsing hosts" << std::endl;
            return 1;
        }
        ++hostIter;
    }
    auto duration = std::chrono::minutes(time);
    bool timerChosen = true;
    if (populate) {
        std::cout << "start population\n";
        clients[0]->populate(clients);
    } else {
        for (auto& client : clients) {
            if (timerChosen && allAnalytical) {
                client->run(duration, true);
                timerChosen = false;
            }
            else if (timerChosen && !client->isAnalytical()) {
                client->run(duration, true);
                timerChosen = false;
            } else {
                client->run(duration, false);
            }
        }
    }


    std::cout << "Will run for " << time << " minutes with " << clients.size() << " clients\n";
    auto startTime = mbench::Clock::now();
    std::vector<std::thread> threads;
    threads.reserve(numThreads - 1);
    for (unsigned i = 0; i < numThreads - 1; ++i) {
        threads.emplace_back([&service](){ service.run(); });
    }
    service.run();
    for (auto& t : threads) {
        t.join();
    }
    if (populate) {
        std::cout << "                                                                                                      \r";
        std::cout << "Done\n";
    }
    std::cout << "Done - writing results\n";
    sqlOk(sqlite3_config(SQLITE_CONFIG_SINGLETHREAD));
    sqlite3* db;
    sqlOk(sqlite3_open(dbFile.c_str(), &db));
    sqlOk(sqlite3_exec(db, "CREATE TABLE results(start int, end int, rt int, tx text, success text, msg text)",
                nullptr, nullptr, nullptr));
    sqlOk(sqlite3_exec(db, "CREATE TABLE clientArgs(idx int, param text)", nullptr, nullptr, nullptr));

    sqlOk(sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, nullptr));
    // Insert arguments
    sqlite3_stmt* stmt;
    sqlOk(sqlite3_prepare_v2(db, "INSERT INTO clientArgs VALUES(?, ?)", -1, &stmt, nullptr));
    for (int i = 0; i < argc; ++i) {
        sqlOk(sqlite3_bind_int(stmt, 1, i));
        sqlOk(sqlite3_bind_text(stmt, 2, argv[i], -1, nullptr));
        int s;
        while ((s = sqlite3_step(stmt)) != SQLITE_DONE) {
            if (s == SQLITE_ERROR) {
                throw std::runtime_error(sqlite3_errmsg(db));
            }
        }
        sqlite3_reset(stmt);
    }
    sqlOk(sqlite3_finalize(stmt));
    sqlOk(sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, nullptr));
    // insert data
    sqlOk(sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, nullptr));
    sqlOk(sqlite3_prepare_v2(db, "INSERT INTO results VALUES(?, ?, ?, ?, ?, ?)", -1, &stmt, nullptr));

    for (auto& client : clients) {
        const auto& log = client->log();
        for (const auto& e : log) {
            auto start = int(std::chrono::duration_cast<std::chrono::microseconds>(e.start - startTime).count());
            auto end   = int(std::chrono::duration_cast<std::chrono::microseconds>(e.end - startTime).count());
            std::string trans = cmdString(e.transaction);
            std::string success = e.success ? "true" : "false";
            std::string msg(e.error.data(), e.error.size());
            sqlOk(sqlite3_bind_int(stmt, 1, start));
            sqlOk(sqlite3_bind_int(stmt, 2, end));
            sqlOk(sqlite3_bind_int64(stmt, 3, e.responseTime));
            sqlOk(sqlite3_bind_text(stmt, 4, trans.data(), trans.size(), nullptr));
            sqlOk(sqlite3_bind_text(stmt, 5, success.data(), success.size(), nullptr));
            sqlOk(sqlite3_bind_text(stmt, 6, msg.data(), msg.size(), nullptr));
            int s;
            while ((s = sqlite3_step(stmt)) != SQLITE_DONE) {
                if (s == SQLITE_ERROR) {
                    throw std::runtime_error(sqlite3_errmsg(db));
                }
            }
            sqlite3_reset(stmt);
        }
    }
    sqlOk(sqlite3_finalize(stmt));
    sqlOk(sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, nullptr));

    std::cout << "done\n";
    sqlite3_close(db);
    return 0;
}
