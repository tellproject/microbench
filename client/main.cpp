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
    case mbench::Commands::T1:
        return "T1";
    case mbench::Commands::T2:
        return "T2";
    case mbench::Commands::T3:
        return "T3";
    case mbench::Commands::T5:
        return "T5";
    case mbench::Commands::Q1:
        return "Q1";
    case mbench::Commands::Q2:
        return "Q2";
    case mbench::Commands::Q3:
        return "Q3";
    case mbench::Commands::Q4:
        return "Q4";
    case mbench::Commands::Q5:
        return "Q5";
    }
    throw std::runtime_error("Invalid command");
}

} // namespace mbench

#define sqlOk(code) assertSql(code, __FILE__, __LINE__)

void assertSql(int code, const char* file, int line) {
    if (code != SQLITE_OK) {
        auto msg = (boost::format("ERROR (%1%:%2%): %3%") % file % line % sqlite3_errstr(code)).str();
        throw std::runtime_error(msg.c_str());
    }
}

std::vector<unsigned> calcAnalyticalDistribution(unsigned numHosts, unsigned numAnayltical) {
    std::vector<unsigned> res(numHosts, 0);
    for (auto& c : res) {
        c = numAnayltical / numHosts;
    }
    auto remaining = numAnayltical % numHosts;
    for (unsigned i = 0; i < remaining; ++i) {
        ++res[i];
    }
    return res;
}

int main(int argc, const char* argv[]) {
    namespace po = boost::program_options;
    unsigned numThreads = 1;
    unsigned sf;
    unsigned clientsPerServer = 1;
    bool populate = false;
    unsigned time = 5;
    unsigned numAnayltical;
    std::string hostStr;
    std::string dbFile("out.db");
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        ("hosts,H", po::value<std::string>(&hostStr)->required(), "Adress to servers")
        ("scaling-factor,s", po::value<unsigned>(&sf)->required(), "Scaling factor")
        ("clients,c", po::value<unsigned>(&clientsPerServer), "Number of clients per server")
        ("analytical,a", po::value<unsigned>(&numAnayltical), "Number of analytical clients (in total)")
        ("threads,t", po::value<unsigned>(&numThreads), "Number of network threads")
        ("populate,P", "Run population instead of benchmark")
        ("db,o", po::value<std::string>(&dbFile), "Output to write to")
        ("time", po::value<unsigned>(&time), "Number of minutes to run")
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

    time += 2;

    std::vector<std::string> hosts;
    boost::split(hosts, hostStr, boost::is_any_of(";,"), boost::token_compress_on);
    unsigned numClients = clientsPerServer * hosts.size();

    if (numAnayltical > numClients) {
        std::cerr << "There can not be more analytical clients than clients in total\n";
        return 1;
    }

    boost::asio::io_service service;
    boost::asio::io_service::strand ioStrand(service);
    std::vector<std::unique_ptr<mbench::Client>> clients;
    clients.reserve(numClients);
    boost::asio::ip::tcp::resolver resolver(service);
    std::vector<std::string> hostPort;
    hostPort.reserve(2);

    auto analyticalClients = calcAnalyticalDistribution(hosts.size(), numAnayltical);
    bool allAnalytical = true;
    unsigned numGetPutClients = numClients - numAnayltical;
    unsigned clientId = 0;
    for (unsigned i = 0; i < hosts.size(); ++i) {
        for (unsigned j = 0; j < clientsPerServer; ++j) {
            bool a = j < analyticalClients[i];
            if (!a) {
                allAnalytical = false;
                clients.emplace_back(new mbench::Client(service, ioStrand, sf, numGetPutClients, clientId++, a));
            } else {
                clients.emplace_back(new mbench::Client(service, ioStrand, sf, numGetPutClients, 0, a));
            }
            auto& client = *clients.back();
            boost::split(hostPort, hosts[i], boost::is_any_of(":"), boost::token_compress_on);
            if (hostPort.size() == 1) {
                boost::asio::connect(client.socket(), resolver.resolve({hostPort[0]}));
            } else if (hostPort.size() == 2) {
                boost::asio::connect(client.socket(), resolver.resolve({hostPort[0], hostPort[1]}));
            } else {
                std::cerr << "Format error while parsing hosts" << std::endl;
                return 1;
            }
        }
    }
    auto duration = std::chrono::minutes(time);
    std::cout << "Will run for " << time << " minutes\n";
    bool timerChosen = true;
    if (populate) {
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


    auto startTime = std::chrono::system_clock::now();
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
        return 0;
    }
    std::cout << "Done - writing results\n";
    sqlOk(sqlite3_config(SQLITE_CONFIG_SINGLETHREAD));
    sqlite3* db;
    sqlOk(sqlite3_open(dbFile.c_str(), &db));
    sqlOk(sqlite3_exec(db, "CREATE TABLE results(start int, end int, tx text, success text, msg text)", nullptr, nullptr, nullptr));

    // Insert data
    sqlOk(sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, nullptr));
    sqlite3_stmt* stmt;
    sqlOk(sqlite3_prepare_v2(db, "INSERT INTO results VALUES(?, ?, ?, ?, ?)", -1, &stmt, nullptr));

    for (auto& client : clients) {
        const auto& log = client->log();
        for (const auto& e : log) {
            auto start = int(std::chrono::duration_cast<std::chrono::milliseconds>(e.start - startTime).count());
            auto end   = int(std::chrono::duration_cast<std::chrono::milliseconds>(e.end - startTime).count());
            std::string trans = cmdString(e.transaction);
            std::string success = e.success ? "true" : "false";
            std::string msg(e.error.data(), e.error.size());
            sqlOk(sqlite3_bind_int(stmt, 1, start));
            sqlOk(sqlite3_bind_int(stmt, 2, end));
            sqlOk(sqlite3_bind_text(stmt, 3, trans.data(), trans.size(), nullptr));
            sqlOk(sqlite3_bind_text(stmt, 4, success.data(), success.size(), nullptr));
            sqlOk(sqlite3_bind_text(stmt, 5, msg.data(), msg.size(), nullptr));
            int s;
            while ((s = sqlite3_step(stmt)) != SQLITE_DONE) {
                if (s == SQLITE_ERROR) {
                    throw std::runtime_error(sqlite3_errmsg(db));
                }
            }
            sqlite3_reset(stmt);
        }
    }
    sqlOk(sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, nullptr));
    sqlOk(sqlite3_finalize(stmt));

    std::cout << "Inserted data, calculating results...\n";
    std::string getPutTP = (boost::format(
            "SELECT count(*)/%1% "
            "FROM results "
            "WHERE tx LIKE 'T%%' "
            "AND start >= 60000 AND end <= 360000 AND success LIKE 'true'"
            ) % double((time - 1)*60)).str();
    std::string scanTP = (boost::format(
            "SELECT count(*)/%1% "
            "FROM results "
            "WHERE tx LIKE 'Q%%' "
            "AND start >= 60000 AND end <= 360000 AND success LIKE 'true'"
            ) % double((time - 1)*60)).str();
    std::string responseTime = (boost::format(
            "SELECT tx, avg(end-start) "
            "FROM results "
            "WHERE start >= 60000 AND end <= 360000 AND success LIKE 'true' "
            "GROUP BY tx;"
            )).str();
    std::cout << "Get/Put throughput:\n";
    std::cout << "===================\n";
    sqlOk(sqlite3_prepare_v2(db, getPutTP.data(), getPutTP.size() + 1, &stmt, nullptr));
    int s;
    while ((s = sqlite3_step(stmt)) != SQLITE_DONE) {
        if (s == SQLITE_ERROR) throw std::runtime_error(sqlite3_errmsg(db));
        double tp = sqlite3_column_double(stmt, 0);
        std::cout << tp << " /second\n";
    }
    sqlOk(sqlite3_finalize(stmt));
    sqlOk(sqlite3_prepare_v2(db, scanTP.data(), scanTP.size() + 1, &stmt, nullptr));
    std::cout << "Scan throughput:\n";
    std::cout << "================\n";
    while ((s = sqlite3_step(stmt)) != SQLITE_DONE) {
        if (s == SQLITE_ERROR) throw std::runtime_error(sqlite3_errmsg(db));
        double tp = sqlite3_column_double(stmt, 0);
        std::cout << tp << " /second\n";
    }
    sqlOk(sqlite3_finalize(stmt));
    sqlOk(sqlite3_prepare_v2(db, responseTime.data(), responseTime.size() + 1, &stmt, nullptr));
    std::cout << "Response Times:\n";
    std::cout << "================\n";
    while ((s = sqlite3_step(stmt)) != SQLITE_DONE) {
        if (s == SQLITE_ERROR) throw std::runtime_error(sqlite3_errmsg(db));
        auto name = sqlite3_column_text(stmt, 0);
        double rt = sqlite3_column_double(stmt, 1);
        std::cout << name << ": " << rt << std::endl;
    }
    std::cout << "done";
    sqlite3_close(db);
}
