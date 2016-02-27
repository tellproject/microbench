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
#include <server/Server.hpp>

#include <boost/program_options.hpp>
#include <thread>

namespace mbench {

template<class Connection, class Transaction, class Config, class ArgFun, class SetupFun>
int mainFun(int argc, const char* argv[], const ArgFun& argFun, const SetupFun& setup) {
    using namespace boost::asio;
    using namespace boost::program_options;
    unsigned numAsioThreads = 1;
    short port = 8713;
    unsigned n = 10;
    options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        ("threads,t", value<unsigned>(&numAsioThreads)->default_value(1), "Number of asio threads")
        ("port,p", value<short>(&port)->default_value(8713), "Port to bind to")
        ("num-columns,n", value<unsigned>(&n)->default_value(10), "Number of columns of table")
        ;
    Config config;
    argFun(desc, config);
    variables_map vm;
    store(parse_command_line(argc, argv, desc), vm);
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    notify(vm);
    if (numAsioThreads == 0) {
        std::cerr << "-t must be at least 1" << std::endl;
        std::terminate();
    }
    setup(vm, config);
    --numAsioThreads;
    io_service service;
    ip::tcp::acceptor acceptor(service, ip::tcp::endpoint(ip::tcp::v4(), port));

    Connection connection(config, service);

    mbench::accept<Connection, Transaction>(acceptor, connection, n);
    std::vector<std::thread> threads;
    threads.reserve(numAsioThreads);
    for (unsigned i = 0; i < numAsioThreads; ++i) {
        threads.emplace_back([&service](){ service.run(); });
    }
    service.run();
    for (auto& t: threads) {
        t.join();
    }
    return 0;
}

} // namespace mbench
