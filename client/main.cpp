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

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include <vector>
#include <thread>

int main(int argc, const char* argv[]) {
    namespace po = boost::program_options;
    unsigned numThreads = 1;
    unsigned sf;
    unsigned clientsPerServer = 1;
    bool populate = false;
    std::string hostStr;
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        ("hosts,H", po::value<std::string>(&hostStr)->required(), "Adress to servers")
        ("scaling-factor,s", po::value<unsigned>(&sf)->required(), "Scaling factor")
        ("clients,c", po::value<unsigned>(&clientsPerServer), "Number of clients per server")
        ("threads,t", po::value<unsigned>(&numThreads), "Number of network threads")
        ("populate,P", "Run population instead of benchmark")
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

    std::vector<std::string> hosts;
    boost::split(hosts, hostStr, boost::is_any_of(";,"), boost::token_compress_on);
    unsigned numClients = clientsPerServer * hosts.size();

    boost::asio::io_service service;
    std::vector<std::unique_ptr<mbench::Client>> clients;
    clients.reserve(numClients);
    boost::asio::ip::tcp::resolver resolver(service);
    std::vector<std::string> hostPort;
    hostPort.reserve(2);
    for (unsigned i = 0; i < hosts.size(); ++i) {
        for (unsigned j = 0; j < clientsPerServer; ++j) {
            clients.emplace_back(new mbench::Client(service, sf));
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
    if (populate) {
        clients[0]->populate(clients);
    } else {
        for (auto& client : clients) {
            client->run();
        }
    }


    std::vector<std::thread> threads;
    threads.reserve(numThreads - 1);
    for (unsigned i = 0; i < numThreads - 1; ++i) {
        threads.emplace_back([&service](){ service.run(); });
    }
    service.run();
    for (auto& t : threads) {
        t.join();
    }
}
