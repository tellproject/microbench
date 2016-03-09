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
#include "Queries.hpp"

#include <server/Server.hpp>
#include <server/main.hpp>

#include <crossbow/allocator.hpp>
#include <telldb/TellDB.hpp>

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
    case mbench::Commands::Q4:
        return "Q4";
    case mbench::Commands::Q5:
        return "Q5";
    }
    throw std::runtime_error("Invalid command");
}
}

int main(int argc, const char* argv[]) {
    crossbow::allocator::init();

    namespace po = boost::program_options;
    return mbench::mainFun<mbench::Connection, mbench::Transaction, tell::store::ClientConfig>(argc, argv,
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

