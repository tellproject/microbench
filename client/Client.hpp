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
#include <boost/asio.hpp>
#include <util/Protocol.hpp>

namespace mbench {

class Client {
    boost::asio::ip::tcp::socket mSocket;
    boost::asio::io_service::strand mIOStrand;
    crossbow::protocol::Client<Commands, Signature> mClient;
    unsigned mSf;
    void populate(uint64_t start, uint64_t end);
public:
    Client(boost::asio::io_service& service, unsigned sf)
        : mSocket(service)
        , mIOStrand(service)
        , mClient(mSocket)
        , mSf(sf)
    {}
    ~Client() {
        if (mSocket.is_open()) mSocket.close();
    }
    boost::asio::ip::tcp::socket& socket() {
        return mSocket;
    }

    void run() {}
    void populate(const std::vector<std::unique_ptr<Client>>& clients);
};

} // namespace mbench

