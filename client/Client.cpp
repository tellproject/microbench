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
#include <crossbow/logger.hpp>
#include <boost/format.hpp>


namespace mbench {
using err_code = boost::system::error_code;

#define assertOk(t) checkResult(t, __FILE__, __LINE__)

template<class T>
struct result_check;

template<>
struct result_check<std::tuple<bool, crossbow::string>> {
    static void check(const std::tuple<bool, crossbow::string>& result,
            const crossbow::string& file,
            unsigned line) {
        if (!std::get<0>(result)) {
            auto msg = boost::format("error (%1%:%2%): %3%") % file % line % std::get<1>(result);
            throw std::runtime_error(msg.str());
        }
    }
};

template<>
struct result_check<err_code> {
    static void check(const err_code& ec,
            const crossbow::string& file,
            unsigned line) {
        if (ec) {
            auto msg = boost::format("error (%1%:%2%): %3%") % file % line % ec.message();
            throw std::runtime_error(msg.str());
        }
    }
};

template<class T>
void checkResult(const T& e,
        const crossbow::string& file,
        unsigned line) {
    result_check<T>::check(e, file, line);
}

namespace {
std::atomic<unsigned long> lastReported(0);
std::atomic<unsigned long> populated(0);
}

void Client::populate(uint64_t start, uint64_t end) {
    if (start >= end) {
        return;
    }
    using result = Signature<Commands::Populate>::result;
    uint64_t last = std::min(start + 100, end);
    mClient.execute<Commands::Populate>([this, last, end] (const err_code& ec, const result& res) {
        assertOk(ec);
        assertOk(res);
        auto p = populated.fetch_add(100) + 100;
        auto l = lastReported.load();
        if (p > 10000 && lastReported < p - 10000 && lastReported.compare_exchange_strong(l, p)) {
            auto msg = (boost::format("Populated %1% rows\n") % p).str();
            mIOStrand.post([msg](){
                std::cout << msg;
            });
        }
        populate(last, end);
    }, start, last);
}

void Client::populate(const std::vector<std::unique_ptr<Client>>& clients) {
    mClient.execute<Commands::CreateSchema>([this, &clients](const err_code& ec,
                const std::tuple<bool, crossbow::string>& res) {
        assertOk(ec);
        assertOk(res);
        LOG_INFO("Created Schema, start population");
        std::cout << "Created Schema - start population" << std::endl;
        uint64_t numTuples = mSf * 1024 * 1024 * 1024;
        uint64_t tuplesPerClient = numTuples / clients.size();
        for (uint64_t i = 0; i < clients.size(); ++i) {
            if (i + 1 == clients.size()) {
                // last client
                clients[i]->populate(i*tuplesPerClient, numTuples);
            } else {
                clients[i]->populate(i*tuplesPerClient, (i + 1) * tuplesPerClient);
            }
        }
    }, mSf);
}

} // namespace mbench
