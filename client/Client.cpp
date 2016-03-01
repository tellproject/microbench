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

uint64_t calcBaseInsertKey(unsigned sf, unsigned numClients, unsigned clientId) {
    auto p = numTuples(sf);
    while (p % numClients != clientId) {
        --p;
    }
    return p;
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

bool Client::done(const Clock::time_point& now) {
    if (now >= mEndTime) {
        return true;
    }
    if (mTimer && mLastTime + std::chrono::seconds(1) >= now) {
        auto duration = now - mLastTime;
        mLastTime = now;
        auto minutes = std::chrono::duration_cast<std::chrono::minutes>(duration).count();
        auto seconds = std::chrono::duration_cast<std::chrono::minutes>(duration).count() % 60;
        std::cout << boost::format("Ran for %1$02d:%2$02d\n")
            % minutes
            % seconds;
    }
    return false;
}

void Client::doRun() {
    auto now = Clock::now();
    if (done(now)) {
        return;
    }
    auto rnd = mDist(mRnd);
    switch (rnd) {
    case 0:
        mClient.execute<Commands::T1>([this, now](
                    const err_code& ec,
                    const typename Signature<Commands::T1>::result& res) {
            assertOk(ec);
            auto end = Clock::now();
            LogEntry l;
            l.success = res.success;
            l.error = res.msg;
            l.transaction = Commands::T1;
            l.start = now;
            l.end = end;
            mBaseInsert = res.lastInsert;
            mLog.emplace_back(std::move(l));
            doRun();
        },
        Signature<Commands::T1>::arguments{mBaseInsert, mNumClients});
    case 1:
        mClient.execute<Commands::T2>([this, now](
                    const err_code& ec,
                    const typename Signature<Commands::T2>::result& res) {
            assertOk(ec);
            auto end = Clock::now();
            LogEntry l;
            l.success = res.success;
            l.error = res.msg;
            l.transaction = Commands::T2;
            l.start = now;
            l.end = end;
            mBaseDelete = res.lastDelete;
            mLog.emplace_back(std::move(l));
            doRun();
        },
        Signature<Commands::T2>::arguments{mBaseDelete, mNumClients});
    case 2:
        mClient.execute<Commands::T3>([this, now](
                    const err_code& ec,
                    const typename Signature<Commands::T3>::result& res) {
            assertOk(ec);
            auto end = Clock::now();
            LogEntry l;
            l.success = res.success;
            l.error = res.msg;
            l.transaction = Commands::T3;
            l.start = now;
            l.end = end;
            mLog.emplace_back(std::move(l));
            doRun();
        },
        Signature<Commands::T3>::arguments{mBaseInsert, mBaseDelete, mNumClients, mClientId});
    case 3:
        mClient.execute<Commands::T5>([this, now](
                    const err_code& ec,
                    const typename Signature<Commands::T5>::result& res) {
            assertOk(ec);
            auto end = Clock::now();
            LogEntry l;
            l.success = res.success;
            l.error = res.msg;
            l.transaction = Commands::T5;
            l.start = now;
            l.end = end;
            mLog.emplace_back(std::move(l));
            doRun();
        },
        Signature<Commands::T5>::arguments{mBaseInsert, mBaseDelete, mNumClients, mClientId});
    default:
        throw std::runtime_error("unexpected query");
    }
}

void Client::doRunAnalytical() {
    auto now = Clock::now();
    if (done(now)) {
        return;
    }
    auto rnd = mDist(mRnd);
    auto fun = [this, now](const err_code& ec, const err_msg& res) {
        assertOk(ec);
        auto end = Clock::now();
        LogEntry l;
        l.success = res.success;
        l.error = res.msg;
        l.transaction = mCurrent;
        l.start = now;
        l.end = end;
        mLog.emplace_back(std::move(l));
    };
    switch (rnd) {
    case 0:
        mCurrent = Commands::Q1;
        mClient.execute<Commands::Q1>(fun);
        break;
    case 1:
        mCurrent = Commands::Q2;
        mClient.execute<Commands::Q2>(fun);
        break;
    case 2:
        mCurrent = Commands::Q3;
        mClient.execute<Commands::Q3>(fun);
        break;
    case 3:
        mCurrent = Commands::Q4;
        mClient.execute<Commands::Q4>(fun);
        break;
    case 4:
        mCurrent = Commands::Q5;
        mClient.execute<Commands::Q5>(fun);
        break;
    default:
        throw std::runtime_error("unexpected query");
    }
}

void Client::run(Clock::duration duration, bool timer) {
    mEndTime = Clock::now() + duration;
    mTimer = timer;
    mLastTime = Clock::now();
    if (mAnalytical) doRunAnalytical();
    else doRun();
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
