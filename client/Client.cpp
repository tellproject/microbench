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

#define assertOk(client, t, cmd) checkResult(client, t, cmd, __FILE__, __LINE__)

template<class T>
struct result_check {
    static void check(
            const Client& client,
            const T& res,
            Commands cmd,
            const crossbow::string& file,
            unsigned line) {
        if (res.success) return;
        auto msg = boost::format("error in %1% (%2%:%3%): %4% (Client %5%/%6%)")
            % cmdString(cmd) % file % line % res.msg
            % client.clientId() % client.numClients();
        throw std::runtime_error(msg.str());
    }
};

template<>
struct result_check<std::tuple<bool, crossbow::string>> {
    static void check(
            const Client& client,
            const std::tuple<bool, crossbow::string>& result,
            Commands cmd,
            const crossbow::string& file,
            unsigned line) {
        if (!std::get<0>(result)) {
            auto msg = boost::format("error in %1% (%2%:%3%): %4% (Client %5%/%6%)")
                % cmdString(cmd) % file % line % std::get<1>(result)
                % client.clientId() % client.numClients();
            throw std::runtime_error(msg.str());
        }
    }
};

template<>
struct result_check<err_code> {
    static void check(
            const Client& client,
            const err_code& ec,
            Commands cmd,
            const crossbow::string& file,
            unsigned line) {
        if (ec) {
            auto msg = boost::format("error in %1% (%2%:%3%): %4% (Client %5%/%6%)")
                % cmdString(cmd) % file % line % ec.message()
                % client.clientId() % client.numClients();
            throw std::runtime_error(msg.str());
        }
    }
};

template<class T>
void checkResult(
        const Client& client,
        const T& e,
        Commands cmd,
        const crossbow::string& file,
        unsigned line) {
    result_check<T>::check(client, e, cmd, file, line);
}

uint64_t calcBaseInsertKey(unsigned sf, unsigned numClients, unsigned clientId) {
    auto p = numTuples(sf) - 1;
    if (numClients == 0) return 0;
    while (p % numClients != clientId) {
        --p;
    }
    return p;
}


namespace {
std::atomic<unsigned long> lastReported(0);
std::atomic<unsigned long> populated(0);
bool firstCallToPrint= true;
}

void Client::populate(uint64_t start, uint64_t end) {
    if (start >= end) {
        return;
    }
    using result = Signature<Commands::Populate>::result;
    uint64_t last = std::min(start + 100, end);
    mClient.execute<Commands::Populate>([this, last, end] (const err_code& ec, const result& res) {
        assertOk(*this, ec, Commands::Populate);
        assertOk(*this, res, Commands::Populate);
        auto p = populated.fetch_add(100) + 100;
        auto l = lastReported.load();
        auto percentage = p*100/mNumTuples;
        if (percentage != l * 100 / mNumTuples && lastReported.compare_exchange_strong(l, p)) {
            mIOStrand.post([percentage](){
                std::cout << '[';
                for (unsigned i = 0; i < 100; ++i) {
                    if (i < percentage) std::cout << '=';
                    else std::cout << ' ';
                }
                std::cout << ']' << '\r';
                std::cout.flush();
            });
        }
        populate(last, end);
    }, start, last);
}

bool Client::done(const Clock::time_point& now) {
    if (now >= mEndTime) {
        mIOStrand.post([this](){
            if (mTimer) std::cout << std::endl;
            std::cout << "Client " << mClientId << " done\n";
        });
        return true;
    }
    if (mTimer && now - std::chrono::seconds(1) >= mLastTime) {
        mLastTime = now;
        auto start = mStartTime;
        mIOStrand.post([now, start]() {
            auto duration = now - start;
            auto minutes = std::chrono::duration_cast<std::chrono::minutes>(duration).count();
            auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
            if (!firstCallToPrint) {
                firstCallToPrint = false;
            } else {
                std::cout << "\b\b\b\b\b\033[K";
            }
            std::cout << boost::format("%1$02d:%2$02d")
                % minutes
                % (seconds % 60);
            std::cout.flush();
        });
    }
    return false;
}

void Client::doRun() {
    auto now = Clock::now();
    if (done(now)) {
        return;
    }
    auto rnd = mDist(mRnd);
    if (mBaseDelete + mNumClients*100 >= mBaseInsert)
        rnd = 0;
    switch (rnd) {
    case 0:
        mClient.execute<Commands::T1>([this, now](
                    const err_code& ec,
                    const typename Signature<Commands::T1>::result& res) {
            assertOk(*this, ec, Commands::T1);
            assertOk(*this, res, Commands::T1);
            auto end = Clock::now();
            LogEntry l;
            l.success = res.success;
            l.error = res.msg;
            l.transaction = Commands::T1;
            l.start = now;
            l.end = end;
            l.responseTime = res.responseTime;
            mBaseInsert = res.lastInsert;
            mLog.emplace_back(std::move(l));
            doRun();
        },
        Signature<Commands::T1>::arguments{mBaseInsert, mNumClients});
        break;
    case 1:
        mClient.execute<Commands::T2>([this, now](
                    const err_code& ec,
                    const typename Signature<Commands::T2>::result& res) {
            assertOk(*this, ec, Commands::T2);
            assertOk(*this, res, Commands::T2);
            auto end = Clock::now();
            LogEntry l;
            l.success = res.success;
            l.error = res.msg;
            l.transaction = Commands::T2;
            l.start = now;
            l.end = end;
            l.responseTime = res.responseTime;
            mBaseDelete = res.lastDelete;
            mLog.emplace_back(std::move(l));
            doRun();
        },
        Signature<Commands::T2>::arguments{mBaseInsert, mBaseDelete, mNumClients});
        break;
    case 2:
        mClient.execute<Commands::T3>([this, now](
                    const err_code& ec,
                    const typename Signature<Commands::T3>::result& res) {
            assertOk(*this, ec, Commands::T3);
            assertOk(*this, res, Commands::T3);
            auto end = Clock::now();
            LogEntry l;
            l.success = res.success;
            l.error = res.msg;
            l.transaction = Commands::T3;
            l.start = now;
            l.end = end;
            l.responseTime = res.responseTime;
            mLog.emplace_back(std::move(l));
            doRun();
        },
        Signature<Commands::T3>::arguments{mBaseInsert, mBaseDelete, mNumClients, mClientId});
        break;
    case 3:
        mClient.execute<Commands::T5>([this, now](
                    const err_code& ec,
                    const typename Signature<Commands::T5>::result& res) {
            assertOk(*this, ec, Commands::T5);
            assertOk(*this, res, Commands::T5);
            auto end = Clock::now();
            LogEntry l;
            l.success = res.success;
            l.error = res.msg;
            l.transaction = Commands::T5;
            l.start = now;
            l.end = end;
            l.responseTime = res.responseTime;
            mLog.emplace_back(std::move(l));
            doRun();
        },
        Signature<Commands::T5>::arguments{mBaseInsert, mBaseDelete, mNumClients, mClientId});
        break;
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
        assertOk(*this, ec, mCurrent);
        assertOk(*this, res, mCurrent);
        //std::cout << "Query done" << std::endl;
        auto end = Clock::now();
        LogEntry l;
        l.success = res.success;
        l.error = res.msg;
        l.transaction = mCurrent;
        l.start = now;
        l.end = end;
        l.responseTime = res.responseTime;
        mLog.emplace_back(std::move(l));
        doRunAnalytical();
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

void Client::run(const Clock::duration& duration, bool timer) {
    auto now = Clock::now();
    mEndTime = now + duration;
    mTimer = timer;
    mLastTime = now;
    if (mAnalytical) doRunAnalytical();
    else doRun();
}

void Client::populate(const std::vector<std::unique_ptr<Client>>& clients) {
    mClient.execute<Commands::CreateSchema>([this, &clients](const err_code& ec,
                const std::tuple<bool, crossbow::string>& res) {
        assertOk(*this, ec, Commands::Populate);
        assertOk(*this, res, Commands::Populate);
        LOG_INFO("Created Schema, start population");
        std::cout << "Created Schema - start population" << std::endl;
        uint64_t numTuples = mSf * 1024 * 1024;
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
