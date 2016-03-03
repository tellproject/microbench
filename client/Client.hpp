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

#include <chrono>
#include <random>
#include <deque>

namespace mbench {
using Clock = std::chrono::high_resolution_clock;

constexpr uint64_t numTuples(unsigned sf) {
    return uint64_t(sf)*uint64_t(1024*1024);
}

extern uint64_t calcBaseInsertKey(unsigned sf, unsigned numClients, unsigned clientId);

struct LogEntry {
    bool success;
    crossbow::string error;
    Commands transaction;
    Clock::time_point start;
    Clock::time_point end;
};

class Client {
private:
    boost::asio::ip::tcp::socket mSocket;
    boost::asio::io_service::strand& mIOStrand;
    crossbow::protocol::Client<Commands, Signature> mClient;
    unsigned mSf;
    bool mTimer = false;
    Clock::time_point mStartTime;
    Clock::time_point mEndTime;
    Clock::time_point mLastTime;
    std::mt19937 mRnd;
    std::uniform_int_distribution<unsigned> mDist;
    bool mAnalytical;
    std::deque<LogEntry> mLog;
    unsigned mNumClients;
    unsigned mClientId;
    unsigned mBaseInsert;
    unsigned mBaseDelete;
    Commands mCurrent;
private:
    void populate(uint64_t start, uint64_t end);
    bool done(const Clock::time_point& now);
    void doRun();
    void doRunAnalytical();
public:
    Client(boost::asio::io_service& service
           , boost::asio::io_service::strand& ioStrand
           , unsigned sf
           , unsigned numClients
           , unsigned clientId
           , bool analytical)
        : mSocket(service)
        , mIOStrand(ioStrand)
        , mClient(mSocket)
        , mSf(sf)
        , mStartTime(Clock::now())
        , mRnd(std::random_device()())
        , mDist(0, analytical ? 4 : 3)
        , mAnalytical(analytical)
        , mNumClients(numClients)
        , mClientId(clientId)
        , mBaseInsert(calcBaseInsertKey(sf, numClients, clientId))
        , mBaseDelete(clientId)
    {}
    ~Client() {
        if (mSocket.is_open()) mSocket.close();
    }
    boost::asio::ip::tcp::socket& socket() {
        return mSocket;
    }

    void run(const Clock::duration& duration, bool timer);
    void populate(const std::vector<std::unique_ptr<Client>>& clients);
    bool isAnalytical() const {
        return mAnalytical;
    }
    const std::deque<LogEntry>& log() const {
        return mLog;
    }

    unsigned numClients() const {
        return mNumClients;
    }

    unsigned clientId() const {
        return mClientId;
    }
};

} // namespace mbench

