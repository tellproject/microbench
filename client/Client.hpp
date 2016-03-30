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
#include <common/Common.hpp>

#include <chrono>
#include <random>
#include <deque>

namespace mbench {

extern uint64_t calcBaseInsertKey(unsigned sf, unsigned numClients, unsigned clientId);

struct LogEntry {
    bool success;
    crossbow::string error;
    Commands transaction;
    Clock::time_point start;
    Clock::time_point end;
    long responseTime;
};

class Client {
private:
    boost::asio::ip::tcp::socket mSocket;
    boost::asio::io_service::strand& mIOStrand;
    crossbow::protocol::Client<Commands, Signature> mClient;
    unsigned mSf;
    uint64_t mNumTuples;
    bool mTimer = false;
    Clock::time_point mStartTime;
    Clock::time_point mEndTime;
    Clock::time_point mLastTime;
    std::mt19937_64 mRnd;
    std::uniform_int_distribution<unsigned> mDist;
    bool mAnalytical;
    std::deque<LogEntry> mLog;
    unsigned mNumClients;
    unsigned mClientId;
    Commands mCurrent;
    BatchOp mBatchOp;
    bool mOnlyQ1;
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
           , bool analytical
           , unsigned numOps
           , double insertProb
           , double deleteProb
           , double updateProb
           , bool onlyQ1)
        : mSocket(service)
        , mIOStrand(ioStrand)
        , mClient(mSocket)
        , mSf(sf)
        , mNumTuples(numTuples(sf))
        , mStartTime(Clock::now())
        , mRnd(randomSeed())
        , mDist(0, analytical ? 2 : 3)
        , mAnalytical(analytical)
        , mNumClients(numClients)
        , mClientId(clientId)
        , mOnlyQ1(onlyQ1)
    {
        mBatchOp.numOps        = numOps;
        mBatchOp.insertProb    = insertProb;
        mBatchOp.deleteProb    = deleteProb;
        mBatchOp.updateProb    = updateProb;
        mBatchOp.clientId      = mClientId;
        mBatchOp.numClients    = mNumClients;
        mBatchOp.baseInsertKey = calcBaseInsertKey(sf, numClients, clientId);
        mBatchOp.baseDeleteKey = clientId;
        assert(mAnalytical ||
                (mBatchOp.baseInsertKey > mBatchOp.baseDeleteKey && mBatchOp.baseInsertKey >= numTuples(mSf) - mNumClients));
    }

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

