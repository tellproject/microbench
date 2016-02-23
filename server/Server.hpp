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
#include "Population.hpp"

#include <crossbow/Protocol.hpp>
#include <random>
#include <limits>

namespace mbench {

GEN_COMMANDS(Commands, (Populate, CreateSchema));

enum class TxType {
    RW, RO, A
};

template<Commands cmd>
struct Signature;

template<>
struct Signature<Commands::Populate> {
    using arguments = std::tuple<uint64_t, uint64_t>;
    using result = std::tuple<bool, crossbow::string>;
};

template<>
struct Signature<Commands::CreateSchema> {
    using arguments = void;
    using result = std::tuple<bool, crossbow::string>;
};

template<class Connection, class Transaction>
class Server {
private: // types
    using string = typename Connection::string_type;
    using disti  = std::uniform_int_distribution<int32_t>;
    using distsi = std::uniform_int_distribution<int16_t>;
    using distbi = std::uniform_int_distribution<int64_t>;
    using distf  = std::uniform_real_distribution<float>;
    using distd  = std::uniform_real_distribution<double>;
private: // members
    boost::asio::io_service& mService;
    boost::asio::ip::tcp::socket mSocket;
    SERVER_TYPE(Commands, Server) mServer;
    Populator<Connection, Transaction> populator;
    Connection& mConnection;
    unsigned mSf, mN;
    std::mt19937_64 mRnd;
    std::tuple<distd, disti, disti, distsi, distsi, distbi, distbi, distd, disti> mDists
        = std::make_tuple(distd(0,1),
                disti(),
                disti(0, 10000),
                distsi(0, 1),
                distsi(0, 255),
                distbi(std::numeric_limits<int64_t>::min(), 0),
                distbi(),
                distd(),
                disti(0, 9));
    std::vector<string> mSyllables = {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
    };
public: // construction
    Server(boost::asio::io_service& service, Connection& connection, unsigned sf, unsigned n)
        : mService(service)
        , mSocket(service)
        , mServer(*this, mSocket)
        , mConnection(connection)
        , mSf(sf)
        , mN(n)
        , mRnd(std::random_device()())
    {}
public:
    boost::asio::ip::tcp::socket& socket() {
        return mSocket;
    }
    void run() {
        mServer.run();
    }
    void close() {
        mSocket.close();
        delete this;
    }
public: // commands
    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::CreateSchema, void>::type
    execute(const Callback& callback) {
        mConnection.startTx(TxType::RW, [callback, this](Transaction& tx) {
            tx.createSchema(mN);
            tx.commit();
            mService.post([callback](){
                callback(std::make_tuple(true, std::string("")));
            });
        });
    }

    void insert(Transaction& tx, uint64_t key) {
        auto t = tx.newTuple(mN);
        for (unsigned j = 0; j < mN; ++j) {
            switch (j % 10) {
            case 0:
                t[0] = std::get<0>(mDists)(mRnd);
                break;
            case 1:
                t[1] = std::get<1>(mDists)(mRnd);
                break;
            case 2:
                t[2] = std::get<2>(mDists)(mRnd);
                break;
            case 3:
                t[3] = std::get<3>(mDists)(mRnd);
                break;
            case 4:
                t[4] = std::get<4>(mDists)(mRnd);
                break;
            case 5:
                t[5] = std::get<5>(mDists)(mRnd);
                break;
            case 6:
                t[6] = std::get<6>(mDists)(mRnd);
                break;
            case 7:
                t[7] = std::get<7>(mDists)(mRnd);
                break;
            case 8:
                {
                    auto& d = std::get<8>(mDists);
                    auto& s = mSyllables;
                    t[8] = s[d(mRnd)] + s[d(mRnd)] + s[d(mRnd)];
                }
                break;
            case 9:
                {
                    auto& d = std::get<8>(mDists);
                    auto& s = mSyllables;
                    t[8] = s[d(mRnd)] + s[d(mRnd)] + s[d(mRnd)];
                }
            }
        }
        tx.insert(key, t);
    }

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::Populate, void>::type
    execute(const std::tuple<uint64_t, uint64_t>& args, const Callback& callback) {
        unsigned start = std::get<0>(args);
        unsigned end = std::get<1>(args);
        mConnection.startTx(TxType::RW, [this, callback, start, end](Transaction& tx) {
            for (uint64_t i = start; i < end; ++i) {
                insert(tx, i);
            }
            tx.commit();
            mService.post([callback](){
                callback(std::make_tuple(true, std::string("")));
            });
        });
    }
};

using err_code = boost::system::error_code;

template<class Connection, class Transaction>
void accept(boost::asio::ip::tcp::acceptor& acceptor, Connection& connection, unsigned sf, unsigned n) {
    auto srv = new mbench::Server<Connection, Transaction>(acceptor.get_io_service(), connection, sf, n);
    acceptor.async_accept(srv->socket(), [srv, &acceptor, &connection, sf, n](const err_code& ec){
        if (ec) {
            std::cerr << ec.message();
            delete srv;
            return;
        }
        srv->run();
        accept<Connection, Transaction>(acceptor, connection, sf, n);
    });
}


} // namespace mbench