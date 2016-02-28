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
#include "Queries.hpp"
#include <util/Protocol.hpp>

#include <crossbow/Protocol.hpp>
#include <boost/format.hpp>
#include <random>
#include <limits>

namespace mbench {

template<class Connection, class Transaction>
class Server {
    friend struct ScanContext<Connection>;
private: // types
    using string = typename Connection::string_type;
    using Field = typename Transaction::Field;
    using disti  = std::uniform_int_distribution<int32_t>;
    using distsi = std::uniform_int_distribution<int16_t>;
    using distbi = std::uniform_int_distribution<int64_t>;
    using distf  = std::uniform_real_distribution<float>;
    using distd  = std::uniform_real_distribution<double>;
private: // members
    boost::asio::io_service& mService;
    boost::asio::ip::tcp::socket mSocket;
    SERVER_TYPE(Commands, Server) mServer;
    Connection& mConnection;
    unsigned mN;
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
    std::uniform_int_distribution<unsigned> mColumnDist;
    ScanContext<Connection> mScanContext;
    Q1<Connection> mQ1;
public: // construction
    Server(boost::asio::io_service& service, Connection& connection, unsigned n)
        : mService(service)
        , mSocket(service)
        , mServer(*this, mSocket)
        , mConnection(connection)
        , mN(n)
        , mRnd(std::random_device()())
        , mColumnDist(0, mN)
        , mScanContext(*this)
        , mQ1(mScanContext)
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
    unsigned N() const {
        return mN;
    }
public:
    std::array<std::pair<unsigned, Field>, 5> rndUpdate() {
        assert(mN >= 10);
        std::array<std::pair<unsigned, Field>, 5> cols;
        std::uniform_int_distribution<unsigned> colDist(0, mN / 10);
        std::uniform_int_distribution<unsigned> boolDist(1, 10);
        auto offset = colDist(mRnd) * mN;
        // double col
        if (boolDist(mRnd) <= 5) {
            cols[0] = std::make_pair(offset, rand<0>());
        } else {
            cols[0] = std::make_pair(offset + 7, rand<7>());
        }
        if (boolDist(mRnd) <= 5) {
            cols[1] = std::make_pair(offset + 1, rand<1>());
        } else {
            cols[1] = std::make_pair(offset + 2, rand<2>());
        }
        if (boolDist(mRnd) <= 5) {
            cols[2] = std::make_pair(offset + 3, rand<3>());
        } else {
            cols[2] = std::make_pair(offset + 4, rand<4>());
        }
        if (boolDist(mRnd) <= 5) {
            cols[3] = std::make_pair(offset + 5, rand<5>());
        } else {
            cols[3] = std::make_pair(offset + 6, rand<6>());
        }
        if (boolDist(mRnd) <= 5) {
            cols[4] = std::make_pair(offset + 8, rand<9>());
        } else {
            cols[4] = std::make_pair(offset + 9, rand<9>());
        }
        return cols;
    }
    template<int I>
    typename std::enable_if<I == 0, double>::type
    rand() {
        return std::get<0>(mDists)(mRnd);
    }
    template<int I>
    typename std::enable_if<I == 1, int32_t>::type
    rand() {
        return std::get<1>(mDists)(mRnd);
    }
    template<int I>
    typename std::enable_if<I == 2, int32_t>::type
    rand() {
        return std::get<2>(mDists)(mRnd);
    }
    template<int I>
    typename std::enable_if<I == 3, int16_t>::type
    rand() {
        return std::get<3>(mDists)(mRnd);
    }
    template<int I>
    typename std::enable_if<I == 4, int16_t>::type
    rand() {
        return std::get<4>(mDists)(mRnd);
    }
    template<int I>
    typename std::enable_if<I == 5, int64_t>::type
    rand() {
        return std::get<5>(mDists)(mRnd);
    }
    template<int I>
    typename std::enable_if<I == 6, int64_t>::type
    rand() {
        return std::get<6>(mDists)(mRnd);
    }
    template<int I>
    typename std::enable_if<I == 7, double>::type
    rand() {
        return std::get<7>(mDists)(mRnd);
    }
    template<int I>
    typename std::enable_if<I == 8, string>::type
    rand() {
        auto& d = std::get<8>(mDists);
        auto& s = mSyllables;
        return s[d(mRnd)] + s[d(mRnd)] + s[d(mRnd)];
    }
    template<int I>
    typename std::enable_if<I == 9, string>::type
    rand() {
        auto& d = std::get<8>(mDists);
        auto& s = mSyllables;
        return s[d(mRnd)] + s[d(mRnd)] + s[d(mRnd)];
    }
public: // commands
    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::CreateSchema, void>::type
    execute(unsigned sf, const Callback& callback) {
        mConnection.startTx(TxType::RW, [callback, this, sf](Transaction& tx) {
            try {
                tx.createSchema(mN, sf);
                tx.commit();
                mService.post([callback](){
                    callback(std::make_tuple(true, std::string("")));
                });
            } catch (std::exception& ex) {
                crossbow::string errmsg = (boost::format("ERROR in (%1%:%2%): %3%")
                            % __FILE__
                            % __LINE__
                            % ex.what()
                        ).str();
                auto res = std::make_tuple(false, crossbow::string(ex.what()));
                mService.post([callback, res]() {
                    callback(res);
                });
            }
        });
    }

    void insert(Transaction& tx, uint64_t key) {
        auto t = tx.newTuple(mN);
        for (unsigned j = 0; j < mN; ++j) {
            switch (j % 10) {
            case 0:
                t[0] = rand<0>();
                break;
            case 1:
                t[1] = rand<1>();
                break;
            case 2:
                t[2] = rand<2>();
                break;
            case 3:
                t[3] = rand<3>();
                break;
            case 4:
                t[4] = rand<4>();
                break;
            case 5:
                t[5] = rand<5>();
                break;
            case 6:
                t[6] = rand<6>();
                break;
            case 7:
                t[7] = rand<7>();
                break;
            case 8:
                t[8] = rand<8>();
                break;
            case 9:
                t[9] = rand<9>();
            }
        }
        tx.insert(key, t);
    }

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::Populate, void>::type
    execute(const std::tuple<uint64_t, uint64_t>& args, const Callback& callback) {
        uint64_t start = std::get<0>(args);
        uint64_t end = std::get<1>(args);
        mConnection.startTx(TxType::RW, [this, callback, start, end](Transaction& tx) {
            try {
                for (uint64_t i = start; i < end; ++i) {
                    insert(tx, i);
                }
                tx.commit();
                mService.post([callback](){
                    callback(std::make_tuple(true, std::string("")));
                });
            } catch (std::exception& ex) {
                crossbow::string errmsg = (boost::format("ERROR in (%1%:%2%): %3%")
                            % __FILE__
                            % __LINE__
                            % ex.what()
                        ).str();
                auto res = std::make_tuple(false, crossbow::string(ex.what()));
                mService.post([callback, res]() {
                    callback(res);
                });
            }
        });
    }

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::T1, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        using res_type = typename Signature<C>::result;
        mConnection.startTx(TxType::RW, [this, callback, args](Transaction& tx) {
            try {
                for (uint64_t i = 0; i < 100ul; ++i) {
                    insert(tx, args.baseInsertKey + i * args.numClients);
                }
                auto newBaseInsert = args.baseInsertKey + 100ul * args.numClients;
                tx.commit();
                mService.post([callback, newBaseInsert](){
                    callback(res_type{true, "", newBaseInsert});
                });
            } catch (std::exception& ex) {
                crossbow::string errmsg = (boost::format("ERROR in (%1%:%2%): %3%")
                            % __FILE__
                            % __LINE__
                            % ex.what()
                        ).str();
                crossbow::string msg(ex.what());
                mService.post([callback, msg]() {
                    callback(res_type{false, msg, 0ul});
                });
            }
        });
    }

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::T2, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        using res_type = typename Signature<C>::result;
        mConnection.startTx(TxType::RW, [this, callback, args](Transaction& tx) {
            try {
                for (uint64_t i = 0; i < 100ul; ++i) {
                    tx.remove(args.baseDeleteKey + i * args.numClients);
                }
                auto newBaseDelete = args.baseDeleteKey + 100ul * args.numClients;
                tx.commit();
                mService.post([callback, newBaseDelete](){
                    callback(res_type{true, "", newBaseDelete});
                });
            } catch (std::exception& ex) {
                crossbow::string errmsg = (boost::format("ERROR in (%1%:%2%): %3%")
                            % __FILE__
                            % __LINE__
                            % ex.what()
                        ).str();
                crossbow::string msg(ex.what());
                mService.post([callback, msg]() {
                    callback(res_type{false, msg, 0ul});
                });
            }
        });
    }

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::T3, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        using res_type = typename Signature<C>::result;
        mConnection.startTx(TxType::RO, [this, callback, args](Transaction& tx) {
            try {
                std::uniform_int_distribution<uint64_t> dist(args.baseDeleteKey, args.baseInsertKey);
                for (auto i = 0; i < 100; ++i) {
                    uint64_t k = dist(mRnd);
                    k = (k / args.numClients) * args.numClients;
                    k += args.clientId;
                    tx.get(k);
                }
                tx.commit();
                mService.post([callback](){
                    callback(res_type{false, ""});
                });
            } catch (std::exception& ex) {
                crossbow::string errmsg = (boost::format("ERROR in (%1%:%2%): %3%")
                            % __FILE__
                            % __LINE__
                            % ex.what()
                        ).str();
                crossbow::string msg(ex.what());
                mService.post([callback, msg]() {
                    callback(res_type{false, msg});
                });
            }
        });
    }

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::T5, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        using res_type = typename Signature<C>::result;
        mConnection.startTx(TxType::RW, [this, callback, args](Transaction& tx) {
            try {
                std::uniform_int_distribution<uint64_t> dist(args.baseDeleteKey, args.baseInsertKey);
                for (auto i = 0; i < 100; ++i) {
                    uint64_t k = dist(mRnd);
                    k = (k / args.numClients) * args.numClients;
                    k += args.clientId;
                    tx.update(k, mN, *this);
                }
                tx.commit();
                mService.post([callback](){
                    callback(res_type{false, ""});
                });
            } catch (std::exception& ex) {
                crossbow::string errmsg = (boost::format("ERROR in (%1%:%2%): %3%")
                            % __FILE__
                            % __LINE__
                            % ex.what()
                        ).str();
                crossbow::string msg(ex.what());
                mService.post([callback, msg]() {
                    callback(res_type{false, msg});
                });
            }
        });
    }

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::Q1, void>::type
    execute(const Callback& callback) {
        mConnection.startTx(TxType::A, [this, callback](Transaction& tx) {
            err_msg res;
            res.success = true;
            try {
            } catch (std::exception& ex) {
                res.success = false;
                res.msg = (boost::format("ERROR in (%1%:%2%): %3%")
                        % __FILE__
                        % __LINE__
                        % ex.what()).str();
                mService.post([callback, res] {
                    callback(res);
                });
            }
        });
    }
};

using err_code = boost::system::error_code;

template<class Connection, class Transaction>
void accept(boost::asio::ip::tcp::acceptor& acceptor, Connection& connection, unsigned n) {
    auto srv = new mbench::Server<Connection, Transaction>(acceptor.get_io_service(), connection, n);
    acceptor.async_accept(srv->socket(), [srv, &acceptor, &connection, n](const err_code& ec){
        if (ec) {
            std::cerr << ec.message();
            delete srv;
            return;
        }
        srv->run();
        accept<Connection, Transaction>(acceptor, connection, n);
    });
}


} // namespace mbench
