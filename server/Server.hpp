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
#include "Queries.hpp"
#include <util/Protocol.hpp>
#include <common/Common.hpp>

#include <crossbow/Protocol.hpp>
#include <boost/format.hpp>
#include <random>
#include <limits>

namespace mbench {

template<class Transaction>
struct BatchOperation {
    std::vector<double> ops;
    std::vector<uint64_t> getKeys;
    std::vector<uint64_t> deletes;
    std::vector<std::pair<uint64_t, typename Transaction::Tuple>> inserts;
    std::vector<std::pair<uint64_t, typename Transaction::UpdateOp>> updates;
    BatchResult res;

    template<class Server>
    TxType init(const BatchOp& op, Server& srv) {
        double getProb = 1.0 - op.insertProb - op.deleteProb - op.updateProb;
        if (getProb < 0.0) {
            std::cerr << "Probabilities sum up to negative number\n";
            throw std::runtime_error("Probabilities sum up to negative number");
        }
        res.baseDeleteKey = op.baseDeleteKey;
        res.baseInsertKey = op.baseInsertKey;
        ops.clear();
        ops.resize(op.numOps);
        getKeys.clear();
        deletes.clear();
        inserts.clear();
        updates.clear();

        for (unsigned i = 0; i < ops.size(); ++i) {
            ops[i] = srv.template rand<0>();
            if (ops[i] < op.insertProb) {
                res.baseInsertKey += op.numClients;
                inserts.emplace_back(res.baseInsertKey, srv.createInsert());
            } else if (ops[i] < op.insertProb + op.updateProb) {
                auto k = srv.rndKey(res.baseInsertKey, res.baseDeleteKey, op.numClients, op.clientId);
                if (srv.mN == 1) {
                    typename Transaction::UpdateOp update;
                    update[0] = std::make_pair(0, srv.template rand<0>());
                    updates.emplace_back(k, update);
                } else {
                    updates.emplace_back(k, srv.rndUpdate());
                }
            } else if (ops[i] < op.insertProb + op.updateProb + op.deleteProb) {
                if (res.baseDeleteKey + op.numClients >= res.baseInsertKey) {
                    ops[i] = -1.0; // enforce insert
                    res.baseInsertKey += op.numClients;
                    inserts.emplace_back(res.baseInsertKey, srv.createInsert());
                } else {
                    deletes.push_back(res.baseDeleteKey);
                    res.baseDeleteKey += op.numClients;
                }
            } else {
                getKeys.push_back(srv.rndKey(res.baseInsertKey, res.baseDeleteKey, op.numClients, op.clientId));
            }
        }
        return getProb == 1.0 ? TxType::RO : TxType::RW;
    }

    void exec(const BatchOp& op, Transaction& tx) {
        auto insIter = inserts.begin();
        auto delIter = deletes.begin();
        auto getIter = getKeys.begin();
        auto updIter = updates.begin();
        for (auto o : ops) {
            if (o < op.insertProb) {
                tx.insert(insIter->first, insIter->second);
                ++insIter;
            } else if (o < op.insertProb + op.updateProb) {
                tx.update(updIter->first, updIter->second);
                ++updIter;
            } else if (o < op.insertProb + op.updateProb + op.deleteProb) {
                tx.remove(*delIter);
                ++delIter;
            } else {
                tx.get(*getIter);
                ++getIter;
            }
        }
        tx.commit();
    }
};

template<class Connection, class Transaction>
class Server {
    friend struct ScanContext<::mbench::Server, Connection, Transaction>;
    friend struct BatchOperation<Transaction>;
    // Ultimate hack to make template instanciation more comfortable
    template<template <template <class, class> class, class, class> class T>
    using GetInstance = T<::mbench::Server, Connection, Transaction>;
private: // types
    using string = typename Connection::string_type;
    using Field = typename Transaction::Field;
    using disti  = std::uniform_int_distribution<int32_t>;
    using distsi = std::uniform_int_distribution<int16_t>;
    using distbi = std::uniform_int_distribution<int64_t>;
    using distf  = std::uniform_real_distribution<float>;
    using distd  = std::uniform_real_distribution<double>;
private: // members
    BatchOperation<Transaction> mBatchOp;
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
                distd(), //TODO: this should actually be distd(std::numeric_limits<double>::min(), std::numeric_limits<double>::max()) to adhere to the benchmark specs...
                disti(0, 9));
    std::vector<string> mSyllables = {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
    };
    GetInstance<ScanContext> mScanContext;
    GetInstance<Q1> mQ1;
    GetInstance<Q2> mQ2;
    GetInstance<Q3> mQ3;
public: // construction
    Server(boost::asio::io_service& service, Connection& connection, unsigned n)
        : mService(service)
        , mSocket(service)
        , mServer(*this, mSocket)
        , mConnection(connection)
        , mN(n)
        , mRnd(randomSeed())
        , mScanContext(*this)
        , mQ1(mScanContext)
        , mQ2(mScanContext)
        , mQ3(mScanContext)
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
    const string& rndSyllable() {
        return mSyllables[std::get<8>(mDists)(mRnd)];
    }
public:
    std::array<std::pair<unsigned, Field>, 5> rndUpdate() {
        assert(mN >= 10);
        std::array<std::pair<unsigned, Field>, 5> cols;
        std::uniform_int_distribution<unsigned> colDist(0, mN / 10 - 1);
        std::uniform_int_distribution<unsigned> boolDist(1, 10);
        auto offset = mN == 10 ? 0 : colDist(mRnd) * 10;
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
            cols[4] = std::make_pair(offset + 8, rand<8>());
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

    typename Transaction::Tuple createInsert() {
        auto t = Transaction::newTuple(mN);
        for (unsigned j = 0; j < mN; ++j) {
            switch (j % 10) {
            case 0:
                t[j] = rand<0>();
                break;
            case 1:
                t[j] = rand<1>();
                break;
            case 2:
                t[j] = rand<2>();
                break;
            case 3:
                t[j] = rand<3>();
                break;
            case 4:
                t[j] = rand<4>();
                break;
            case 5:
                t[j] = rand<5>();
                break;
            case 6:
                t[j] = rand<6>();
                break;
            case 7:
                t[j] = rand<7>();
                break;
            case 8:
                t[j] = rand<8>();
                break;
            case 9:
                t[j] = rand<9>();
            }
        }
        return t;
    }

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::Populate, void>::type
    execute(const std::tuple<uint64_t, uint64_t>& args, const Callback& callback) {
        uint64_t start = std::get<0>(args);
        uint64_t end = std::get<1>(args);
        mConnection.startTx(TxType::RW, [this, callback, start, end](Transaction& tx) {
            try {
                std::vector<std::pair<uint64_t, typename Transaction::Tuple>> inserts;
                for (uint64_t i = start; i < end; ++i) {
                    inserts.emplace_back(i, createInsert());
                }
                auto start = Clock::now();
                for (const auto& ins : inserts) {
                    tx.insert(ins.first, ins.second);
                }
                tx.commit();
                auto responseTime = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
                mService.post([callback, responseTime](){
                    callback(std::make_tuple(true, std::string(""), responseTime));
                });
            } catch (std::exception& ex) {
                crossbow::string errmsg = (boost::format("ERROR in (%1%:%2%): %3%")
                            % __FILE__
                            % __LINE__
                            % ex.what()
                        ).str();
                auto res = std::make_tuple(false, crossbow::string(errmsg.c_str()), 0l);
                mService.post([callback, res]() {
                    callback(res);
                });
            }
        });
    }

    uint64_t rndKey(uint64_t baseInsertKey, uint64_t baseDeleteKey, unsigned numClients, unsigned clientId) {
        std::uniform_int_distribution<uint64_t> dist(baseDeleteKey, baseInsertKey);
        uint64_t k = dist(mRnd);
        k = (k / numClients) * numClients;
        k += clientId;
        if (k >= baseInsertKey) {
            k -= numClients;
        } else if (k < baseDeleteKey) {
            k += numClients;
        }
        return k;
    }

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::BatchOp, void>::type
    execute(const BatchOp& op, const Callback& callback) {
        mConnection.startTx(mBatchOp.init(op, *this), [this, callback, op](Transaction& tx) {
            try {
                auto res = mBatchOp.res;
                auto start = Clock::now();
                mBatchOp.exec(op, tx);
                res.responseTime = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
                res.success = true;
                mService.post([callback, res]() {
                    callback(res);
                });
            } catch (std::exception& ex) {
                crossbow::string errmsg = (boost::format("ERROR in (%1%:%2%): %3%")
                            % __FILE__
                            % __LINE__
                            % ex.what()
                        ).str();
                mService.post([callback, errmsg]() {
                    callback(BatchResult{false, errmsg, 0ul, 0ul, 0ul});
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
                auto begin = Clock::now();
                mQ1(tx);
                tx.commit();
                auto end = Clock::now();
                res.responseTime = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
                mService.post([callback, res]() {
                    callback(res);
                });
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

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::Q2, void>::type
    execute(const Callback& callback) {
        mConnection.startTx(TxType::A, [this, callback](Transaction& tx) {
            err_msg res;
            res.success = true;
            try {
                auto begin = Clock::now();
                mQ2(tx);
                tx.commit();
                auto end = Clock::now();
                res.responseTime = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
                mService.post([callback, res]() {
                    callback(res);
                });
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

    template<Commands C, class Callback>
    typename std::enable_if<C == Commands::Q3, void>::type
    execute(const Callback& callback) {
        mConnection.startTx(TxType::A, [this, callback](Transaction& tx) {
            err_msg res;
            res.success = true;
            try {
                auto begin = Clock::now();
                mQ3(tx);
                tx.commit();
                auto end = Clock::now();
                res.responseTime = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
                mService.post([callback, res]() {
                    callback(res);
                });
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
