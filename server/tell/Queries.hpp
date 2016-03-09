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
#include "ScanContext.hpp"
#include <server/Queries.hpp>
#include "Connection.hpp"

#include <telldb/ScanQuery.hpp>

namespace mbench {

template<template <class, class> class Server>
struct Q1<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q1(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        tell::db::Aggregation query(tx.tableId(),
                {{scanContext.rndFun(),
                tx.idOfPos(scanContext.template rndCol<'A'>())}});
        auto iter = tx.transaction().scan(query, *scanContext.memoryManager);
        while (iter->hasNext()) {
            iter->nextChunk();
        }
    }
};

template<template <class, class> class Server>
struct Q2<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q2(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        unsigned iU, jU;
        std::tie(iU,jU) = scanContext.template rndTwoCols<'A'>();
        auto i = tx.idOfPos(iU);
        auto j = tx.idOfPos(iU);
        tell::db::Aggregation query(tx.tableId(),
                {{scanContext.rndFun(),
                i}});
        query && std::make_tuple(tell::store::PredicateType::GREATER, j, tell::db::Field(double(0.0)))
              && std::make_tuple(tell::store::PredicateType::LESS, j, tell::db::Field(double(0.5)));
        auto iter = tx.transaction().scan(query, *scanContext.memoryManager);
        while (iter->hasNext()) {
            iter->nextChunk();
        }
    }
};

template<template <class, class> class Server>
struct Q3<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q3(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        auto& rnd = scanContext.rnd();
        auto fun = scanContext.rndFun();
        auto ai = tx.idOfPos(scanContext.template rndCol<'A'>());
        tell::db::Aggregation query(tx.tableId(), {{fun, ai}});
        auto ci = tx.idOfPos(scanContext.template rndCol<'C'>());
        auto di = tx.idOfPos(scanContext.template rndCol<'D'>());
        auto ei = tx.idOfPos(scanContext.template rndCol<'E'>());
        std::uniform_int_distribution<int32_t> cdist(0, 7500);
        std::uniform_int_distribution<int16_t> ddist(0, 1);
        std::uniform_int_distribution<int16_t> edist(0, 255);
        auto c = cdist(rnd);
        decltype(c) cupper = c + 500;
        auto d = ddist(rnd);
        auto e = edist(rnd);
        auto dcond = std::make_tuple(tell::store::PredicateType::EQUAL, di, tell::db::Field(d));
        auto econd = std::make_tuple(tell::store::PredicateType::EQUAL, ei, tell::db::Field(e));
        auto clarger = std::make_tuple(tell::store::PredicateType::GREATER_EQUAL, ci, tell::db::Field(c));
        auto csmaller = std::make_tuple(tell::store::PredicateType::GREATER_EQUAL, ci, tell::db::Field(cupper));

        query && csmaller && clarger && dcond && econd;

        auto iter = tx.transaction().scan(query, *scanContext.memoryManager);
        while (iter->hasNext()) {
            iter->nextChunk();
        }
    }
};

template<template <class, class> class Server>
struct Q4<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q4(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        auto fun = scanContext.rndFun();
        auto ai = tx.idOfPos(scanContext.template rndCol<'A'>());
        auto ki = tx.idOfPos(scanContext.template rndCol<'J'>());
        tell::db::Aggregation query(tx.tableId(), {{fun, ai}});
        const auto& syllable = scanContext.rndSyllable();

        query && std::make_tuple(tell::store::PredicateType::PREFIX_LIKE, ki, tell::db::Field(syllable));

        auto iter = tx.transaction().scan(query, *scanContext.memoryManager);
        while (iter->hasNext()) {
            iter->nextChunk();
        }
    }
};

template<template <class, class> class Server>
struct Q5<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q5(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        auto fun = scanContext.rndFun();
        auto ai = tx.idOfPos(scanContext.template rndCol<'A'>());
        auto ki = tx.idOfPos(scanContext.template rndCol<'J'>());
        tell::db::Aggregation query(tx.tableId(), {{fun, ai}});
        const auto& syllable = scanContext.rndSyllable();

        query && std::make_tuple(tell::store::PredicateType::POSTFIX_LIKE, ki, tell::db::Field(syllable));

        auto iter = tx.transaction().scan(query, *scanContext.memoryManager);
        while (iter->hasNext()) {
            iter->nextChunk();
        }
    }
};
} // namespace mbench
