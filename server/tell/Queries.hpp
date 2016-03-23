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
                {{tell::store::AggregationType::MAX,
                tx.idOfPos(0)}});
        auto iter = tx.transaction().scan(query, *scanMemoryManager);
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
        auto j = tx.idOfPos(7);
        tell::db::Aggregation query(tx.tableId(),
                {{tell::store::AggregationType::MAX,
                tx.idOfPos(0)}});
        query && std::make_tuple(tell::store::PredicateType::GREATER, j, tell::db::Field(double(0.0)))
              && std::make_tuple(tell::store::PredicateType::LESS, j, tell::db::Field(double(0.5)));
        auto iter = tx.transaction().scan(query, *scanMemoryManager);
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
        auto f = tx.idOfPos(4);
        tell::db::FullScan query(tx.tableId());
        query && std::make_tuple(tell::store::PredicateType::GREATER, f, tell::db::Field(int16_t(0)))
              && std::make_tuple(tell::store::PredicateType::LESS, f, tell::db::Field(int16_t(26)));
        auto iter = tx.transaction().scan(query, *scanMemoryManager);
        while (iter->hasNext()) {
            iter->nextChunk();
        }
    }
};

} // namespace mbench
