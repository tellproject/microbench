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
#include "Connection.hpp"

#include <server/Queries.hpp>

namespace mbench {


template<template <class, class> class Server>
struct ScanContext<Server, Connection, Transaction>
: public ContextBase<ScanContext<Server, Connection, Transaction>, tell::store::AggregationType, crossbow::string> {
    using AggregationFunction = tell::store::AggregationType;
    using string = crossbow::string;

    Server<Connection, Transaction>& server;

    ScanContext(Server<Connection, Transaction>& server)
        : server(server)
    {}

    constexpr tell::store::AggregationType sum() const {
        return tell::store::AggregationType::SUM;
    }

    constexpr tell::store::AggregationType count() const {
        return tell::store::AggregationType::CNT;
    }

    constexpr tell::store::AggregationType min() const {
        return tell::store::AggregationType::MIN;
    }

    constexpr tell::store::AggregationType max() const {
        return tell::store::AggregationType::MIN;
    }

    std::mt19937_64& rnd() {
        return server.mRnd;
    }

    unsigned N() const {
        return server.N();
    }

    const string& rndSyllable() {
        return server.rndSyllable();
    }
};

}
