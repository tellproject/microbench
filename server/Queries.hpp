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
#include <random>
#include <tuple>

namespace mbench {

template<class Context, class AggregationFunction, class Str>
struct ContextBase {
    using string = Str;
    std::uniform_int_distribution<int> funSelect;

    ContextBase ()
        : funSelect(1, 4)
    {}

    AggregationFunction rndFun() {
        auto self = static_cast<Context*>(this);
        auto f = funSelect(self->rnd());
        switch (f) {
        case 1:
            return self->sum();
        case 2:
            return self->count();
        case 3:
            return self->min();
        case 4:
            return self->max();
        default:
            throw std::runtime_error("Unexpected value");
        }
    }

    template<char C>
    std::tuple<unsigned, unsigned> rndTwoCols() {
        auto fst = rndCol<C>();
        unsigned n = static_cast<Context*>(this)->N();
        if (n <= 10) {
            return std::make_tuple(fst, fst);
        }
        unsigned snd = rndCol<C>();
        while (fst == snd) {
            snd = rndCol<C>();
        }
        return std::make_tuple(fst, snd);
    }

    template<char C>
    unsigned rndCol() {
        auto self = static_cast<Context*>(this);
        unsigned offset = C - 'A';
        unsigned n = static_cast<Context*>(this)->N();
        if (n <= 10) return 0;
        std::uniform_int_distribution<unsigned> dist(0, (n - 1)/10);
        return dist(self->rnd()) * 10 + offset;
    }

    const string& rndSyllable() {
        return static_cast<const Context*>(this)->rndSyllable();
    }
};

template<template <class, class> class Server, class Connection, class Transaction>
struct ScanContext;

template<template <class, class> class Server, class Connection, class Transaction>
struct Q1;

template<template <class, class> class Server, class Connection, class Transaction>
struct Q2;

template<template <class, class> class Server, class Connection, class Transaction>
struct Q3;

template<template <class, class> class Server, class Connection, class Transaction>
struct Q4;

template<template <class, class> class Server, class Connection, class Transaction>
struct Q5;

} // namespace mbench
