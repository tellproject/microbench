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

namespace mbench {

template<class Context>
struct ContextBase {
    std::uniform_int_distribution<int> funSelect;

    ContextBase ()
        : funSelect(1, 4)
    {}

    auto rndFun() -> decltype(this->sum()) {
        auto f = funSelect(this->rnd());
        switch (f) {
        case 1:
            return this->sum();
        case 2:
            return this->count();
        case 3:
            return this->min();
        case 4:
            return this->max();
        }
    }
};

template<class Connection>
struct ScanContext;

template<class Connection>
struct Q1;

} // namespace mbench
