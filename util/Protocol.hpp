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
#include <crossbow/Protocol.hpp>

namespace mbench {
GEN_COMMANDS(Commands, (CreateSchema, Populate));

enum class TxType : uint32_t {
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
    using arguments = unsigned; // scaling factor
    using result = std::tuple<bool, crossbow::string>;
};

}
