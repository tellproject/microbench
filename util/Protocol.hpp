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
#include <chrono>

namespace mbench {
using Clock = std::chrono::steady_clock;

GEN_COMMANDS(Commands, (CreateSchema, Populate, BatchOp, Q1, Q2, Q3));

std::string cmdString(Commands cmd);

enum class TxType : uint32_t {
    RW, RO, A
};

struct err_msg {
    using is_serializable = crossbow::is_serializable;
    bool success;
    crossbow::string msg;
    long responseTime;

    template<class Archiver>
    void operator& (Archiver& ar) {
        ar & success;
        ar & msg;
        ar & responseTime;
    }
};

template<Commands cmd>
struct Signature;

template<>
struct Signature<Commands::Populate> {
    using arguments = std::tuple<uint64_t, uint64_t>;
    using result = std::tuple<bool, crossbow::string, long>;
};

template<>
struct Signature<Commands::CreateSchema> {
    using arguments = unsigned; // scaling factor
    using result = std::tuple<bool, crossbow::string>;
};

struct BatchOp {
    unsigned numOps;
    double insertProb;
    double deleteProb;
    double updateProb;
    unsigned clientId;
    uint64_t numClients;
    uint64_t baseInsertKey;
    uint64_t baseDeleteKey;
};

struct BatchResult {
    using is_serializable = crossbow::is_serializable;
    bool success;
    crossbow::string msg;
    uint64_t baseInsertKey;
    uint64_t baseDeleteKey;
    long responseTime;

    template<class Archiver>
    void operator& (Archiver& ar) {
        ar & success;
        ar & msg;
        ar & baseInsertKey;
        ar & baseDeleteKey;
        ar & responseTime;
    }
};

template<>
struct Signature<Commands::BatchOp> {
    using arguments = BatchOp;
    using result = BatchResult;
};

template<>
struct Signature<Commands::Q1> {
    using arguments = void;
    using result = err_msg;
};

template<>
struct Signature<Commands::Q2> {
    using arguments = void;
    using result = err_msg;
};

template<>
struct Signature<Commands::Q3> {
    using arguments = void;
    using result = err_msg;
};

}
