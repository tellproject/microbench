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
GEN_COMMANDS(Commands, (CreateSchema, Populate, T1, T2, T3, T5, Q1));

enum class TxType : uint32_t {
    RW, RO, A
};

struct err_msg {
    using is_serializable = crossbow::is_serializable;
    bool success;
    crossbow::string msg;

    template<class Archiver>
    void operator& (Archiver& ar) {
        ar & success;
        ar & msg;
    }
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

template<>
struct Signature<Commands::T1> {
    struct arguments {
        uint64_t baseInsertKey;
        uint64_t numClients;
    };
    struct result {
        using is_serializable = crossbow::is_serializable;
        bool success;
        crossbow::string msg;
        uint64_t lastInsert;

        template<class Archiver>
        void operator& (Archiver& ar) {
            ar & success;
            ar & msg;
            ar & lastInsert;
        }
    };
};

template<>
struct Signature<Commands::T2> {
    struct arguments {
        uint64_t baseDeleteKey;
        uint64_t numClients;
    };
    struct result {
        using is_serializable = crossbow::is_serializable;
        bool success;
        crossbow::string msg;
        uint64_t lastDelete;

        template<class Archiver>
        void operator& (Archiver& ar) {
            ar & success;
            ar & msg;
            ar & lastDelete;
        }
    };
};

template<>
struct Signature<Commands::T3> {
    struct arguments {
        uint64_t baseInsertKey;
        uint64_t baseDeleteKey;
        uint64_t numClients;
        uint64_t clientId;
    };
    struct result {
        using is_serializable = crossbow::is_serializable;
        bool success;
        crossbow::string msg;

        template<class Archiver>
        void operator& (Archiver& ar) {
            ar & success;
            ar & msg;
        }
    };
};

template<>
struct Signature<Commands::T5> {
    using arguments = typename Signature<Commands::T3>::arguments;
    using result = typename Signature<Commands::T3>::result;
};

template<>
struct Signature<Commands::Q1> {
    using arguments = void;
    using result = err_msg;
};

}
