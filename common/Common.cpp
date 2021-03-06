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
#include "Common.hpp"
#include <mutex>
#include <cstdio>
#include <vector>

namespace mbench {
namespace {

std::mutex rndMutex;

struct random_device {
    FILE* urandom;
    random_device()
        : urandom(fopen("/dev/urandom", "r"))
    {
    }
    ~random_device() {
        fclose(urandom);
    }
    size_t operator() () {
        size_t res;
        fread(&res, sizeof(res), 1, urandom);
        return res;
    }
};

random_device rndDevice;

}

size_t randomSeed() {
    std::unique_lock<std::mutex> _(rndMutex);
    return rndDevice();
}

uint64_t numTuples(unsigned sf) {
    return uint64_t(sf)*uint64_t(1024*1024);
}

}
