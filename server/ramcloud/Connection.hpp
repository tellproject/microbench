#pragma once

#include "Transaction.hpp"

#include <stdlib.h>
#include <iostream>
#include <thread>
#include <mutex>

#include "ClusterMetrics.h"
#include "Context.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "ShortMacros.h"
#include "Crc32C.h"
#include "ObjectFinder.h"
#include "RamCloud.h"
#include "Tub.h"
#include "IndexLookup.h"
#include "TableEnumerator.h"

constexpr int session_timeout = 100000;

namespace mbench {

struct ConnectionConfig {
    std::string clustername;
    std::string locator;
    uint32_t serverspan;
};

extern RAMCloud::RamCloud& getInstance(const ConnectionConfig& config);
extern RAMCloud::Context& getInstance(bool);

class Connection {
    uint32_t mServerspan;
    ConnectionConfig mConfig;
public: // types
    using string_type = std::string;
public:
    Connection(const ConnectionConfig& config, boost::asio::io_service&, unsigned) :
        mServerspan(config.serverspan),
        mConfig(config)
    {
    }
    Connection(Connection&&) = delete;
    Connection& operator=(Connection&&) = delete;

    template<class Callback>
    void startTx(mbench::TxType txType, const Callback& callback) {
        Transaction tx (getInstance(mConfig), mServerspan);
        callback(tx);
    }
};

template<template <class, class> class Server>
struct ScanContext<Server, Connection, Transaction>
: public ContextBase<ScanContext<Server, Connection, Transaction>, unsigned, crossbow::string> {
    using string = crossbow::string;
    using AggregationFunction = unsigned;

    Server<Connection, Transaction>& server;

    ScanContext(Server<Connection, Transaction>& server)
        : server(server)
    {}
    ScanContext(ScanContext&&) = delete;
    ScanContext& operator=(ScanContext&&) = delete;

    // dummy implemenations, need to be changed if necessary!!
    constexpr unsigned sum() const {
        return 0;
    }

    constexpr unsigned count() const {
        return 0; 
    }

    constexpr unsigned min() const {
        return 0; 
    }

    constexpr unsigned max() const {
        return 0; 
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

} // namespace mbench

