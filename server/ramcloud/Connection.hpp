#pragma once

#include "Transaction.hpp"

#include <stdlib.h>
#include <iostream>
#include <thread>

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

using namespace RAMCloud;

const int session_timeout = 100000;

namespace mbench {

    struct ConnectionConfig {
        std::string clustername;
        std::string locator;
        uint32_t serverspan;
    };
    
    class Connection {
        std::unique_ptr<RamCloud> mClient;
        uint32_t mServerspan;
    public: // types
        using string_type = std::string;
    public:
        Connection(const ConnectionConfig& config, boost::asio::io_service&, unsigned) :
            mServerspan(config.serverspan)
        {
            Context context(false);
            context.transportManager->setSessionTimeout(session_timeout);
            mClient.reset(new RamCloud(&context, config.locator.c_str(), config.clustername.c_str()));
        }
    
        template<class Callback>
        void startTx(mbench::TxType txType, const Callback& callback) {
            Transaction tx (*mClient, mServerspan);
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

