
#include <server/Server.hpp>
#include <server/main.hpp>
#include <server/Queries.hpp>

#include <stdlib.h>
#include <iostream>
#include <atomic>

#include <boost/variant.hpp>

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

namespace mbench {

class Connection;

const std::string tName = "maintable";

class Transaction {
    RamCloud &mClient;
    uint32_t mServerspan;
    uint64_t mTableId = 0;
    std::vector<std::pair<uint64_t, std::stringstream>> putOps;
    std::vector<uint64_t> delOps;
    std::vector<uint64_t> getOps;

public: //types
    using Field = boost::variant<int16_t, int32_t, int64_t, float, double, std::string>;
    using Tuple = std::vector<Field>;
    using UpdateOp = std::array<std::pair<unsigned, Field>, 5>;

public:
    Transaction(RamCloud &client, uint32_t serverspan) :
            mClient(client),
            mServerspan(serverspan)
    {}

    static Tuple newTuple(unsigned n) {
        return std::vector<Field>(n);
    }

    uint64_t table() {
        if (mTableId > 0)
            return mTableId;
        try {
            mTableId = mClient.getTableId(tName.c_str());
        } catch (ClientException &e) 
        {
             // std::cout<<"Table doesn't exist. It will be created." << std::endl;
        }
        return mTableId;
    } 

    void createSchema(unsigned nCols, unsigned sf) {
        uint64_t tableId = table();
        if (tableId > 0) {
            //std::cout<<"Table will be dropped and recreated." << std::endl;
            mClient.dropTable(tName.c_str());
        } 
        //std::cout<<"Creating table" << tName << std::endl;
        tableId = mClient.createTable(tName.c_str(), mServerspan);
    }

    void get(uint64_t key) {
        getOps.emplace_back(key);
    }

    void remove(uint64_t key) {
        delOps.emplace_back (key);
    }

    void update(uint64_t key, const UpdateOp& updOp) {
//        std::stringstream val;
//        //TODO: insert a get first...
//        for (uint i = 0; i < updOp.size(); ++i)
//            //if (value[i] != null)
//                val << updOp[i].second;
//        putOps.emplace_back(std::make_pair(key, val));
    }

    void insert(uint64_t key, const Tuple &value) {
//        std::stringstream val;
//        for (uint i = 0; i < value.size(); ++i)
//            val << value[i];
//        putOps.emplace_back (std::make_pair(key, val));
    }

    void commit() {
        auto tableId = table();
        Buffer buffer;
        for (auto const& key: getOps) {
            mClient.read(tableId, (const void*) &key, sizeof(uint64_t), &buffer);
            // no deserialization needed because we do not care
        }
        for (auto & op: putOps) {
            int pos0 = op.second.tellg();
            op.second.seekp(0, std::ios::end);
            uint32_t valSz = op.second.tellg();
            op.second.seekp(pos0, std::ios::beg);
            mClient.write(tableId, (const void*) &op.first, sizeof(uint64_t), op.second.str().c_str(), valSz); //keySize, valueBuffer, valueBufferSize
        }
        for (auto const& key: delOps) {
            mClient.remove(tableId, (const void*) &key, sizeof(uint64_t));
        }
    }
};


template<template <class, class> class Server>
struct Q1<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q1(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
//        std::vector<std::string> projection;
//        int maxColIdx = projection.size();
//        projection.push_back(nameOfCol(0));
//
//        ScannerList scanners;
//        std::vector<KuduRowResult> resultBatch;
//        KuduScanner &scanner = openScan(tx.table(), scanners, projection);
//        double max = 0, val;
//        while (scanner.HasMoreRows()) {
//            assertOk(scanner.NextBatch(&resultBatch));
//            for (KuduRowResult row : resultBatch) {
//                getField(row, maxColIdx, val);
//                max = std::max(max, val);
//            }
//        }
//        tx.commit();
    }
};

template<template <class, class> class Server>
struct Q2<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q2(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
//        unsigned iU, jU;
//        std::tie(iU,jU) = scanContext.template rndTwoCols<'A'>();
//        std::vector<std::string> projection;
//        int maxColIdx = projection.size();
//        projection.push_back(nameOfCol(0));
//        std::string filterColName = nameOfCol(7);
//
//        ScannerList scanners;
//        std::vector<KuduRowResult> resultBatch;
//        KuduScanner &scanner = openScan(tx.table(), scanners, projection,
//                filterColName, double(0.0), KuduPredicate::GREATER_EQUAL,
//                filterColName, double(0.5), KuduPredicate::LESS_EQUAL);
//        double max = 0, val;
//        while (scanner.HasMoreRows()) {
//            assertOk(scanner.NextBatch(&resultBatch));
//            for (KuduRowResult row : resultBatch) {
//                getField(row, maxColIdx, val);
//                max = std::max(max, val);
//            }
//        }
//        tx.commit();
    }
};

template<template <class, class> class Server>
struct Q3<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q3(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
//        std::vector<std::string> projection;
//        std::string filterColName = nameOfCol(4);
//
//        ScannerList scanners;
//        std::vector<KuduRowResult> resultBatch;
//        KuduScanner &scanner = openScan(tx.table(), scanners, projection,
//                filterColName, int16_t(0), KuduPredicate::GREATER_EQUAL,
//                filterColName, int16_t(128), KuduPredicate::LESS_EQUAL);
//        while (scanner.HasMoreRows()) {
//            assertOk(scanner.NextBatch(&resultBatch));
//        }
//        tx.commit();
    }
};

} // namespace mbench
