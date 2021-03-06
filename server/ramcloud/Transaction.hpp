
#include <server/Server.hpp>
#include <server/main.hpp>
#include <server/Queries.hpp>

#include <crossbow/Serializer.hpp>
#include <crossbow/string.hpp>

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


namespace mbench {

struct Record10 {
    using is_serializable = crossbow::is_serializable;
    double A;
    int32_t B;
    int32_t C;
    int16_t D;
    int16_t E;
    int64_t F;
    int64_t G;
    double H;
    crossbow::string I;
    crossbow::string J;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & A;
        ar & B;
        ar & C;
        ar & D;
        ar & E;
        ar & F;
        ar & G;
        ar & H;
        ar & I;
        ar & J;
    }
};

class Connection;

class Transaction {
    RAMCloud::RamCloud &mClient;
    uint32_t mServerspan;
    uint64_t mTableId = 0;
    std::vector<std::pair<uint64_t, Record10>> putOps;
    std::vector<uint64_t> delOps;
    std::vector<uint64_t> getOps;
    static const std::string tName;
    Transaction(Transaction&&) = delete;
    Transaction& operator=(Transaction&&) = delete;

public:
    void static deserialize(const void* addr, Record10 &record) {
        crossbow::deserializer des(reinterpret_cast<const uint8_t*>(addr));
        des & record;
    }

private:
    void write(uint64_t key, Record10 &record, uint64_t tableId){
        crossbow::sizer sizer;
        sizer & record;

        crossbow::serializer ser(sizer.size);
        ser & record;
        mClient.write(tableId, (const void*) &key, sizeof(uint64_t), ser.buffer.get(), sizer.size);
    }


    void read(uint64_t key, Record10 &record, uint64_t tableId) {
        RAMCloud::Buffer buffer;
        mClient.read(tableId, (const void*) &key, sizeof(uint64_t), &buffer);
        deserialize(buffer.getRange(0, buffer.size()), record);
    }

public: //types
    using Field = boost::variant<int16_t, int32_t, int64_t, float, double, std::string>;
    using Tuple = std::vector<Field>;
    using UpdateOp = std::array<std::pair<unsigned, Field>, 5>;

public:
    Transaction(RAMCloud::RamCloud &client, uint32_t serverspan) :
            mClient(client),
            mServerspan(serverspan)
    {}

    static Tuple newTuple(unsigned n) {
        return std::vector<Field>(n);
    }

    uint64_t table() {
        if (mTableId > 0)
            return mTableId;
        mTableId = mClient.getTableId(tName.c_str());
        return mTableId;
    }

    RAMCloud::RamCloud& client() {
        return mClient;
    }

    void createSchema(unsigned nCols, unsigned sf) {
        if (nCols != 10)
            throw std::runtime_error("number of columns must be 10, anything else is unsupported!");
        // uint64_t tableId = table();
        // if (tableId > 0) {
        //     //std::cout<<"Table will be dropped and recreated." << std::endl;
        //     mClient.dropTable(tName.c_str());
        // } 
        std::cout<<"Creating table " << tName << std::endl;
        mTableId = mClient.createTable(tName.c_str(), mServerspan);
    }

    void get(uint64_t key) {
        getOps.emplace_back(key);
    }

    void remove(uint64_t key) {
        delOps.emplace_back (key);
    }

    void update(uint64_t key, const UpdateOp& updOp) {
        auto tableId = table();
        Record10 record;
        read(key, record, tableId);
        for(auto updPair : updOp) {
            auto value = updPair.second;
            switch(updPair.first) {
            case 0:
                record.A = boost::get<double>(value);
                break;
            case 1:
                record.B = boost::get<int32_t>(value);
                break;
            case 2:
                record.C = boost::get<int32_t>(value);
                break;
            case 3:
                record.D = boost::get<int16_t>(value);
                break;
            case 4:
                record.E = boost::get<int16_t>(value);
                break;
            case 5:
                record.F = boost::get<int64_t>(value);
                break;
            case 6:
                record.G = boost::get<int64_t>(value);
                break;
            case 7:
                record.H = boost::get<double>(value);
                break;
            case 8:
                record.I = boost::get<std::string>(value);
                break;
            case 9:
                record.J = boost::get<std::string>(value);
            }
        }
        putOps.emplace_back(std::make_pair(key, std::move(record)));
    }

    void insert(uint64_t key, const Tuple &value) {
        auto tableId = table();
        Record10 record;
        record.A = boost::get<double>(value[0]);
        record.B = boost::get<int32_t>(value[1]);
        record.C = boost::get<int32_t>(value[2]);
        record.D = boost::get<int16_t>(value[3]);
        record.E = boost::get<int16_t>(value[4]);
        record.F = boost::get<int64_t>(value[5]);
        record.G = boost::get<int64_t>(value[6]);
        record.H = boost::get<double>(value[7]);
        record.I = boost::get<std::string>(value[8]);
        record.J = boost::get<std::string>(value[9]);
        write(key, record, tableId);
    }

    void commit() {
        auto tableId = table();
        RAMCloud::Buffer buffer;
        {
            std::vector<RAMCloud::Tub<RAMCloud::ObjectBuffer>> tubs(getOps.size());
            std::unique_ptr<RAMCloud::MultiReadObject*[]> readRequest(new RAMCloud::MultiReadObject*[getOps.size()]);
            for (unsigned i = 0; i < getOps.size(); ++i) {
                uint64_t key = getOps[i];
                readRequest[i] = new RAMCloud::MultiReadObject(tableId, &key, sizeof(key), &(tubs[i]));
            }
            // no deserialization needed because we do not care for the precise result
            mClient.multiRead(readRequest.get(), getOps.size());
            for (unsigned i = 0; i < getOps.size(); ++i) {
                delete readRequest[i];
            }
        }
        {
            std::vector<crossbow::serializer> serializers;
            serializers.reserve(putOps.size());
            std::unique_ptr<RAMCloud::MultiWriteObject*[]> writeReqs(new RAMCloud::MultiWriteObject*[putOps.size()]);
            for (unsigned i = 0; i < putOps.size(); ++i) {
                uint64_t key = putOps[i].first;
                const auto& rec = putOps[i].second;
                crossbow::sizer sizer;
                sizer & rec;

                serializers.emplace_back(sizer.size);
                auto& ser = serializers.back();
                ser & rec;
                writeReqs[i] = new RAMCloud::MultiWriteObject(tableId, &key, sizeof(key),
                        ser.buffer.get(), sizer.size);
            }
            mClient.multiWrite(writeReqs.get(), putOps.size());
            for (unsigned i = 0; i < putOps.size(); ++i) {
                delete writeReqs[i];
            }
        }
        {
            std::unique_ptr<RAMCloud::MultiRemoveObject*[]> remReqs(new RAMCloud::MultiRemoveObject*[delOps.size()]);
            for (unsigned i = 0; i < delOps.size(); ++i) {
                uint64_t key = delOps[i];
                remReqs[i] = new RAMCloud::MultiRemoveObject(tableId, &key, sizeof(key));
            }
            mClient.multiRemove(remReqs.get(), delOps.size());
            for (unsigned i = 0; i < delOps.size(); ++i) {
                delete remReqs[i];
            }
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
        auto tableId = tx.table();
        uint32_t keyLength, dataLength;
        Record10 record;
        double max = 0;
        RAMCloud::TableEnumerator tEnum(tx.client(), tableId, false);
        while (tEnum.hasNext()) {
            const uint64_t *key;
            const void* objs = nullptr;
            tEnum.nextKeyAndData(&keyLength, reinterpret_cast<const void**>(&key), &dataLength, &objs);
            Transaction::deserialize(objs, record);
            max = std::max(max, record.A);
        }
    }
};

template<template <class, class> class Server>
struct Q2<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q2(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        throw std::runtime_error("Query 2 not implemented for Ramcloud");
    }
};

template<template <class, class> class Server>
struct Q3<Server, Connection, Transaction> {
    ScanContext<Server, Connection, Transaction>& scanContext;

    Q3(ScanContext<Server, Connection, Transaction>& scanContext)
        : scanContext(scanContext)
    {}

    void operator() (Transaction& tx) {
        throw std::runtime_error("Query 3 not implemented for Ramcloud");
    }
};

} // namespace mbench
