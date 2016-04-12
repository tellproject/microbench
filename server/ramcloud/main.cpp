#include "Connection.hpp"
#include <server/Server.hpp>
#include <server/main.hpp>

int main(int argc, const char* argv[]) {
    namespace po = boost::program_options;
    return mbench::mainFun<mbench::Connection, mbench::Transaction, mbench::ConnectionConfig>(argc, argv,
            [](po::options_description& desc, mbench::ConnectionConfig& config) {
                desc.add_options()
                    ("clustername,c", po::value<std::string>()->required(), "RamCLOUD cluster name")
                    ("locator,l", po::value<std::string>()->required(), "RamCLOUD locator name")
                    ("serverspan,x", po::value<uint32_t>(&config.serverspan)->required(), "RamCloud server span (number of storage nodes to shard across)")
                    ;
            }, [](po::variables_map& vm, mbench::ConnectionConfig& config) {
                config.clustername = vm["clustername"].as<std::string>();
                config.locator = vm["locator"].as<std::string>();
            });
}
