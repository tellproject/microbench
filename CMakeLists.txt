cmake_minimum_required(VERSION 3.0.2)
project(microbenchmark)

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(Boost REQUIRED COMPONENTS system)
find_package(Crossbow REQUIRED)
find_package(Jemalloc REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16")

find_package(Threads)

add_executable(mbclient client/main.cpp)
target_include_directories(mbclient PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mbclient PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories(mbclient PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(mbclient ${Boost_LIBRARIES})
target_link_libraries(mbclient ${CMAKE_THREAD_LIBS_INIT})

add_executable(mbserver server/main.cpp)
target_include_directories(mbserver PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mbserver PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories(mbserver PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(mbserver ${Boost_LIBRARIES})
target_link_libraries(mbserver ${CMAKE_THREAD_LIBS_INIT})

