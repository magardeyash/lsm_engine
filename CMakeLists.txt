cmake_minimum_required(VERSION 3.14)
project(lsm_engine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Options
option(LSM_ENABLE_TESTING "Enable building tests" ON)
option(LSM_ENABLE_BENCH "Enable building benchmark" ON)

# No optional zstd for simple build

# Core library
file(GLOB_RECURSE SOURCES "src/*.cc")
add_library(lsm_engine STATIC ${SOURCES})
target_include_directories(lsm_engine PUBLIC include PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

find_package(Threads REQUIRED)
target_link_libraries(lsm_engine Threads::Threads)

# Tests
if(LSM_ENABLE_TESTING)
    enable_testing()
    include(FetchContent)
    FetchContent_Declare(
      googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    )
    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    file(GLOB TEST_SOURCES "tests/test_*.cc")
    list(REMOVE_ITEM TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_bench.cc")

    foreach(test_file ${TEST_SOURCES})
        get_filename_component(test_name ${test_file} NAME_WE)
        add_executable(${test_name} ${test_file})
        target_include_directories(${test_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
        target_link_libraries(${test_name} lsm_engine gtest_main)
        add_test(NAME ${test_name} COMMAND ${test_name})
    endforeach()
endif()

# Benchmark
if(LSM_ENABLE_BENCH AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_bench.cc")
    add_executable(lsm_bench tests/test_bench.cc)
    target_include_directories(lsm_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(lsm_bench lsm_engine)
endif()

# Example integration
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/demo.cc")
    add_executable(lsm_demo examples/demo.cc)
    target_include_directories(lsm_demo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(lsm_demo lsm_engine)
endif()
