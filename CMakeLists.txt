cmake_minimum_required(VERSION 3.16)
project(actorlib VERSION 0.1.0 LANGUAGES CXX)

option(ACTORLIB_CLANG_TIDY "Run clang-tidy (.clang-tidy) on every compiled source" OFF)
option(ACTORLIB_BUILD_BENCHMARKS "Build the nanobench benchmarks in benchmarks/" OFF)

if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

if(ACTORLIB_CLANG_TIDY)
    find_program(ACTORLIB_CLANG_TIDY_EXE NAMES clang-tidy clang-tidy-18)
    if(NOT ACTORLIB_CLANG_TIDY_EXE)
        message(FATAL_ERROR "ACTORLIB_CLANG_TIDY is ON but clang-tidy was not found")
    endif()
    set(CMAKE_CXX_CLANG_TIDY "${ACTORLIB_CLANG_TIDY_EXE}")
endif()

find_package(Threads REQUIRED)

add_library(actorlib src/actorlib.cpp)
add_library(actorlib::actorlib ALIAS actorlib)
target_link_libraries(actorlib PUBLIC Threads::Threads)

target_compile_features(actorlib PUBLIC cxx_std_20)
set_target_properties(actorlib PROPERTIES CXX_EXTENSIONS OFF)
target_include_directories(actorlib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")

if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    include(CTest)
    if(BUILD_TESTING)
        add_subdirectory(tests)
    endif()
    if(ACTORLIB_BUILD_BENCHMARKS)
        add_subdirectory(benchmarks)
    endif()
endif()
