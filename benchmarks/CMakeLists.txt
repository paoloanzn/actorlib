include(FetchContent)
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()
FetchContent_Declare(nanobench
    URL https://codeload.github.com/martinus/nanobench/zip/refs/tags/v4.6.0
    URL_HASH SHA256=f548c78039bdda85878d341afb20b6214db104e210c52ec560c75d53b6137ae8
)
FetchContent_MakeAvailable(nanobench)
# nanobench is third-party: no clang-tidy on its implementation file, and a system include
# so compiler warnings and clang-tidy skip its header.
set_target_properties(nanobench PROPERTIES CXX_CLANG_TIDY "")
get_target_property(nanobench_include_dirs nanobench INTERFACE_INCLUDE_DIRECTORIES)
set_target_properties(nanobench PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${nanobench_include_dirs}")

if(NOT CMAKE_BUILD_TYPE MATCHES "^(Release|RelWithDebInfo)$")
    message(WARNING "actorlib benchmarks: CMAKE_BUILD_TYPE is '${CMAKE_BUILD_TYPE}', numbers are only meaningful in Release")
endif()

add_executable(actorlib_benchmarks mailbox_arena.cpp)
target_link_libraries(actorlib_benchmarks PRIVATE actorlib::actorlib nanobench::nanobench)
