if(NOT CMAKE_BUILD_TYPE MATCHES "^(Release|RelWithDebInfo)$")
    message(WARNING "actorlib benchmarks: CMAKE_BUILD_TYPE is '${CMAKE_BUILD_TYPE}', numbers are only meaningful in Release")
endif()

# The headline benchmarks. benchmarks/bench.sh builds them against main and against the
# working tree and compares the two.
add_executable(actorlib_benchmarks runtime.cpp)
target_link_libraries(actorlib_benchmarks PRIVATE actorlib::actorlib)
