include(FetchContent)
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()
FetchContent_Declare(doctest
    URL https://codeload.github.com/doctest/doctest/zip/refs/tags/v2.4.12
    URL_HASH SHA256=7a7afb5f70d0b749d49ddfcb8a454299a8fcd53e9db9c131abe99b456e88a1fe
)
FetchContent_MakeAvailable(doctest)
# Include doctest as a system header so compiler warnings and clang-tidy skip it.
get_target_property(doctest_include_dirs doctest INTERFACE_INCLUDE_DIRECTORIES)
set_target_properties(doctest PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${doctest_include_dirs}")

add_executable(actorlib_tests mailbox_arena.cpp virtual_worker.cpp)
target_link_libraries(actorlib_tests PRIVATE actorlib::actorlib doctest::doctest)
add_test(NAME actorlib_tests COMMAND actorlib_tests)
