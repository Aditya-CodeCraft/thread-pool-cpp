include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
  URL_HASH SHA256=7b42b4d6ed48810c5362c265a17faebe90dc2373c885e5216439d37927f02926
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(thread_pool_tests
  test_thread_safe_queue.cpp
  test_thread_pool.cpp
)
target_link_libraries(thread_pool_tests PRIVATE thread_pool::thread_pool GTest::gtest_main)

include(GoogleTest)
# DISCOVERY_MODE PRE_TEST: list tests at ctest time, so sanitizer runtimes are
# not required at build time.
gtest_discover_tests(thread_pool_tests DISCOVERY_MODE PRE_TEST PROPERTIES TIMEOUT 120)
