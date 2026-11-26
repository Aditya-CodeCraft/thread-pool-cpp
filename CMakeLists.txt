cmake_minimum_required(VERSION 3.16)
project(thread_pool_cpp VERSION 1.0.0 LANGUAGES CXX)

option(TP_BUILD_TESTS "Build the GoogleTest suite" ON)
set(TP_SANITIZER "" CACHE STRING "Sanitizers to enable: '', 'address;undefined' or 'thread'")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

find_package(Threads REQUIRED)

add_library(thread_pool src/thread_pool.cpp)
add_library(thread_pool::thread_pool ALIAS thread_pool)
target_include_directories(thread_pool PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_compile_features(thread_pool PUBLIC cxx_std_17)
set_target_properties(thread_pool PROPERTIES CXX_EXTENSIONS OFF)
target_link_libraries(thread_pool PUBLIC Threads::Threads)
target_compile_options(thread_pool PRIVATE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic -Wshadow -Wconversion>)

# Sanitizer flags are PUBLIC so the tests and the library are instrumented together.
if(TP_SANITIZER)
  if("thread" IN_LIST TP_SANITIZER AND "address" IN_LIST TP_SANITIZER)
    message(FATAL_ERROR "ThreadSanitizer cannot be combined with AddressSanitizer")
  endif()
  list(JOIN TP_SANITIZER "," _tp_san)
  target_compile_options(thread_pool PUBLIC -fsanitize=${_tp_san} -fno-omit-frame-pointer -fno-sanitize-recover=all)
  target_link_options(thread_pool PUBLIC -fsanitize=${_tp_san})
endif()

if(TP_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
