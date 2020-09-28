cmake_minimum_required (VERSION 3.2.2)

include(ExternalProject)
project(FASTER)

# The set of additional options that we can pass into cmake. These include
# value size (VALUE_SIZE), the storage medium to use for the hybrid log
# (USE_AZURE), the transport layer that the system should use for requests
# processing (ENABLE_INFINIBAND) and whether to include unit tests (TESTS).
set(VALUE_SIZE "8" CACHE STRING "Value size the system should be compiled with")
OPTION(USE_AZURE "Extend FASTER's hybrid log to blob store" OFF)
OPTION(ENABLE_INFINIBAND "Use an Infrc based network stack" OFF)
OPTION(TESTS "Compile unit tests" OFF)

# The basic set of libraries required in order to build benchmarks and tests.
set (LIBS
     faster
     stdc++fs
     uuid
     tbb
     gcc
     aio
     m
     stdc++
     pthread
     boost_program_options
     boost_system
     boost_thread
     crypto
     ssl
)

# Libraries required if we're using Azure blob store.
set (AZURE
     azurestorage
     cpprest
)

# Libraries required if we're using Infiniband for the server and client.
set (INFRC
     ibverbs
     rdmacm
)

# Basic setup for Debug and Release builds. If we're using blob store or
# infiniband, then ensure we pass those flags in during compilation and
# also make sure we link in the correct libraries.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DVALUE_SIZE=${VALUE_SIZE}")

if(USE_AZURE)
  set(LIBS ${LIBS} ${AZURE})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_AZURE=1")
endif()

if(ENABLE_INFINIBAND)
  set(LIBS ${LIBS} ${INFRC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_INFINIBAND=1")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Og -g -D_DEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")

# Always set _DEBUG compiler directive when compiling bits.
set_directory_properties(PROPERTIES COMPILE_DEFINITIONS_DEBUG "_DEBUG")

include_directories(${CMAKE_SOURCE_DIR}/src)

# Set the directory targets when build in libs and binaries.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

# Function to automate building benchmark binaries.
FUNCTION(ADD_BENCH BENCH)
  add_executable(${BENCH} ${BENCH_HEADERS} ${BENCH}.cc)
  target_link_libraries(${BENCH} ${LIBS})
ENDFUNCTION()

# Build benchmarks and the faster library.
add_subdirectory(src)
add_subdirectory(benchmark-dir)

# If testing is enabled, install gtest and compile unit tests.
if(TESTS)
  enable_testing()

  # Set the link libraries for test compilation.
  set(TEST_LIBS ${LIBS} ${AZURE} ${INFRC} gtest)

  ##### BEGIN GOOGLE TEST INSTALLATION #####
  # Download and unpack googletest at configure time
  configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
  execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
  if(result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
  endif()
  execute_process(COMMAND ${CMAKE_COMMAND} --build .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
  if(result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
  endif()

  # Prevent overriding the parent project's compiler/linker
  # settings on Windows
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

  # Add googletest directly to our build. This defines
  # the gtest and gtest_main targets.
  add_subdirectory(${CMAKE_BINARY_DIR}/googletest-src
                   ${CMAKE_BINARY_DIR}/googletest-build
                   EXCLUDE_FROM_ALL)

  ##### END GOOGLE TEST INSTALLATION #####

  #Function to automate building test binaries.
  FUNCTION(ADD_UNIT_TEST TEST_NAME HEADERS)
    add_executable(${TEST_NAME} ${HEADERS} ${TEST_NAME}.cc)
    target_link_libraries(${TEST_NAME} ${TEST_LIBS})
    add_test(${TEST_NAME} ${CMAKE_BINARY_DIR}/${TEST_NAME})
  ENDFUNCTION()

  # Build tests.
  add_subdirectory(test)
endif()
