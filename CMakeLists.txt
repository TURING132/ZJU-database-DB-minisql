CMAKE_MINIMUM_REQUIRED(VERSION 3.16)

# Suppress cmake policy warning
if (APPLE)
    SET(CMAKE_MACOSX_RPATH 0)
endif()

# Project info
PROJECT(MiniSQL
        VERSION 2022.7
        DESCRIPTION "ZheJiang University 2022~2023 Spring-Summer Database System Project"
        LANGUAGES C CXX)
SET(BUILD_SHARED_LIBS ON)
SET(CMAKE_CXX_STANDARD 17)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable CTest
ENABLE_TESTING()

# Check clang-format
IF (NOT DEFINED CLANG_FORMAT_BIN)
    FIND_PROGRAM(CLANG_FORMAT_BIN NAMES clang-format clang-format-12)
ENDIF()
IF ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
    MESSAGE(WARNING "Could NOT find clang-format.")
ENDIF()

# Options
ADD_DEFINITIONS(-DENABLE_OUTPUT_DBG_INFO)

# Set include directories
SET(THIRD_PARTY_DIR ${PROJECT_SOURCE_DIR}/thirdparty)
SET(SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
SET(TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
SET(GTEST_INCLUDE_DIR ${THIRD_PARTY_DIR}/googletest/include)
SET(GLOG_INCLUDE_DIR ${THIRD_PARTY_DIR}/glog/src)
INCLUDE_DIRECTORIES(${SRC_INCLUDE_DIR} ${TEST_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${GTEST_INCLUDE_DIR} ${GLOG_INCLUDE_DIR})

# Set cmake module path
LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# Thirdparty module gtest
ADD_SUBDIRECTORY(${THIRD_PARTY_DIR}/googletest ${CMAKE_BINARY_DIR}/googletest-build)
TARGET_COMPILE_OPTIONS(gtest PRIVATE "-fPIC")
TARGET_COMPILE_OPTIONS(gtest_main PRIVATE "-fPIC")
# Thirdparty module glog
ADD_SUBDIRECTORY(${THIRD_PARTY_DIR}/glog ${CMAKE_BINARY_DIR}/glog-build)

# Compile options in debug mode
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb")
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fno-optimize-sibling-calls")
#SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls")
if (APPLE)
    # Note: memory leak detection is not currently supported in macOS BigSur
    MESSAGE(STATUS "For macOS 10.x, you need to set ASAN_OPTIONS=detect_leaks=1 to detect memory leak.")
endif()
# Compile options in cxx
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -Wextra -Werror -Wno-unused-parameter")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -std=gnu++11 -fPIC -Wall -Wextra -Wattributes -Wunused-parameter")

# Subdirectory
ADD_SUBDIRECTORY(src ${CMAKE_BINARY_DIR}/bin)
ADD_SUBDIRECTORY(test ${CMAKE_BINARY_DIR}/test)

# Output messages
MESSAGE(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
MESSAGE(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
MESSAGE(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
MESSAGE(STATUS "CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")
MESSAGE(STATUS "CMAKE_BINARY_DIR: ${CMAKE_BINARY_DIR}")