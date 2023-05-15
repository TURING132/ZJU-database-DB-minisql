# recursive files
FILE(GLOB_RECURSE MAIN_SOURCES ${PROJECT_SOURCE_DIR}/src/*/*.cpp
        ${PROJECT_SOURCE_DIR}/src/*/*/*.cpp
        ${PROJECT_SOURCE_DIR}/src/*/*/*/*.cpp
        ${PROJECT_SOURCE_DIR}/src/*/*.c
        ${PROJECT_SOURCE_DIR}/src/*/*/*.c
        )
MESSAGE(STATUS "Source file lists: ${MAIN_SOURCES}")
ADD_LIBRARY(zSql SHARED ${MAIN_SOURCES})
TARGET_LINK_LIBRARIES(zSql glog)

ADD_EXECUTABLE(main main.cpp)
TARGET_LINK_LIBRARIES(main glog zSql)