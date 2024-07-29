
AUX_SOURCE_DIRECTORY(. PIKIWIDB_SRC)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src/pstd)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src/std)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src/net)

find_package(AWSSDK REQUIRED COMPONENTS  s3-crt transfer kinesis)
include_directories(${AWS_INCLUDE_DIR})

ADD_EXECUTABLE(pikiwidb ${PIKIWIDB_SRC}
        cmd_thread_pool.cc
        base_cmd.cc
        cmd_kv.h
        cmd_kv.cc
        cmd_kv.cc
        cmd_thread_pool_worker.h
        cmd_thread_pool_worker.cc
        cmd_table_manager.cc)

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
SET(CURL_LIBRARY "-lcurl")

FIND_PACKAGE(CURL REQUIRED)

TARGET_INCLUDE_DIRECTORIES(pikiwidb PRIVATE
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_SOURCE_DIR}/src/net
        )
TARGET_LINK_LIBRARIES(pikiwidb libnet; dl ; fmt; nlohmann_json::nlohmann_json; ${CURL_LIBRARIES}; ${AWSSDK_LINK_LIBRARIES}; pstd)
SET_TARGET_PROPERTIES(pikiwidb PROPERTIES LINKER_LANGUAGE CXX)
