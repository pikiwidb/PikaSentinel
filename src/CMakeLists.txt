AUX_SOURCE_DIRECTORY(. PIKIWIDB_SRC)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src/std)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src/net)
INCLUDE_DIRECTORIES(${jsoncpp_SOURCE_DIR}/include)

find_package(AWSSDK REQUIRED COMPONENTS s3-crt transfer kinesis)
INCLUDE_DIRECTORIES(${AWSSDK_INCLUDE_DIRS})

set(AWSSDK_S3_CRT_LIB "/usr/local/lib/libaws-cpp-sdk-s3-crt.dylib")
set(AWSSDK_TRANSFER_LIB "/usr/local/lib/libaws-cpp-sdk-transfer.dylib")
set(AWSSDK_KINESIS_LIB "/usr/local/lib/libaws-cpp-sdk-kinesis.dylib")
set(AWSSDK_CORE_LIB "/usr/local/lib/libaws-cpp-sdk-core.dylib")

ADD_EXECUTABLE(pikiwidb ${PIKIWIDB_SRC}
        sentinel_service.h
        sentinel_service.cc)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
TARGET_LINK_LIBRARIES(pikiwidb libnet; dl ; fmt; jsoncpp_lib; ${AWSSDK_S3_CRT_LIB} ${AWSSDK_TRANSFER_LIB} ${AWSSDK_KINESIS_LIB} ${AWSSDK_CORE_LIBS})
SET_TARGET_PROPERTIES(pikiwidb PROPERTIES LINKER_LANGUAGE CXX)