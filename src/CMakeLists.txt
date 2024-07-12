cmake_minimum_required(VERSION 3.1)

project(pikiwidb)

# Use C++11 standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Disable AWS SDK testing
option(BUILD_TESTING "Build testing" OFF)

# Set the CMAKE_PREFIX_PATH to use Homebrew's curl
set(CMAKE_PREFIX_PATH "/opt/homebrew/opt/curl/")

# Additional options for AWS SDK
set(AUTORUN_UNIT_TESTS OFF)
set(BUILD_ONLY "s3")
set(CMAKE_INSTALL_PREFIX "~/sdk-install")
AUX_SOURCE_DIRECTORY(. PIKIWIDB_SRC)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src/std)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src/net)
include_directories(${jsoncpp_SOURCE_DIR}/include)
find_package(AWSSDK REQUIRED COMPONENTS s3-crt transfer kinesis)
include_directories(${AWS_INCLUDE_DIR})

ADD_EXECUTABLE(pikiwidb ${PIKIWIDB_SRC}
        sentinel_service.h
        sentinel_service.cc)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
TARGET_LINK_LIBRARIES(pikiwidb libnet; dl ; fmt; ${AWSSDK_LINK_LIBRARIES})
SET_TARGET_PROPERTIES(pikiwidb PROPERTIES LINKER_LANGUAGE CXX)