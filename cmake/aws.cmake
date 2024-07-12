# Fetch and configure jsoncpp


FetchContent_Declare(
        aws-sdk-cpp
        GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
        GIT_TAG 1.11.50
)
FetchContent_MakeAvailable(aws-sdk-cpp)