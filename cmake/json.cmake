# Fetch and configure jsoncpp

FetchContent_Declare(
        jsoncpp
        GIT_REPOSITORY https://github.com/open-source-parsers/jsoncpp.git
        GIT_TAG 1.9.5
)
FetchContent_MakeAvailable(jsoncpp)