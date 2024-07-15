include_guard()

FETCHCONTENT_DECLARE(
        etcd
        GIT_REPOSITORY https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git
)
FetchContent_MakeAvailable(etcd)