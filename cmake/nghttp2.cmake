# nodejs/nghttp2
FETCHCONTENT_DECLARE(
        nghttp2
        URL https://github.com/nghttp2/nghttp2/releases/download/v1.62.0/nghttp2-1.62.0.tar.gz
        URL_HASH MD5=673fa9c5deba004b5f72f4bbca004d72
        DOWNLOAD_NO_PROGRESS 1
        UPDATE_COMMAND ""
        LOG_CONFIGURE 1
        LOG_BUILD 1
        LOG_INSTALL 1
        BUILD_ALWAYS 1
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
        -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
        -DBUILD_STATIC_LIBS=ON
        -DBUILD_SHARED_LIBS=OFF
        BUILD_COMMAND make -j${CPU_CORE}
)
FETCHCONTENT_MAKEAVAILABLE(nghttp2)