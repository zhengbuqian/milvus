add_library(cachinglayer_test_utils
    cachinglayer_test_utils.h
)

target_link_libraries(cachinglayer_test_utils
    PRIVATE
    cachinglayer
    gtest
    folly
)

add_executable(cachinglayer_test
    cache_slot_test.cpp
    eviction_manager_test.cpp
)

target_link_libraries(cachinglayer_test
    PRIVATE
    cachinglayer_test_utils
    cachinglayer
    gtest
    folly
)

add_test(NAME cachinglayer_test COMMAND cachinglayer_test)