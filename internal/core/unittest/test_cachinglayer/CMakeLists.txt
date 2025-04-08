set(CACHINGLAYER_TEST_FILES
    ../init_gtest.cpp
    test_dlist.cpp
    test_cache_slot.cpp
)

add_executable(cachinglayer_test
    ${CACHINGLAYER_TEST_FILES}
)

target_link_libraries(cachinglayer_test
    gtest
    gmock
    milvus_core
    milvus-storage
)

install(TARGETS cachinglayer_test DESTINATION unittest)
