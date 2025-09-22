# Copyright (C) 2019-2024 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

include_directories(${CMAKE_HOME_DIRECTORY}/src)
include_directories(${CMAKE_HOME_DIRECTORY}/unittest)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Collect source files from main directory and subdirectories
set(MAIN_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/scalar_filter_benchmark.cpp
)
file(GLOB CONFIG_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/config/*.cpp")
file(GLOB CORE_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/core/*.cpp")
file(GLOB DICT_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/dictionaries/*.cpp")
file(GLOB GEN_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/generators/*.cpp")
file(GLOB UTILS_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/utils/*.cpp")

set(SCALAR_BENCH_SRCS
    ${MAIN_SRCS}
    ${CONFIG_SRCS}
    ${CORE_SRCS}
    ${DICT_SRCS}
    ${GEN_SRCS}
    ${UTILS_SRCS}
)

add_executable(scalar_filter_bench ${SCALAR_BENCH_SRCS})

target_compile_definitions(scalar_filter_bench
    PRIVATE SCALAR_BENCH_CASES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/bench_cases"
)

target_link_libraries(scalar_filter_bench
    milvus_core
    milvus-storage
    milvus-common
    pthread
)

# Set C++ standard
set_target_properties(scalar_filter_bench PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# Add compile options for performance
target_compile_options(scalar_filter_bench PRIVATE
    -O3
    -march=native
)

install(TARGETS scalar_filter_bench DESTINATION unittest)
