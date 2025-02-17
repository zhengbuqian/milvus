#-------------------------------------------------------------------------------
# Copyright (C) 2019-2025 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------

message(STATUS "Building eyrie from source")

set(GIT_REPOSITORY  "https://github.com/zilliztech/eyrie.git")
set(GIT_TAG "eyrie-dev")

message(STATUS "Eyrie repo: ${GIT_REPOSITORY}")
message(STATUS "Eyrie version: ${GIT_TAG}")

message(STATUS "Building eyrie-${GIT_TAG} from source")
message(STATUS ${CMAKE_BUILD_TYPE})

set( CMAKE_PREFIX_PATH ${CONAN_BOOST_ROOT} )

FetchContent_Declare(
        eyrie
        GIT_REPOSITORY  ${GIT_REPOSITORY}
        GIT_TAG         ${GIT_TAG}
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/eyrie-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/eyrie-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

FetchContent_GetProperties(eyrie)
if ( NOT eyrie_POPULATED )
    FetchContent_Populate(eyrie)
    add_subdirectory( ${eyrie_SOURCE_DIR}
                      ${eyrie_BINARY_DIR} )
endif()

set( EYRIE_INCLUDE_DIR ${eyrie_SOURCE_DIR}/include CACHE INTERNAL "Path to eyrie include directory" )
