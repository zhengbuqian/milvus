#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
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

FetchContent_Declare(
    simdjson
    URL https://github.com/simdjson/simdjson/archive/refs/tags/v3.12.2.tar.gz
    URL_HASH MD5=79dcf1a542c6a5e6fe7214f6b1a38303
)

# Disable simdjson's use of target_compile_features on problematic platforms
if (APPLE AND CMAKE_CXX_COMPILER MATCHES "homebrew.*llvm")
    set(SIMDJSON_DISABLE_DEPRECATED_API ON CACHE BOOL "" FORCE)
    set(CMAKE_CXX_STANDARD 17 CACHE STRING "" FORCE)
    set(CMAKE_CXX_STANDARD_REQUIRED ON CACHE BOOL "" FORCE)
endif()
message(STATUS "Before simdjson FetchContent: CMAKE_CXX_COMPILER_ID = ${CMAKE_CXX_COMPILER_ID}")

# Ensure compiler variables are set for subprojects
if (APPLE AND CMAKE_CXX_COMPILER MATCHES "homebrew.*llvm")
    set(CMAKE_CXX_COMPILER_ID "Clang" CACHE STRING "" FORCE)
    set(CMAKE_C_COMPILER_ID "Clang" CACHE STRING "" FORCE)
    if (NOT CMAKE_CXX_COMPILER_VERSION)
        set(CMAKE_CXX_COMPILER_VERSION "17.0.0" CACHE STRING "" FORCE)
        set(CMAKE_C_COMPILER_VERSION "17.0.0" CACHE STRING "" FORCE)
    endif()
    # Ensure compiler features are set
    set(CMAKE_CXX_COMPILE_FEATURES "cxx_std_11;cxx_std_14;cxx_std_17;cxx_std_20" CACHE STRING "" FORCE)
endif()

# Set variables to pass to FetchContent
set(CMAKE_TRY_COMPILE_CONFIGURATION Release)
set(CMAKE_CXX_COMPILER_WORKS TRUE CACHE INTERNAL "")
set(CMAKE_C_COMPILER_WORKS TRUE CACHE INTERNAL "")

# Populate CMake's internal compiler feature variables for Clang on macOS
if (APPLE AND CMAKE_CXX_COMPILER MATCHES "homebrew.*llvm")
    # This is what CMake would normally detect for Clang
    set(CMAKE_CXX11_COMPILE_FEATURES "cxx_std_11" CACHE INTERNAL "")
    set(CMAKE_CXX14_COMPILE_FEATURES "cxx_std_14" CACHE INTERNAL "")
    set(CMAKE_CXX17_COMPILE_FEATURES "cxx_std_17" CACHE INTERNAL "")
    set(CMAKE_CXX20_COMPILE_FEATURES "cxx_std_20" CACHE INTERNAL "")
    
    # Set the compiler features for the current compiler
    set(CMAKE_CXX_COMPILE_FEATURES 
        cxx_std_98
        cxx_std_11
        cxx_std_14
        cxx_std_17
        cxx_std_20
        cxx_alias_templates
        cxx_alignas
        cxx_alignof
        cxx_attributes
        cxx_auto_type
        cxx_constexpr
        cxx_decltype
        cxx_default_function_template_args
        cxx_defaulted_functions
        cxx_defaulted_move_initializers
        cxx_delegating_constructors
        cxx_deleted_functions
        cxx_enum_forward_declarations
        cxx_explicit_conversions
        cxx_extended_friend_declarations
        cxx_extern_templates
        cxx_final
        cxx_func_identifier
        cxx_generalized_initializers
        cxx_inheriting_constructors
        cxx_inline_namespaces
        cxx_lambdas
        cxx_local_type_template_args
        cxx_long_long_type
        cxx_noexcept
        cxx_nonstatic_member_init
        cxx_nullptr
        cxx_override
        cxx_range_for
        cxx_raw_string_literals
        cxx_reference_qualified_functions
        cxx_right_angle_brackets
        cxx_rvalue_references
        cxx_sizeof_member
        cxx_static_assert
        cxx_strong_enums
        cxx_thread_local
        cxx_trailing_return_types
        cxx_unicode_literals
        cxx_uniform_initialization
        cxx_unrestricted_unions
        cxx_user_literals
        cxx_variadic_macros
        cxx_variadic_templates
        CACHE INTERNAL "")
        
    set(CMAKE_CXX98_COMPILE_FEATURES "cxx_std_98" CACHE INTERNAL "")
endif()

FetchContent_MakeAvailable(simdjson)
message(STATUS "After simdjson FetchContent: CMAKE_CXX_COMPILER_ID = ${CMAKE_CXX_COMPILER_ID}")

set( SIMDJSON_INCLUDE_DIR ${simdjson_SOURCE_DIR}/include CACHE INTERNAL "Path to simdjson include directory" )
