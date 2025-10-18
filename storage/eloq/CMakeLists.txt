# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1335 USA
cmake_minimum_required(VERSION 3.8)
project(eloq)

set(WITH_DATA_STORE "ELOQDSS_ROCKSDB" CACHE STRING "Which key-value storage to use")
set_property(CACHE WITH_DATA_STORE PROPERTY STRINGS  "DYNAMODB" "BIGTABLE" "ELOQDSS_ROCKSDB_CLOUD_S3" "ELOQDSS_ELOQSTORE" "ELOQDSS_ROCKSDB")
message(NOTICE "WITH_DATA_STORE" : ${WITH_DATA_STORE})

set(METRICS_LIB "eloq-metrics" CACHE STRING "metrics library name.")

option(WITH_LOG_SERVICE "Compile with built-in log service." ON)

if(WITH_LOG_SERVICE)
	message("WITH_LOG_SERVICE: " ${WITH_LOG_SERVICE})
	add_compile_definitions(WITH_LOG_SERVICE)
# Add compile flags for LOG STATE TYPE
if(WITH_LOG_STATE STREQUAL "MEMORY")
  add_compile_definitions(LOG_STATE_TYPE_MEM)
elseif(WITH_LOG_STATE STREQUAL "ROCKSDB")
  add_compile_definitions(LOG_STATE_TYPE_RKDB)
  add_compile_definitions(LOG_STATE_TYPE_RKDB_ALL)
elseif(WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_S3")
  add_compile_definitions(LOG_STATE_TYPE_RKDB_S3)
elseif(WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_GCS")
  add_compile_definitions(LOG_STATE_TYPE_RKDB_GCS)
else()
  message(FATAL_ERROR "Unknown WITH_LOG_STATE: ${WITH_LOG_STATE}")
endif()
endif()

option(OPEN_LOG_SERVICE "Compile with open log service." ON)
message("OPEN_LOG_SERVICE: " ${OPEN_LOG_SERVICE})
if (OPEN_LOG_SERVICE)
	add_compile_definitions(OPEN_LOG_SERVICE=1)
endif()
set(STATISTICS ON CACHE BOOL "Turn on STATISTICS")

if(ELOQ_MODULE_ENABLED)
    add_compile_definitions(ELOQ_MODULE_ENABLED)
endif()

if (EXT_TX_PROC_ENABLED)
	add_compile_definitions(EXT_TX_PROC_ENABLED)
endif()

if(SMALL_RANGE)
    add_compile_definitions(SMALL_RANGE)
endif()

if(STATISTICS)
    add_compile_definitions(STATISTICS)
endif()

find_package(MIMALLOC REQUIRED)
include_directories(${MIMALLOC_INCLUDE_DIR})

option(SMALL_RANGE "Whether enable small range" OFF)
message(NOTICE "SMALL_RANGE : ${SMALL_RANGE}")

SET(MYELOQ_SOURCES
	ha_eloq_macro.h
	ha_eloq.h
	ha_eloq.cc
	eloq_i_s.h
	eloq_i_s.cc
	eloq_errors.h
	eloq_errors.cpp
	eloqsql_key.h
	eloqsql_schema.h
	eloqsql_schema.cpp
	eloqsql_catalog_factory.h
	eloqsql_catalog_factory.cpp
	eloq_db.cpp
	eloq_buff.h
	eloq_key_def.h
	eloq_key_def.cc
	eloq_system_handler.h
	eloq_system_handler.cpp
	log_wrapper.h
	slice.h
	slice.cc

	# eloq_tests.hpp
)

# Add compile definitions based on options
if(WITH_DATA_STORE STREQUAL "DYNAMODB")
    set(KV_STORAGE_VAL 1 CACHE STRING "dynamodb" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_DYNAMODB)
    find_package(AWSSDK REQUIRED COMPONENTS dynamodb)
elseif(WITH_DATA_STORE STREQUAL "BIGTABLE")
    set(KV_STORAGE_VAL 2 CACHE STRING "big table" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_BIGTABLE)
    find_package(google_cloud_cpp_bigtable REQUIRED)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3")
    set(KV_STORAGE_VAL 3 CACHE STRING "eloq_ds" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
    set(KV_STORAGE_VAL 3 CACHE STRING "eloq_ds" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
    set(KV_STORAGE_VAL 3 CACHE STRING "eloq_ds" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
    option(WITH_TXSERVICE "Whether compile eloqstore with txservice" ON)
    set(ELOQSTORE_PARENT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/store_handler/eloq_data_store_service CACHE PATH "EloqStore parent directory")
    INCLUDE(${CMAKE_CURRENT_SOURCE_DIR}/store_handler/eloq_data_store_service/build_eloq_store.cmake)
else()
    message(FATAL_ERROR "Unset WITH_DATA_STORE")
endif()
add_compile_definitions(WITH_DATA_STORE=${KV_STORAGE_VAL})

if(USE_ONE_DATA_STORE_SHARD_ENABLED)
	# add preprocessor defition USE_ONE_DATA_STORE_SHARD
	add_compile_definitions(USE_ONE_DATA_STORE_SHARD)
endif()


MYSQL_ADD_PLUGIN(eloq ${MYELOQ_SOURCES}
	STORAGE_ENGINE
  	MANDATORY
  	STATIC_ONLY
	MODULE_OUTPUT_NAME ha_eloq
	LINK_LIBRARIES data_substrate ${MYELOQ_LIBRARY}
	COMPONENT eloq-engine)
