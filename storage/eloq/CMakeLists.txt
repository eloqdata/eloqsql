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

set(WITH_KV_STORAGE "CASSANDRA" CACHE STRING "Which key-value storage to use")
set_property(CACHE WITH_KV_STORAGE PROPERTY STRINGS "CASSANDRA" "DYNAMODB" "BIGTABLE")
message(NOTICE "WITH_KV_STORAGE" : ${WITH_KV_STORAGE})

set(METRICS_LIB "eloq-metrics" CACHE STRING "metrics library name.")

if(WITH_KV_STORAGE STREQUAL "CASSANDRA")
	set(KV_STORAGE_VAL 0 CACHE STRING "cassandra" FORCE)
	INCLUDE(build_cass_driver.cmake)
elseif(WITH_KV_STORAGE STREQUAL "DYNAMODB")
	set(KV_STORAGE_VAL 1 CACHE STRING "dynamodb" FORCE)
	find_package(AWSSDK REQUIRED COMPONENTS dynamodb)
elseif(WITH_KV_STORAGE STREQUAL "BIGTABLE")
	set(KV_STORAGE_VAL 2 CACHE STRING "big table" FORCE)
	find_package(google_cloud_cpp_bigtable REQUIRED)
else()
	message(FATAL_ERROR "Unset WITH_KV_STORAGE")
endif()

option(WITH_LOG_SERVICE "Compile with built-in log service." ON)

if(WITH_LOG_SERVICE)
	message("WITH_LOG_SERVICE: " ${WITH_LOG_SERVICE})
	add_compile_definitions(WITH_LOG_SERVICE)
endif()

set(STATISTICS ON CACHE BOOL "Turn on STATISTICS")

INCLUDE(build_tx_service.cmake)

if(WITH_LOG_SERVICE)
	INCLUDE(build_log_service.cmake)
endif()

INCLUDE(build_eloq_metrics.cmake)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/cc)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/remote)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/fault)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/store)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/tx-log-protos)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/eloq_metrics/include)

if(WITH_LOG_SERVICE)
	include_directories(${CMAKE_CURRENT_SOURCE_DIR}/log_service/include)
endif()

option(RANGE_PARTITION_ENABLED "Whether enable range partition" OFF)
message(NOTICE "RANGE_PARTITION_ENABLED : ${RANGE_PARTITION_ENABLED}")

option(USE_ONE_CASS_SHARD_ENABLED "Whether use one shard for kv store" ON)
message(NOTICE "USE_ONE_CASS_SHARD_ENABLED : ${USE_ONE_CASS_SHARD_ENABLED}")

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
	eloq_key.h
	eloq_schema.h
	eloq_schema.cpp
	eloq_catalog_factory.h
	eloq_catalog_factory.cpp
	eloq_db.cpp
	eloq_buff.h
	eloq_key_def.h
	eloq_key_def.cc
	eloq_system_handler.h
	eloq_system_handler.cpp
	log_wrapper.h
	slice.h
	slice.cc
	sequences.h
	sequences.cpp

	# eloq_tests.hpp
	store_handler/partition.h
	store_handler/kv_store.h
)

if(WITH_KV_STORAGE STREQUAL "CASSANDRA")
	SET(MYELOQ_SOURCES
		${MYELOQ_SOURCES}
		store_handler/cass_scanner.h
		store_handler/cass_scanner.cpp
		store_handler/cass_handler.h
		store_handler/cass_handler.cpp
		store_handler/cass_big_number.h
		store_handler/cass_big_number.cpp)
	SET(MYELOQ_LIBRARY txservice cassandra_static)
elseif(WITH_KV_STORAGE STREQUAL "DYNAMODB")
	SET(MYELOQ_SOURCES ${MYELOQ_SOURCES}
		store_handler/dynamo_handler.h
		store_handler/dynamo_handler.cpp
		store_handler/dynamo_scanner.h
		store_handler/dynamo_scanner.cpp)
	SET(MYELOQ_LIBRARY txservice ${AWSSDK_LINK_LIBRARIES})
elseif(WITH_KV_STORAGE STREQUAL "BIGTABLE")
	SET(MYELOQ_SOURCES
		${MYELOQ_SOURCES}
		store_handler/bigtable_handler.h
		store_handler/bigtable_handler.cpp
		store_handler/bigtable_scanner.h
		store_handler/bigtable_scanner.cpp)
	SET(MYELOQ_LIBRARY txservice google-cloud-cpp::bigtable)
else()
	message(FATAL_ERROR "Unset WITH_KV_STORAGE")
endif()

if(WITH_LOG_SERVICE)
	SET(MYELOQ_LIBRARY ${MYELOQ_LIBRARY} logservice)
endif()

add_compile_definitions(WITH_KV_STORAGE=${KV_STORAGE_VAL})

if(USE_ONE_CASS_SHARD_ENABLED)
	# add preprocessor defition USE_ONE_CASS_SHARD
	add_compile_definitions(USE_ONE_CASS_SHARD)
endif()

SET(MYELOQ_SOURCES ${MYELOQ_SOURCES} metrics_registry_impl.h metrics_registry_impl.cpp)
SET(MYELOQ_LIBRARY ${MYELOQ_LIBRARY} ${METRICS_LIB})

MYSQL_ADD_PLUGIN(eloq ${MYELOQ_SOURCES}
	STORAGE_ENGINE
	MODULE_ONLY
	MODULE_OUTPUT_NAME ha_eloq
	LINK_LIBRARIES ${MYELOQ_LIBRARY}
	COMPONENT eloq-engine)
