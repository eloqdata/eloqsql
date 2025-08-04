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

set(WITH_DATA_STORE "CASSANDRA" CACHE STRING "Which key-value storage to use")
set_property(CACHE WITH_DATA_STORE PROPERTY STRINGS "CASSANDRA" "DYNAMODB" "BIGTABLE" "ELOQDSS_ROCKSDB_CLOUD_S3" "ELOQDSS_ELOQSTORE" "ELOQDSS_ROCKSDB")
message(NOTICE "WITH_DATA_STORE" : ${WITH_DATA_STORE})

set(METRICS_LIB "eloq-metrics" CACHE STRING "metrics library name.")

if(WITH_DATA_STORE STREQUAL "CASSANDRA")
	set(KV_STORAGE_VAL 0 CACHE STRING "cassandra" FORCE)
	add_compile_definitions(DATA_STORE_TYPE_CASSANDRA)
	INCLUDE(build_cass_driver.cmake)
elseif(WITH_DATA_STORE STREQUAL "DYNAMODB")
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
	add_compile_definitions(ROCKSDB_CLOUD_FS_TYPE=1)
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

if (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
  find_path(ROCKSDB_INCLUDE_PATH NAMES rocksdb/db.h)
  if (NOT ROCKSDB_INCLUDE_PATH)
    message(FATAL_ERROR "Fail to find RocksDB include path")
  endif()
  message(STATUS "ROCKSDB_INCLUDE_PATH: ${ROCKSDB_INCLUDE_PATH}")
  include_directories(${ROCKSDB_INCLUDE_PATH})

  find_library(ROCKSDB_LIB NAMES rocksdb)
  if (NOT ROCKSDB_LIB)
    message(FATAL_ERROR "Fail to find RocksDB lib path")
  endif()
  message(STATUS "ROCKSDB_LIB: ${ROCKSDB_LIB}")
  set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${ROCKSDB_LIB})
endif()

if ((WITH_DATA_STORE STREQUAL "ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ROCKSDB_CLOUD_GCS") 
          OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS"))
    if ((WITH_DATA_STORE STREQUAL "ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3"))
        find_path(AWS_CORE_INCLUDE_PATH aws/core/Aws.h)
        if((NOT AWS_CORE_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find aws/core include path")
        endif()
        message(STATUS "aws/core include path: ${AWS_CORE_INCLUDE_PATH}")

        find_library(AWS_CORE_LIB aws-cpp-sdk-core)
        if((NOT AWS_CORE_LIB ))
          message(FATAL_ERROR "Fail to find aws-cpp-sdk-core lib")
        endif()
        message(STATUS "aws-cpp-sdk-core library: ${AWS_CORE_LIB}")

        find_path(AWS_KINESIS_INCLUDE_PATH aws/kinesis/KinesisClient.h)
        if((NOT AWS_KINESIS_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find aws/kinesis include path")
        endif()
        message(STATUS "aws/kinesis include path: ${AWS_KINESIS_INCLUDE_PATH}")

        find_library(AWS_KINESIS_LIB aws-cpp-sdk-kinesis)
        if((NOT AWS_KINESIS_LIB))
          message(FATAL_ERROR "Fail to find aws-cpp-sdk-kinesis lib")
        endif()
        message(STATUS "aws-cpp-sdk-kinesis library: ${AWS_KINESIS_LIB}")

        find_path(AWS_KINESIS_INCLUDE_PATH aws/kinesis/KinesisClient.h)
        if((NOT AWS_KINESIS_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find aws/kinesis include path")
        endif()
        message(STATUS "aws/kinesis include path: ${AWS_KINESIS_INCLUDE_PATH}")

        find_library(AWS_KINESIS_LIB aws-cpp-sdk-kinesis)
        if((NOT AWS_KINESIS_LIB))
          message(FATAL_ERROR "Fail to find aws-cpp-sdk-kinesis lib")
        endif()
        message(STATUS "aws-cpp-sdk-kinesis library: ${AWS_KINESIS_LIB}")

        find_path(AWS_S3_INCLUDE_PATH aws/s3/S3Client.h)
        if((NOT AWS_S3_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find aws/s3 include path")
        endif()
        message(STATUS "aws/s3 include path: ${AWS_S3_INCLUDE_PATH}")

        find_library(AWS_S3_LIB aws-cpp-sdk-s3)
        if((NOT AWS_S3_LIB ))
          message(FATAL_ERROR "Fail to find aws-cpp-sdk-s3 lib")
        endif()
        message(STATUS "aws-cpp-sdk-s3 library: ${AWS_S3_LIB}")

        set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${AWS_CORE_INCLUDE_PATH})
        set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${AWS_KINESIS_INCLUDE_PATH})
        set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${AWS_S3_INCLUDE_PATH})

        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${AWS_CORE_LIB})
        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${AWS_KINESIS_LIB})
        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${AWS_S3_LIB})

        find_library(ROCKSDB_CLOUD_LIB NAMES rocksdb-cloud-aws)
    elseif ((WITH_DATA_STORE STREQUAL "ROCKSDB_CLOUD_GCS") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS"))
        find_path(GCP_CS_INCLUDE_PATH google/cloud/storage/client.h)
        if((NOT GCP_CS_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find google/cloud/storage include path")
        endif()
        message(STATUS "google/cloud/storage include path: ${GCP_CS_INCLUDE_PATH}")

        find_library(GCP_COMMON_LIB google_cloud_cpp_common)
        if((NOT GCP_COMMON_LIB))
          message(FATAL_ERROR "Fail to find google_cloud_cpp_common lib")
        endif()
        message(STATUS "google_cloud_cpp_common library: ${GCP_COMMON_LIB}")

        find_library(GCP_CS_LIB google_cloud_cpp_storage)
        if((NOT GCP_CS_LIB))
          message(FATAL_ERROR "Fail to find google_cloud_cpp_storage lib")
        endif()
        message(STATUS "google_cloud_cpp_storage library: ${GCP_CS_LIB}")

        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${GCP_COMMON_LIB})
        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${GCP_CS_LIB})

        find_library(ROCKSDB_CLOUD_LIB NAMES rocksdb-cloud-gcp)
    endif ()

    # Common RocksDB Cloud setup (outside the S3/GCS specific blocks)
    find_path(ROCKSDB_CLOUD_INCLUDE_PATH NAMES rocksdb/db.h PATH_SUFFIXES "rocksdb_cloud_header")
    if (NOT ROCKSDB_CLOUD_INCLUDE_PATH)
      message(FATAL_ERROR "Fail to find RocksDB Cloud include path")
    endif ()
    message(STATUS "ROCKSDB_CLOUD_INCLUDE_PATH: ${ROCKSDB_CLOUD_INCLUDE_PATH}")
    set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${ROCKSDB_CLOUD_INCLUDE_PATH})
    include_directories(${ROCKSDB_INCLUDE_PATH})

    if (NOT ROCKSDB_CLOUD_LIB)
      message(FATAL_ERROR "Fail to find RocksDB Cloud lib path")
    endif ()
    message(STATUS "ROCKSDB_CLOUD_LIB: ${ROCKSDB_CLOUD_LIB}")
    set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${ROCKSDB_CLOUD_LIB})
endif()

if ((WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE"))
  # compile proto file
  set(DS_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/store_handler/eloq_data_store_service)
  message(NOTICE "data store service proto dir: ${DS_PROTO_DIR}")
  set(PROTO_SRC ${DS_PROTO_DIR})
  set(PROTO_NAME ds_request)
  execute_process(
    COMMAND protoc ./${PROTO_NAME}.proto --cpp_out=./
    WORKING_DIRECTORY ${PROTO_SRC}
  )
endif()


option(WITH_LOG_SERVICE "Compile with built-in log service." ON)

if(WITH_LOG_SERVICE)
	message("WITH_LOG_SERVICE: " ${WITH_LOG_SERVICE})
	add_compile_definitions(WITH_LOG_SERVICE)
endif()

option(OPEN_LOG_SERVICE "Compile with open log service." ON)
message("OPEN_LOG_SERVICE: " ${OPEN_LOG_SERVICE})
if (OPEN_LOG_SERVICE)
	add_compile_definitions(OPEN_LOG_SERVICE=1)
endif()

set(STATISTICS ON CACHE BOOL "Turn on STATISTICS")

INCLUDE(build_tx_service.cmake)

if(WITH_LOG_SERVICE)
  if (OPEN_LOG_SERVICE)
    INCLUDE(build_log_service.cmake)
  else()
    INCLUDE(build_eloq_log_service.cmake)
  endif()
endif()

INCLUDE(build_eloq_metrics.cmake)

find_package(MIMALLOC REQUIRED)
include_directories(${MIMALLOC_INCLUDE_DIR})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/cc)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/remote)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/fault)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/store)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/tx-log-protos)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/eloq_metrics/include)

if(WITH_LOG_SERVICE)
    if (OPEN_LOG_SERVICE)
	    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/log_service/include)
    else ()
        include_directories(${CMAKE_CURRENT_SOURCE_DIR}/eloq_log_service/include)
    endif ()
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

	# eloq_tests.hpp
	store_handler/partition.h
	store_handler/kv_store.h
)

if(WITH_DATA_STORE STREQUAL "CASSANDRA")
	SET(MYELOQ_SOURCES
		${MYELOQ_SOURCES}
		store_handler/cass_scanner.h
		store_handler/cass_scanner.cpp
		store_handler/cass_handler.h
		store_handler/cass_handler.cpp
		store_handler/cass_big_number.h
		store_handler/cass_big_number.cpp
		)
	SET(MYELOQ_LIBRARY txservice cassandra_static)
elseif(WITH_DATA_STORE STREQUAL "DYNAMODB")
	SET(MYELOQ_SOURCES ${MYELOQ_SOURCES}
		store_handler/dynamo_handler.h
		store_handler/dynamo_handler.cpp
		store_handler/dynamo_scanner.h
		store_handler/dynamo_scanner.cpp)
	SET(MYELOQ_LIBRARY txservice ${AWSSDK_LINK_LIBRARIES})
elseif(WITH_DATA_STORE STREQUAL "BIGTABLE")
	SET(MYELOQ_SOURCES
		${MYELOQ_SOURCES}
		store_handler/bigtable_handler.h
		store_handler/bigtable_handler.cpp
		store_handler/bigtable_scanner.h
		store_handler/bigtable_scanner.cpp)
	SET(MYELOQ_LIBRARY txservice google-cloud-cpp::bigtable)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3" OR WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
	SET(MYELOQ_SOURCES
		${MYELOQ_SOURCES}    
    store_handler/data_store_service_client.h        
		store_handler/data_store_service_client.cpp
    store_handler/data_store_service_client_closure.h        
		store_handler/data_store_service_client_closure.cpp
    store_handler/data_store_service_scanner.h        
		store_handler/data_store_service_scanner.cpp
    store_handler/store_util.h        
		store_handler/store_util.cpp

		store_handler/eloq_data_store_service/ini.h
		store_handler/eloq_data_store_service/ini.c
		store_handler/eloq_data_store_service/INIReader.h
		store_handler/eloq_data_store_service/INIReader.cpp
		store_handler/eloq_data_store_service/thread_worker_pool.h
		store_handler/eloq_data_store_service/thread_worker_pool.cpp
		store_handler/eloq_data_store_service/data_store_service.h
		store_handler/eloq_data_store_service/data_store_service.cpp
		store_handler/eloq_data_store_service/data_store_fault_inject.h
		store_handler/eloq_data_store_service/data_store_fault_inject.cpp
		store_handler/eloq_data_store_service/data_store_service_config.h
		store_handler/eloq_data_store_service/data_store_service_config.cpp
		store_handler/eloq_data_store_service/ds_request.pb.h
		store_handler/eloq_data_store_service/ds_request.pb.cc
		store_handler/eloq_data_store_service/rocksdb_config.h
		store_handler/eloq_data_store_service/rocksdb_config.cpp
                store_handler/eloq_data_store_service/rocksdb_data_store_common.h
                store_handler/eloq_data_store_service/rocksdb_data_store_common.cpp
	)

  if (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3")
    SET(MYELOQ_SOURCES
      ${MYELOQ_SOURCES}
		  store_handler/eloq_data_store_service/rocksdb_cloud_data_store.h
		  store_handler/eloq_data_store_service/rocksdb_cloud_data_store.cpp
    )
  elseif (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
    SET(MYELOQ_SOURCES
      ${MYELOQ_SOURCES}
		  store_handler/eloq_data_store_service/rocksdb_data_store.h
		  store_handler/eloq_data_store_service/rocksdb_data_store.cpp
    )
  endif()
	SET(MYELOQ_LIBRARY ${MYELOQ_LIBRARY} txservice ${ROCKSDB_LIBRARIES})
	
	include_directories(${CMAKE_CURRENT_SOURCE_DIR}/store_handler/eloq_data_store_service)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
        SET(MYELOQ_SOURCES
            ${MYELOQ_SOURCES}
            store_handler/data_store_service_client.cpp
            store_handler/data_store_service_client_closure.cpp
            store_handler/data_store_service_scanner.cpp
            store_handler/store_util.cpp
            store_handler/eloq_data_store_service/thread_worker_pool.cpp
            store_handler/eloq_data_store_service/data_store_service.cpp
            store_handler/eloq_data_store_service/data_store_fault_inject.cpp
            store_handler/eloq_data_store_service/data_store_service_config.cpp
            store_handler/eloq_data_store_service/eloq_store_data_store.cpp
            store_handler/eloq_data_store_service/ds_request.pb.cc
        )
        SET(MYELOQ_LIBRARY ${MYELOQ_LIBRARY} txservice eloqstore)

	include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/store_handler
        ${CMAKE_CURRENT_SOURCE_DIR}/store_handler/eloq_data_store_service)
else()
	message(FATAL_ERROR "Unset WITH_DATA_STORE")
endif()

if(WITH_LOG_SERVICE)
	SET(MYELOQ_LIBRARY ${MYELOQ_LIBRARY} logservice)
endif()

# add_compile_definitions(WITH_DATA_STORE=${KV_STORAGE_VAL})

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
