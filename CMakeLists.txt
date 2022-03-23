cmake_minimum_required(VERSION 3.16)
project(storj_emulator)

set(CMAKE_CXX_STANDARD 17)

add_executable(storj_emulator main.cpp
        storj/piece.cpp
        storj/piece.h
        storj/stripe.cpp
        storj/stripe.h
        storj/segment.cpp
        storj/segment.h
        storj/config.cpp
        storj/config.h
        storj/data_processor.cpp
        storj/data_processor.h
        storj/erasure_share.cpp
        storj/erasure_share.h
        storj/data_manager.cpp
        storj/data_manager.h
        storj/file.cpp
        storj/file.h
        storj/storage_node.cpp
        storj/storage_node.h)

find_package(SQLite3)
if (SQLITE3_FOUND)
    include_directories(${SQLite3_INCLUDE_DIRS})
    target_link_libraries(storj_emulator LINK_PUBLIC ${SQLite3_LIBRARIES})
endif (SQLITE3_FOUND)
