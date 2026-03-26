cmake_minimum_required(VERSION 3.4)

set(erasurefs_SOURCES "erasurefs.cc" "stripefs.cc" PARENT_SCOPE)
set(erasurefs_HEADERS "erasurefs.hpp" PARENT_SCOPE)
set(erasurefs_CMAKE_EXE_LINKER_FLAGS "-u mirrorfs_reg -u stripefs_reg" PARENT_SCOPE
