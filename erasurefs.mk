erasurefs_SOURCES = erasurefs.cc erasurecode.cc
erasurefs_HEADERS = erasurefs.hpp
erasurefs_LDFLAGS = -u erasurefs_reg -l isal
erasurefs_COMPILE_FLAGS = -fPIC
