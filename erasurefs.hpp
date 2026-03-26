#pragma once

#include "rocksdb/convenience.h"
#include "env/composite_env_wrapper.h"
#include "env/io_posix.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "port/lang.h"
#include "port/port.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/compression_context_cache.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/thread_local.h"
#include "util/threadpool_imp.h"

#include "erasurecode.hpp"

#include <unistd.h>

#include <iostream>
#include <csignal>
#include <ranges>
#include <string>
#include <vector>
#include <numeric>
#include <utility>
#include <set>
#include <optional>

#include <sys/types.h>

static constexpr std::size_t fragment_length = 1024*1024*256;

static inline
std::string make_path(const std::string& prefix, const std::string& path) {
        return prefix + '/' + path;
}

static inline
std::string make_path_offset(const std::string& prefix, const std::string& path, off_t offset, size_t p = 0/*for parity fragments index*/) {
        std::size_t frag_n = offset / fragment_length;
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%06ld%02ld", frag_n, p);
        return prefix + '/' + path + "_" + buf;
}

static inline
bool is_parity_file(const std::string& path) {
        if (path.size() >= 9 and
            path[path.size() - 9] == '_' and
            isdigit(path[path.size() - 8]) and
            isdigit(path[path.size() - 7]) and
            isdigit(path[path.size() - 6]) and
            isdigit(path[path.size() - 5]) and
            isdigit(path[path.size() - 4]) and
            isdigit(path[path.size() - 3]) and
            isdigit(path[path.size() - 2]) and
            isdigit(path[path.size() - 1])) {
                return (path[path.size() - 2] != '0' or
                        (path[path.size() - 1]) != '0');
        }
        return false;
}

static inline
std::optional<std::string> remove_path_offset(const std::string& path) {
        if (path.size() >= 9 and
            path[path.size() - 9] == '_' and
            isdigit(path[path.size() - 8]) and
            isdigit(path[path.size() - 7]) and
            isdigit(path[path.size() - 6]) and
            isdigit(path[path.size() - 5]) and
            isdigit(path[path.size() - 4]) and
            isdigit(path[path.size() - 3]) and
            isdigit(path[path.size() - 2]) and
            isdigit(path[path.size() - 1])) {
                return std::make_optional(path.substr(0, path.size() - 9));
        }
        else {
                return std::nullopt;
        }
}

static inline
std::vector<std::string> split(const std::string& s, char delim) {
        std::vector<std::string> elems;
        std::string item;
        for (char c: s) {
                if (c == delim) {
                        if (not item.empty()) {
                                elems.push_back(item);
                        }
                        item.clear();
                }
                else {
                        item += c;
                }
        }
        if (not item.empty()) {
                elems.push_back(item);
        }
        return elems;
}

static inline
std::size_t stripe_index(const std::string& path, std::size_t m) {
        size_t h = std::hash<std::string>()(path);
        return h % m;
}

static inline
std::size_t logical_fragment_offset(off_t off) {
        return off / fragment_length;
}

/*
  logical offset => file offset
  physical offset => file offset + parity fragment
  
 */

static inline
size_t hash_path(const std::string& path) {
        const auto& splits = split(path, '/');
        assert(not splits.empty());
        return std::hash<std::string>()(splits.back());

        /*
        size_t ret = 0;
        for (const auto& p: split(path, '/')) {
                if (p.empty()) {
                        continue;
                }
                ret |= std::hash<std::string>()(p);
        }
        return ret;
        */
}

/* where to place the offset */
static inline
std::size_t physical_fragment_index(const std::string& path, std::size_t m, size_t k, off_t off) {
        off_t logical_off_index = off / fragment_length;
        size_t physical_fragmented_offset = (logical_off_index / k) * m + (logical_off_index % k);
        //size_t h = std::hash<std::string>()(path);
        size_t h = hash_path(path);
        return ((h % m) + physical_fragmented_offset) % m;
        //(i / k) * m + (i % k)
}



