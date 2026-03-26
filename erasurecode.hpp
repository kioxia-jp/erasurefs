#pragma once

#include <vector>
#include <optional>
#include <cassert>

using u8 = unsigned char;

class ErasureCode {
        const int k_, p_, m_;
        const size_t len_;
public:
        ErasureCode(int k, int p, size_t len) : k_(k), p_(p), m_(k + p), len_(len) {
                assert(k_ > 0);
                assert(p_ >= 0);
                assert(m_ > 0);
        }
        ErasureCode() = delete;
        ErasureCode(const ErasureCode& rhs) : ErasureCode(rhs.get_k(), rhs.get_p(), rhs.get_length()) {}
        ~ErasureCode() {}
        std::vector<std::vector<char>> encode(std::vector<std::vector<char>>&& buf) const;
        std::vector<std::vector<char>> decode(std::vector<std::optional<std::vector<char>>>&& buf) const;

        ErasureCode& operator = (const ErasureCode& rhs) = delete;

        std::size_t get_k() const { return static_cast<size_t>(k_); }
        std::size_t get_p() const { return static_cast<size_t>(p_); }
        std::size_t get_m() const { return static_cast<size_t>(m_); }
        size_t get_length() const { return len_; }
};
