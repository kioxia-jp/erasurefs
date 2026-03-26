#include "erasurecode.hpp"
#include <isa-l.h>

#include <vector>
#include <optional>
#include <iostream>
#include <random>
#include <algorithm>

#include <cassert>

// C++ impl of gf_gen_decode_matrix_simple in isa-l/examples/ec/ec_simple_example.c
static inline int
gen_decode_matrix(const std::vector<u8>& encode_matrix,
                  std::vector<u8>& decode_matrix,
                  std::vector<u8>& decode_index,
                  const std::vector<u8>& frag_err_list, int k, int n) {

        std::vector<bool> is_missing(n, false);

        for (int err_idx: frag_err_list) {
                assert(err_idx < n);
                assert(err_idx >= 0);
                is_missing[err_idx] = true;
        }

        std::vector<u8> temp_matrix(k * k, 0);
        std::vector<u8> invert_matrix(k * k);

        for (int i = 0, r = 0; i < k; i++, r++) {
                while (is_missing[r]) {
                        r++;
                }
                for (int j = 0; j < k; j++) {
                        temp_matrix[k * i + j] = encode_matrix[k * r + j];
                }
                decode_index[i] = r;
        }

        int r = gf_invert_matrix(temp_matrix.data(), invert_matrix.data(), k);
        if (r < 0) {
                std::cerr << "gf_invert_matrix failed" << std::endl;
                return -1;
        }


        for (size_t i = 0; i < frag_err_list.size(); i++) {
                int err_idx = frag_err_list[i];
                if (err_idx < k) {
                        const u8* inv_row = &invert_matrix[err_idx * k];
                        std::copy(inv_row, inv_row + k, &decode_matrix[i * k]);
                }
                else {
                        std::fill(&decode_matrix[i * k], &decode_matrix[i * k] + k, 0);
                }
        }

        return 0;
}

std::vector<std::vector<char>> ErasureCode::encode(std::vector<std::vector<char>>&& buf) const {
        assert(buf.size() == static_cast<size_t>(k_));
        std::vector<u8*> frags;
        for (auto& b: buf) {
                assert(b.size() == len_);
                frags.push_back(reinterpret_cast<u8*>(b.data()));
        }
        assert(frags.size() == buf.size());

        for (int i = 0; i < p_; i++) {
                buf.emplace_back(len_);
                frags.push_back(reinterpret_cast<u8*>(buf.back().data()));
        }

        assert(frags.size() == buf.size());
        assert(frags.size() == static_cast<size_t>(m_));

        std::vector<u8> encode_matrix(m_ * k_);
        gf_gen_cauchy1_matrix(&encode_matrix[0], m_, k_);

        std::vector<u8> g_tbls(k_ * p_ * 32);
        ec_init_tables(k_, p_, &encode_matrix[k_ * k_], &g_tbls[0]);
                
        ec_encode_data(len_, k_, p_, &g_tbls[0], &frags[0], &frags[k_]);

        return buf;
}

std::vector<std::vector<char>> ErasureCode::decode(std::vector<std::optional<std::vector<char>>>&& buf) const {
        assert(buf.size() == static_cast<size_t>(m_));
        std::vector<u8> decode_matrix(m_ * k_);
        std::vector<u8> decode_index(k_);

        std::vector<u8> frag_err_list;
        for (int i = 0; i < static_cast<int>(buf.size()); i++) {
                if (not buf[i].has_value()) {
                        frag_err_list.push_back(i);
                }
        }

        std::vector<u8> encode_matrix(m_ * k_);
        gf_gen_cauchy1_matrix(&encode_matrix[0], m_, k_);

        int r = gen_decode_matrix(encode_matrix, decode_matrix, 
                                  decode_index, frag_err_list, k_, m_);
        if (r != 0) {
                throw r;
        }

        std::vector<u8*> frags;
        for (auto& b: buf) {
                if (b.has_value()) {
                        frags.push_back(reinterpret_cast<u8*>(b.value().data()));
                }
                else {
                        frags.push_back(nullptr);
                }
        }
        assert(frags.size() == static_cast<size_t>(m_));

        std::vector<u8*> recover_srcs(k_);
        for (int i = 0; i < k_; i++) {
                recover_srcs.at(i) = frags.at(static_cast<size_t>(decode_index.at(i)));
        }
                
        std::vector<u8> g_tbls(k_ * p_ * 32);
        ec_init_tables(k_, frag_err_list.size(), &decode_matrix[0], &g_tbls[0]);

        std::vector<std::vector<char>> recover_out;
        std::vector<u8*> recover_outp;

        for (int i = 0; i < p_; i++) {
                recover_out.emplace_back(len_);
                recover_outp.push_back(reinterpret_cast<u8*>(recover_out.back().data()));
        }
        assert(recover_out.size() == static_cast<size_t>(p_));
        assert(recover_outp.size() == static_cast<size_t>(p_));

        ec_encode_data(len_, k_, frag_err_list.size(), &g_tbls[0], &recover_srcs[0], &recover_outp[0]);

        std::vector<std::vector<char>> ret;
        int i = 0;
        for (auto& b: buf) {
                if (b.has_value()) {
                        ret.push_back(std::move(b.value()));
                }
                else {
                        ret.push_back(std::move(recover_out.at(i)));
                        i++;
                }
        }
        assert(static_cast<size_t>(i) == recover_out.size());
        assert(buf.size() == ret.size());
        return ret;
}

#ifdef BUILD_STANDALONE
static inline std::vector<int>
gen_ran(size_t m, int n) {
        std::random_device rnd;
        std::mt19937 mt(rnd());
        
        std::vector<int> v(m);
        std::iota(v.begin(), v.end(), 0);
        std::shuffle(v.begin(), v.end(), mt);
        v.erase(v.begin() + n, v.end());
        return v;
}

int main() {
        size_t len = 4096;
        int k = 4;
        int p = 2;
        ErasureCode c(k, p, len);

        std::vector<std::vector<char>> buf;
        for (int i = 0; i < k; i++) {
                std::vector<char> b(len, 0);
                snprintf(b.data(), b.size(), "%dx%dx%dx%dx%d", i, i, i, i, i);
                buf.push_back(b);
        }

        auto encoded_buf = c.encode(std::move(buf));
        assert(encoded_buf.size() == static_cast<size_t>(k + p));
        
        std::vector<std::optional<std::vector<char>>> opt_buf;
        for (auto&& b: encoded_buf) {
                opt_buf.push_back(std::make_optional(std::move(b)));
        }

        // erase some fragments
        auto rs = gen_ran(k + p, p);
        std::sort(rs.begin(), rs.end());
        for (auto r: rs) {
                std::cout << "remove " << r << std::endl;
                assert(opt_buf.at(r).has_value());
                opt_buf.at(r) = std::nullopt;
                assert(not opt_buf.at(r).has_value());
        }
        
        ErasureCode c2(k, p, len);
        auto d = c2.decode(std::move(opt_buf));

        for (int i = 0; i < k; i++) {
                printf("%s ", d.at(i).data());
        }
        printf("\n");
}
#endif
