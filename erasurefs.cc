//  Copyright (c) 2024, Kioxia corporation.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "erasurefs.hpp"

namespace ROCKSDB_NAMESPACE {
        
        class ErasureFsFileLock : public FileLock {
                std::shared_ptr<FileSystem> fs_;
                const ErasureCode ec_;
                const std::string path_;
                FileLock* lock_;

        public:
                ErasureFsFileLock(const std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>>& prefixes,
                                  const ErasureCode& ec,
                                  const std::string& path,
                                  const IOOptions& options, IODebugContext* dbg) : ec_(ec), path_(path), lock_(nullptr) {
                        std::size_t frag_index = physical_fragment_index(path, ec_.get_m(), ec_.get_k(), 0);
                        fs_ = prefixes.at(frag_index).first;
                        const std::string& prefix = prefixes.at(frag_index).second;
                        auto ret = fs_->LockFile(make_path_offset(prefix, path, 0), options, &lock_, dbg);
                        if (not ret.ok()) {
                                std::cout << fs_->Name() << " lock file failed " << ret.ToString() << std::endl;
                                throw ret;
                        }
                        assert(lock_ != nullptr);
                }
                virtual ~ErasureFsFileLock() {
                        if (lock_ != nullptr) {
                                fs_->UnlockFile(lock_, IOOptions(), nullptr);
                        }
                        // std::cout << __FUNCTION__ << " " << path_ << std::endl;
                }
                IOStatus UnlockFile(const IOOptions& options, IODebugContext* dbg) {
                        assert(lock_ != nullptr);
                        auto ret = fs_->UnlockFile(lock_, options, dbg);
                        lock_ = nullptr;
                        return ret;
                }
        };
        
        class ErasureFsSequentialFile : public FSSequentialFile {
                std::unique_ptr<FSSequentialFile> tail_fragment_;
                std::mutex mtx_;
                const std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>> prefixes_;
                const ErasureCode ec_;                
                const std::string path_;
                const FileOptions options_;
                uint64_t cur_;
                uint64_t tail_fragment_offset_;
                
                void open_current_fragment() {
                        std::size_t frag_off = logical_fragment_offset(cur_);
                        std::size_t frag_index = physical_fragment_index(path_, ec_.get_m(), ec_.get_k(), cur_);
                        const auto& fs = prefixes_.at(frag_index).first;
                        const auto& prefix = prefixes_.at(frag_index).second;
                        std::unique_ptr<FSSequentialFile> fragment;
                        // std::cout << "Seq create fragment " << path_ << " at " << prefix << " frag_off " << frag_off << std::endl;
                        auto ret = fs->NewSequentialFile(make_path_offset(prefix, path_, cur_), options_, &fragment, nullptr);
                        if (not ret.ok()) {
                                std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                throw ret;
                        }
                        tail_fragment_ = std::move(fragment);
                        tail_fragment_offset_ = frag_off;
                }

        public:
                ErasureFsSequentialFile(const std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>>& prefixes,
                                        const ErasureCode& ec,
                                        const std::string& path,
                                        const FileOptions& options, IODebugContext* /*dbg*/) : prefixes_(prefixes), ec_(ec),
                                                                                               path_(path), options_(options), cur_(0), tail_fragment_offset_(~0ULL) {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;
                        open_current_fragment();
                }
                virtual ~ErasureFsSequentialFile() {
                        // std::cout << __FUNCTION__ << " " << path_ << std::endl;
                }
                IOStatus Read(size_t n, const IOOptions& options, Slice* result, char* scratch,
                              IODebugContext* dbg) override {
                        std::lock_guard<std::mutex> lock_guard(mtx_);

                        if (n == 0) {
                                *result = Slice(scratch, 0);
                                return IOStatus::OK();
                        }
                        assert(logical_fragment_offset(cur_) == tail_fragment_offset_);                        
                        // std::cout << __FUNCTION__ << " seq " << path_ << " n "  << n << " cur "<< cur_ << std::endl;
                        /*
                                           cur<---------------->n
                                            v                   v
                             |---------|---------|---------|--------|
                                            |head|   body  |tail|
                         */

                        size_t head_size = fragment_length - (cur_ % fragment_length);
                        size_t tail_size = (cur_ + n) % fragment_length;
                        size_t rem_size = n;
                        size_t total_read = 0;
                        
                        // read n only
                        if (head_size > n) {
                                assert(head_size <= fragment_length);
                                assert(n <= fragment_length);
                                auto ret = tail_fragment_->Read(n, options, result, scratch, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " "<< path_ << " fail " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                assert(result->data() == scratch);
                                assert(result->size() <= n);
                                cur_ += result->size();
                                rem_size -= result->size();
                                total_read += result->size();
                                assert(rem_size + total_read == n);

                                // std::cout << __FUNCTION__ << " " << path_ << " success " << result->size() << std::endl;
                                
                                return IOStatus::OK();
                        }


                        // read head_size firstly
                        if (head_size <= n) {
                                assert(head_size <= fragment_length);
                                assert(total_read == 0);
                                Slice s;
                                auto ret = tail_fragment_->Read(head_size, options, &s, scratch + total_read, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " "<< path_ << " fail " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                assert(s.data() == scratch + total_read);
                                assert(s.size() <= head_size);
                                cur_ += s.size();
                                rem_size -= s.size();
                                total_read += s.size();

                                
                                if (s.size() < head_size) {
                                        goto End;
                                }

                                open_current_fragment();
                                assert(logical_fragment_offset(cur_) == tail_fragment_offset_);
                        }

                        while (rem_size >= fragment_length) {
                                assert((cur_ % fragment_length) == 0);
                                Slice s;
                                auto ret = tail_fragment_->Read(fragment_length, options, &s, scratch + total_read, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " "<< path_ << " fail " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                assert(s.data() == scratch + total_read);
                                assert(s.size() <= fragment_length);
                                cur_ += s.size();
                                rem_size -= s.size();
                                total_read += s.size();

                                if (s.size() < fragment_length) {
                                        goto End;
                                }

                                open_current_fragment();
                                assert(logical_fragment_offset(cur_) == tail_fragment_offset_);
                        }

                        assert(rem_size == tail_size);
                        if (tail_size > 0) {
                                assert(tail_size < fragment_length);
                                assert((cur_ % fragment_length) == 0);
                                Slice s;
                                auto ret = tail_fragment_->Read(tail_size, options, &s, scratch + total_read, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " "<< path_ << " fail " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                assert(s.data() == scratch + total_read);
                                assert(s.size() <= tail_size);
                                cur_ += s.size();
                                rem_size -= s.size();
                                total_read += s.size();
                        }
                End:

                        assert(logical_fragment_offset(cur_) == tail_fragment_offset_);
                        assert(rem_size + total_read == n);
                        *result = Slice(scratch, total_read);

                        // std::cout << __FUNCTION__ << " seq " << path_ << " total_read "  << total_read << std::endl;
                        
                        return IOStatus::OK();
                }
                IOStatus Skip(uint64_t n) override {
                        std::lock_guard<std::mutex> lock_guard(mtx_);
                        
                        // std::cout << __FUNCTION__ << " old cur " << cur_ << " n " << n << " new cur " << (cur_ + n) << std::endl;
                        if (logical_fragment_offset(cur_) < logical_fragment_offset(cur_ + n)) {
                                cur_ += n;                                
                                open_current_fragment();
                                return tail_fragment_->Skip(cur_ % fragment_length);
                        }
                        else {
                                cur_ += n;
                                return tail_fragment_->Skip(n);
                        }
                }
        };

        class ErasureFsRandomAccessFile : public FSRandomAccessFile {
                const std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>> prefixes_;
                const ErasureCode ec_;
                const std::string path_;
                const FileOptions options_;
                const bool use_direct_io_;
                mutable std::map<std::pair<uint64_t, size_t>, char*> pool_;
                mutable std::mutex mtx_;
                
                std::unique_ptr<FSRandomAccessFile> get_fragment(size_t fragment_offset) const {
                        // std::cout << __FUNCTION__ << " " << path_ << " " << fragment_offset << std::endl;
                        
                        const auto off = fragment_offset * fragment_length;
                        const std::size_t frag_index = physical_fragment_index(path_, ec_.get_m(), ec_.get_k(), off);
                        const auto& fs = prefixes_.at(frag_index).first;
                        const auto& prefix = prefixes_.at(frag_index).second;
                        std::unique_ptr<FSRandomAccessFile> fragment;
                        auto ret = fs->NewRandomAccessFile(make_path_offset(prefix, path_, off), options_, &fragment, nullptr);
                        if (not ret.ok()) {
                                std::cout << __FUNCTION__ << " rnd XXXXXXX " << fs->Name() << " " << path_ << " " << ret.ToString() << std::endl;
                                throw ret;
                        }
                        
                        return fragment;
                }
                
        public:
                ErasureFsRandomAccessFile(const std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>>& prefixes,
                                          const ErasureCode& ec,
                                          const std::string& path, 
                                          const FileOptions& options, IODebugContext* /*dbg*/) : prefixes_(prefixes), ec_(ec), path_(path),
                                                                                                 options_(options), use_direct_io_ (not options_.use_mmap_reads) {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;                        
                        assert(not prefixes.empty());
                }
                virtual ~ErasureFsRandomAccessFile() {
                        // std::cout << __FUNCTION__ << " " << path_ << std::endl;
                        for (const auto& p: pool_) {
                                free(p.second);
                        }
                }

                IOStatus Read(uint64_t offset, size_t n, const IOOptions& options, Slice* result,
                              char* scratch, IODebugContext* dbg) const override {
                        char* mmap_scratch = nullptr;
                        std::lock_guard<std::mutex> lock_guard(mtx_);
                        if (options_.use_mmap_reads) {
                                //std::lock_guard<std::mutex> lock_guard(mtx_);
                                auto m = pool_.find(std::make_pair(offset, n));
                                if (m == pool_.end()) {
                                        mmap_scratch = static_cast<char*>(malloc(n));
                                        if (mmap_scratch == nullptr) {
                                                return IOStatus::IOError("buffer malloc fail");
                                        }
                                        assert(mmap_scratch != nullptr);
                                        pool_.emplace(std::make_pair(offset, n), mmap_scratch);
                                }
                                else {
                                        mmap_scratch = m->second;
                                }
                                assert(mmap_scratch != nullptr);
                        }

                        
                        /*
                                         offset<--------------->n
                                            v                   v
                             |---------|---------|---------|--------|
                                            |head|   body  |tail|
                         */

                        size_t head_size = fragment_length - (offset % fragment_length);
                        size_t tail_size = (offset + n) % fragment_length;
                        size_t rem_size = n;
                        size_t total_read = 0;

                        // std::cout << "RND " <<__FUNCTION__ << " " << path_ << //" tid " << std::this_thread::get_id() << " this " << this <<
                        //         " off " << offset << " fragoff " << offset / fragment_length << " n " << n  <<
                        //         " head " << head_size << " tail " << tail_size << std::endl;

                        size_t cur = offset;
                        
                        // read n only
                        if (head_size >= n) {
                                try {
                                        assert(head_size <= fragment_length);
                                        std::size_t frag_off = logical_fragment_offset(offset);
                                        auto fragment = get_fragment(frag_off);
                                        Slice s;
                                        auto ret = fragment->Read(offset % fragment_length, n, options, &s, scratch, dbg);
                                        if (not ret.ok()) {
                                                std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                                return ret;
                                        }
                                        if (options_.use_mmap_reads) {
                                                memcpy(mmap_scratch + total_read, s.data(), s.size());
                                        }
                                        assert(s.size() <= n);
                                        rem_size -= s.size();
                                        total_read += s.size();
                                        assert(rem_size + total_read == n);
                                }
                                catch (...) {
                                        *result = Slice(scratch, 0);
                                        return IOStatus::OK();
                                }
                                goto End;
                        }


                        // read head_size firstly
                        if (head_size < n) {
                                try {
                                        std::size_t frag_off = logical_fragment_offset(cur);
                                        auto fragment = get_fragment(frag_off);
                                        assert(total_read == 0);
                                        Slice s(scratch + total_read, head_size);
                                        auto ret = fragment->Read(cur % fragment_length, head_size, options, &s, scratch + total_read, dbg);
                                        if (not ret.ok()) {
                                                std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                                return ret;
                                        }
                                        if (options_.use_mmap_reads) {
                                                memcpy(mmap_scratch + total_read, s.data(), s.size());
                                        }
                                        
                                        assert(s.size() <= head_size);
                                        cur += s.size();
                                        rem_size -= s.size();
                                        total_read += s.size();
                                        
                                        if (s.size() < head_size) {
                                                goto End;
                                        }
                                }
                                catch (...) {
                                        *result = Slice(scratch, 0);
                                        return IOStatus::OK();
                                }
                        }

                        while (rem_size >= fragment_length) {
                                assert((cur % fragment_length) == 0);
                                std::size_t frag_off = logical_fragment_offset(cur);
                                auto fragment = get_fragment(frag_off);
                                Slice s(scratch + total_read, fragment_length);
                                auto ret = fragment->Read(cur % fragment_length, fragment_length, options, &s, scratch + total_read, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                if (options_.use_mmap_reads) {
                                        memcpy(mmap_scratch + total_read, s.data(), s.size());
                                }

                                assert(s.size() <= fragment_length);
                                cur += s.size();
                                rem_size -= s.size();
                                total_read += s.size();

                                if (s.size() < fragment_length) {
                                        goto End;
                                }
                        }

                        assert(rem_size == tail_size);
                        if (tail_size > 0) {
                                assert((cur % fragment_length) == 0);
                                std::size_t frag_off = logical_fragment_offset(cur);
                                auto fragment = get_fragment(frag_off);
                                Slice s(scratch + total_read, tail_size);
                                auto ret = fragment->Read(cur % fragment_length, tail_size, options, &s, scratch + total_read, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                if (options_.use_mmap_reads) {
                                        memcpy(mmap_scratch + total_read, s.data(), s.size());
                                }

                                assert(s.size() <= tail_size);
                                cur += s.size();
                                rem_size -= s.size();
                                total_read += s.size();
                        }
                End:
                        
                        assert(rem_size + total_read == n);
                        if (not options_.use_mmap_reads) {
                                *result = Slice(scratch, total_read);
                                assert(mmap_scratch == nullptr);
                        }
                        else {
                                assert(mmap_scratch != nullptr);
                                *result = Slice(mmap_scratch, total_read);
                        }
                        return IOStatus::OK();
                }
                
                bool use_direct_io() const override {
                        return use_direct_io_;
                }
                size_t GetRequiredBufferAlignment() const override {
                        return kDefaultPageSize;
                }
        };


      
        class ErasureFsWritableFile : public FSWritableFile {
                std::mutex mtx_;
                const std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>> prefixes_;
                const ErasureCode ec_;
                const std::string path_;
                const FileOptions options_;
                uint64_t cur_;
                uint64_t tail_fragment_offset_;
                std::unique_ptr<FSWritableFile> tail_fragment_;
                Env::WriteLifeTimeHint hint_;

                void make_parity() {
                        // std::cout << __FUNCTION__ << " " << path_ << std::endl;
                        std::vector<std::vector<char>> bufs;
                        //assert(ec_.get_k() == tail_fragments_.size());
                        assert((cur_ % (ec_.get_k() * fragment_length)) == 0);
                        
                        //tail_fragments_.clear();
                        
                        assert(cur_ >= fragment_length * ec_.get_k());
                        for (auto c = cur_ - fragment_length * ec_.get_k(); c < cur_; c += fragment_length) {
                                std::size_t frag_index = physical_fragment_index(path_, ec_.get_m(), ec_.get_k(), c);
                                const auto& fs = prefixes_.at(frag_index).first;
                                const auto& prefix = prefixes_.at(frag_index).second;
                                std::unique_ptr<FSSequentialFile> fragment;
                                auto ret = fs->NewSequentialFile(make_path_offset(prefix, path_, c), FileOptions(), &fragment, nullptr);
                                if (not ret.ok()) {
                                        std::cout << "Fail: NewSequentialFile " << fs->Name() << " " << make_path_offset(prefix, path_, c) << " " << ret.ToString() << std::endl;
                                        throw ret;
                                }
                                std::vector<char> buf(fragment_length);
                                Slice s;
                                IOOptions iops;
                                ret = fragment->Read(fragment_length, iops, &s, buf.data(), nullptr);
                                if (not ret.ok()) {
                                        throw ret;
                                }
                                assert(s.size() <= fragment_length);
                                if (s.size() < fragment_length) {
                                        uint64_t fsize;
                                        ret = fs->GetFileSize(make_path_offset(prefix, path_, c), IOOptions(), &fsize, nullptr);
                                        assert(ret.ok());
                                        std::cout << "make parity fail: " << make_path_offset(prefix, path_, c) << " read size " << s.size() << " cur " << cur_ << " fsize " << fsize << std::endl;
                                }
                                assert(s.size() == fragment_length);
                                bufs.push_back(std::move(buf));
                        }

                        assert(bufs.size() == ec_.get_k());
                        auto enc_bufs = ec_.encode(std::move(bufs));
                        assert(enc_bufs.size() == ec_.get_m());
                
                        /* save ec parity fragments */

                        // remove k fragments, remaining parity fragments
                        enc_bufs.erase(enc_bufs.begin(), enc_bufs.begin() + ec_.get_k());
                        assert(enc_bufs.size() == ec_.get_p());

                        for (auto p = 0U; p < ec_.get_p(); p++) {
                                auto c = cur_ - 1; // naming last fragment 
                                std::size_t frag_index = physical_fragment_index(path_, ec_.get_m(), ec_.get_k(), c);
                                const auto& fs = prefixes_.at(frag_index).first;
                                const auto& prefix = prefixes_.at(frag_index).second;
                                std::unique_ptr<FSWritableFile> fragment;
                                auto ret = fs->NewWritableFile(make_path_offset(prefix, path_, c, p + 1 /* not zero means parity */),
                                                               FileOptions(), &fragment, nullptr);
                                if (not ret.ok()) {
                                        throw ret;
                                }

                                fragment->SetWriteLifeTimeHint(hint_);
                                
                                auto& b = enc_bufs.at(p);
                                Slice s(b.data(), b.size());
                                assert(b.size() == fragment_length);
                                ret = fragment->Append(s, IOOptions(), nullptr);
                                if (not ret.ok()) {
                                        throw ret;
                                }
                        }

                        // remove mirroring fragments
                }

                void reopen_current_fragment(size_t s) {
                        assert(tail_fragment_.get() == nullptr); // call in constructor
                        assert(cur_ == 0);

                        cur_ = s;

                        // logical
                        std::size_t frag_off = logical_fragment_offset(cur_);

                        // pysical
                        assert(ec_.get_m() == prefixes_.size());
                        std::size_t frag_index = physical_fragment_index(path_, ec_.get_m(), ec_.get_k(), cur_);
                        const auto& fs = prefixes_.at(frag_index).first;
                        const auto& prefix = prefixes_.at(frag_index).second;
                        
                        std::unique_ptr<FSWritableFile> fragment;
                        // std::cout << "Writ reopen fragment " << path_ << " at " << prefix << " frag_off " << frag_off << " cur " << cur_ << std::endl;
                        // make_path_offset => logical
                        auto ret = fs->ReopenWritableFile(make_path_offset(prefix, path_, cur_), options_, &fragment, nullptr);
                        if (not ret.ok()) {
                                std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                throw ret;
                        }
                        tail_fragment_ = std::move(fragment);
                        tail_fragment_->SetWriteLifeTimeHint(hint_);
                        tail_fragment_offset_ = frag_off;
                }

                void open_current_fragment() {
                        if (tail_fragment_.get() != nullptr) {
                                tail_fragment_->Close(IOOptions(), nullptr);
                                tail_fragment_.reset();
                        }
                                
                        if (cur_ > 0 and (cur_ % (ec_.get_k() * fragment_length)) == 0) {
                                make_parity();
                        }

                        // logical
                        std::size_t frag_off = logical_fragment_offset(cur_);

                        // pysical
                        assert(ec_.get_m() == prefixes_.size());
                        std::size_t frag_index = physical_fragment_index(path_, ec_.get_m(), ec_.get_k(), cur_);
                        const auto& fs = prefixes_.at(frag_index).first;
                        const auto& prefix = prefixes_.at(frag_index).second;
                        
                        std::unique_ptr<FSWritableFile> fragment;
                        // std::cout << "Writ create fragment " << path_ << " at " << prefix << " frag_off " << frag_off << " cur " << cur_ << std::endl;
                        // make_path_offset => logical
                        auto ret = fs->NewWritableFile(make_path_offset(prefix, path_, cur_), options_, &fragment, nullptr);
                        if (not ret.ok()) {
                                std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                throw ret;
                        }
                        tail_fragment_ = std::move(fragment);
                        tail_fragment_->SetWriteLifeTimeHint(hint_);
                        tail_fragment_offset_ = frag_off;
                }

        public:
                ErasureFsWritableFile(const std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>>& prefixes,
                                      const ErasureCode& ec,
                                      const std::string& path,
                                      const FileOptions& options, uint64_t reopen /* >0 means reopen file offsset */, IODebugContext* /*dbg*/)
                        : prefixes_(prefixes), ec_(ec),
                          path_(path), options_(options), cur_(0),  tail_fragment_offset_(~0UL) {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;//" tid " << std::this_thread::get_id() << std::endl;
                        if (reopen > 0) {
                                reopen_current_fragment(reopen);
                        }
                        else {
                                open_current_fragment();
                        }
                }
                virtual ~ErasureFsWritableFile() {
                        // std::cout << __FUNCTION__ << " " << path_ << std::endl;
                }

                IOStatus Append(const Slice& data, const IOOptions& options,
                                IODebugContext* dbg) override {
                        std::lock_guard<std::mutex> lock(mtx_);
                        // std::cout << "WritableFile " << __FUNCTION__ << " cur_ " << cur_ << " append data size " << data.size() << " " << path_ << std::endl;
                        assert(logical_fragment_offset(cur_) == tail_fragment_offset_);
                        if (data.size() == 0) {
                                return IOStatus::OK();
                        }

                        /*
                                           cur<---------------->n
                                            v                   v
                             |---------|---------|---------|--------|
                                            |head|   body  |tail|
                         */
                        size_t head_size = fragment_length - (cur_ % fragment_length);
                        size_t tail_size = (cur_ + data.size()) % fragment_length;
                        size_t rem_size = data.size();
                        size_t total_write = 0;

                        if (head_size > data.size()) {
                                auto ret = tail_fragment_->Append(data, options, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                cur_ += data.size();
                                rem_size -= data.size();
                                total_write += data.size();
                                assert(rem_size + total_write == data.size());

                                return IOStatus::OK();
                        }

                        if (head_size <= data.size()) {
                                Slice data_head = Slice(data.data(), head_size);
                                auto ret = tail_fragment_->Append(data_head, options, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                assert(data_head.size() == head_size);
                                cur_ += data_head.size();
                                rem_size -= data_head.size();
                                total_write += data_head.size();
                                assert(rem_size + total_write == data.size());

                                open_current_fragment();
                                assert(logical_fragment_offset(cur_) == tail_fragment_offset_);
                        }

                        while (rem_size >= fragment_length) {
                                assert((cur_ % fragment_length) == 0);

                                Slice data_body = Slice(data.data() + total_write, fragment_length);
                                auto ret = tail_fragment_->Append(data_body, options, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                assert(data_body.size() == fragment_length);
                                cur_ += data_body.size();
                                rem_size -= data_body.size();
                                total_write += data_body.size();
                                assert(rem_size + total_write == data.size());                                

                                open_current_fragment();
                                assert(logical_fragment_offset(cur_) == tail_fragment_offset_);
                        }

                        if (tail_size > 0) {
                                assert((cur_ % fragment_length) == 0);                                
                                assert(tail_size < fragment_length);

                                Slice data_tail = Slice(data.data() + total_write, tail_size);
                                auto ret = tail_fragment_->Append(data_tail, options, dbg);
                                if (not ret.ok()) {
                                        std::cout << __FUNCTION__ << " " << path_ << " " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                assert(data_tail.size() == tail_size);
                                cur_ += data_tail.size();
                                rem_size -= data_tail.size();
                                total_write += data_tail.size();
                                assert(rem_size + total_write == data.size());                                
                        }
                        assert(rem_size == 0);
                        assert(logical_fragment_offset(cur_) == tail_fragment_offset_);
                        return IOStatus::OK();
                }
                
                IOStatus Append(const Slice& d, const IOOptions& opt, const DataVerificationInfo&, IODebugContext* dbg) override {
                        auto status = Append(d, opt, dbg);
                        return status;
                }

                IOStatus PositionedAppend(const Slice& d, uint64_t off, const IOOptions& opt, IODebugContext* dbg) override {
                        // std::cout << "WritableFile " << __FUNCTION__ << " "<< path_ << " cur_ " << cur_ << " off " << offset << " data.size() " << data.size() << std::endl;
                        auto ret = Truncate(off, opt, dbg);
                        if (not ret.ok()) {
                                return ret;
                        }

                        assert(cur_ == off);

                        auto status = Append(d, opt, dbg);
                        return status;
                }

                IOStatus PositionedAppend(const Slice& d, uint64_t off, const IOOptions& opt, const DataVerificationInfo&, IODebugContext* dbg) override {

                        // std::cout << "WritableFile " << __FUNCTION__ << " "<< path_ << " cur_ " << cur_ << " off " << offset << std::endl;   
                        auto status = PositionedAppend(d, off, opt, dbg);
                        return status;
                }
                
                IOStatus Truncate(uint64_t size, const IOOptions& options, IODebugContext* dbg) override {

                        // std::cout << "WritableFile " << __FUNCTION__ << " "<< path_ << " size "  << size << " cur " << cur_ << std::endl;
                        if (cur_ == size) {
                                return IOStatus::OK();
                        }
                        if (cur_ < size) {
                                std::cout << "Large Size Truncate " << cur_ << " to " << size << std::endl;
                                size_t diff = size - cur_;
                                std::vector<char> data(diff, '\0');
                                Slice s(data.data(), data.size());
                                return Append(s, IOOptions(), dbg);
                        }

                        std::lock_guard<std::mutex> lock(mtx_);                        
                        if (logical_fragment_offset(cur_) > logical_fragment_offset(size)) {
                                // std::cout << "offset move back Truncate " << path_  << " from " << logical_fragment_offset(cur_) << " to " << logical_fragment_offset(size) << std::endl;
                                // return IOStatus::NotSupported("truncate");
                                for (auto c = (logical_fragment_offset(size) + 1) * fragment_length;
                                     c <= logical_fragment_offset(cur_) * fragment_length;
                                     c += fragment_length) {
                                        // std::cout << "Try to remove fragment at " << path_ << " " << logical_fragment_offset(c) << std::endl;
                                        std::size_t frag_index = physical_fragment_index(path_, ec_.get_m(), ec_.get_k(), c);
                                        const auto& fs = prefixes_.at(frag_index).first;
                                        const auto& prefix = prefixes_.at(frag_index).second;
                                        // std::cout << "Delete due to truncate: " << make_path_offset(prefix, path_, c) << " " << fs->Name() << std::endl;
                                        auto ret = fs->DeleteFile(make_path_offset(prefix, path_, c), options, dbg);
                                        if (not ret.ok()) {
                                                std::cout << "Delete fail " << make_path_offset(prefix, path_, c) << " " << ret.ToString() << std::endl;
                                        }
                                }
                                cur_ = size;
                                open_current_fragment();
                        }
                        cur_ = size;
                        assert(logical_fragment_offset(cur_) == tail_fragment_offset_);
                        
                        auto ret = tail_fragment_->Truncate(size % fragment_length, options, dbg);

                        return ret;
                }
                IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
                        std::lock_guard<std::mutex> lock(mtx_);
                        auto ret = tail_fragment_->Close(options, dbg);
                        // std::cout << "WritableFile " << __FUNCTION__ << " "<< path_ << " " << ret.ToString() << std::endl;
                        assert(ret.ok());
                        return ret;
                }
                IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
                        std::lock_guard<std::mutex> lock(mtx_);
                        auto ret = tail_fragment_->Flush(options, dbg);
                        // std::cout << __FUNCTION__ << " " << path_ << " cur " << cur_ << " " << ret.ToString() << std::endl;
                        return ret;
                }
                IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
                        std::lock_guard<std::mutex> lock(mtx_);
                        // std::cout << __FUNCTION__ << " " << path_ << " cur " << cur_ << std::endl;
                        return tail_fragment_->Sync(options, dbg);
                }
                uint64_t GetFileSize(const IOOptions& /*options*/, IODebugContext* /*dbg*/) override {
                        std::lock_guard<std::mutex> lock(mtx_);
                        uint64_t ret = cur_;
                        // std::cout << "WritableFile " << __FUNCTION__ << " "<< path_ << " " << ret << std::endl;
                        return ret;
                }
                bool use_direct_io() const override {
                        auto ret = tail_fragment_->use_direct_io();
                        //std::cout << __FUNCTION__ << " " << ret << std::endl;
                        return ret;
                }
                size_t GetRequiredBufferAlignment() const override {
                        return kDefaultPageSize;
                }
                bool IsSyncThreadSafe() const override {
                        auto ret = tail_fragment_->IsSyncThreadSafe();
                        //std::cout << __FUNCTION__ << " " << ret << std::endl;
                        return ret;
                }
                void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
                        // std::cout << "WritableFile " << __FUNCTION__ << " " << path_ << " hint " << hint << std::endl;
                        tail_fragment_->SetWriteLifeTimeHint(hint);
                        hint_ = hint;
                }
        };

        class ErasureFsDirectory : public FSDirectory {
                std::vector<std::unique_ptr<FSDirectory>> fragments_;
        public:
                ErasureFsDirectory(const std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>>& prefixes, const std::string& path,
                                   const IOOptions& options, IODebugContext* dbg) {
                        assert(not prefixes.empty());
                        for (const auto& p: prefixes) {
                                const auto& fs = p.first;
                                const auto& prefix = p.second;
                                
                                std::unique_ptr<FSDirectory> fragment;
                                auto ret = fs->NewDirectory(make_path(prefix, path), options, &fragment, dbg);
                                if (not ret.ok()) {
                                        throw ret;
                                }
                                assert(fragment.get() != nullptr);
                                fragments_.push_back(std::move(fragment));
                                assert(fragment.get() == nullptr);
                        }
                }

                IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
                        for (const auto& fragment: fragments_) {
                                auto ret = fragment->Fsync(options, dbg);
                                if (not ret.ok()) {
                                        return ret;
                                }
                        }
                        return IOStatus::OK();
                }

                IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
                        for (const auto& fragment: fragments_) {
                                auto ret = fragment->Close(options, dbg);
                                if (not ret.ok()) {
                                        return ret;
                                }
                        }
                        return IOStatus::OK();
                }

                IOStatus FsyncWithDirOptions(const IOOptions& options, IODebugContext* dbg, const DirFsyncOptions& dir_fsync_options) override {
                        for (const auto& fragment: fragments_) {
                                auto ret = fragment->FsyncWithDirOptions(options, dbg, dir_fsync_options);
                                if (not ret.ok()) {
                                        return ret;
                                }
                        }
                        return IOStatus::OK();
                }
        };
        
        class ErasureFileSystem : public FileSystem {
                std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>> prefixes_;
                const ErasureCode ec_;
                
                const std::vector<std::string> SUB_DIRS = { "/tmp", "/var", "/var/tmp" };

                IOStatus create_prefixes() const {
                        assert(not prefixes_.empty());
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& dir = p.second;
                                IOOptions options;
                                auto ret = fs->CreateDirIfMissing(dir, options, nullptr);
                                if (not ret.ok()) {
                                        return ret;
                                }
                                for (const auto& subdir: SUB_DIRS) {
                                        ret = fs->CreateDirIfMissing(make_path(dir, subdir), options, nullptr);
                                        if (not ret.ok()) {
                                                return ret;
                                        }
                                }
                        }
                        return IOStatus::OK();
                }

                IOStatus create_testdir() const {
                        assert(not get_prefixes().empty());
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& dir = p.second;

                                IOOptions options;
                                std::string test_dir;

                                auto ret = fs->GetTestDirectory(options, &test_dir, nullptr);
                                if (not ret.ok()) {
                                        return ret;
                                }
                                
                                ret = fs->CreateDirIfMissing(make_path(dir, test_dir), options, nullptr);
                                if (not ret.ok()) {
                                        return ret;
                                }
                        }
                        return IOStatus::OK();
                }
                
                
        public:
                const std::vector<std::pair<std::shared_ptr<FileSystem>, std::string>>& get_prefixes() const {
                        return prefixes_;
                }
                
                ErasureFileSystem(const std::vector<std::string>& prefixes) : ec_(prefixes.size() - 1, 1, fragment_length) {
                        assert(static_cast<size_t>(ec_.get_m()) == prefixes.size());
                        assert(ec_.get_p() == 1);

                        assert(not prefixes.empty());

                        std::cout << __FUNCTION__;
                        for (const auto& p: prefixes) {
                                std::cout << " " << p;

                                ConfigOptions config_options_;
                                std::shared_ptr<FileSystem> fs;
                                auto status = FileSystem::CreateFromString(config_options_, p, &fs);                        
                                if (not status.ok()) {
                                        std::cout << __FUNCTION__ << " posix file system creation fail" << std::endl;
                                        throw status;
                                }
                                assert(fs.get() != nullptr);

                                const std::string pat = "posix://";
                                if (p.size() <= pat.size()) {
                                        throw "Invalid uri";
                                }
                                if (strncmp(pat.c_str(), p.c_str(), pat.size()) == 0) {
                                        const std::string& sub = p.substr(pat.size());
                                        prefixes_.emplace_back(fs, sub);
                                }
                                else {
                                        prefixes_.emplace_back(fs, "/");
                                }
                        }
                        
                        std::cout << std::endl;

                        auto status = create_prefixes();
                        if (not status.ok()) {
                                std::cout << __FUNCTION__ << "prefixes creation failed" << std::endl;
                        }
                        status = create_testdir();
                        if (not status.ok()) {
                                std::cout << __FUNCTION__ << "testdir creation failed" << std::endl;
                        }
                }
                
                ErasureFileSystem() = delete;
                ErasureFileSystem(const ErasureFileSystem&) = delete;
                ErasureFileSystem& operator = (const ErasureFileSystem&) = delete;
                virtual ~ErasureFileSystem() {}
                
                IOStatus NewSequentialFile(const std::string& path, const FileOptions& options, std::unique_ptr<FSSequentialFile>* result,
                                           IODebugContext* dbg) override {
                        try {
                                result->reset(new ErasureFsSequentialFile(get_prefixes(), ec_, path, options, dbg));
                                return IOStatus::OK();
                        }
                        catch (const IOStatus& s) {
                                std::cout << "NewSeq " << path << " failed " << s.ToString() << std::endl;
                                return IOError("NewSeq", path, ENOENT);
                        }
                }
                const char* Name() const {
                        return "ErasureFs";
                }
                const char* NickName() const {
                        return "ErasureFs";
                }

                IOStatus NewRandomAccessFile(const std::string& path, const FileOptions& options, std::unique_ptr<FSRandomAccessFile>* result,
                                             IODebugContext* dbg) override {
                        auto ret = FileExists(path, IOOptions(), dbg);
                        if (not ret.ok()) {
                                return IOError(std::string(__FUNCTION__) + " FileExist err", path, ENOENT);
                        }
                        try {
                                if (options.use_mmap_reads) {
                                        std::cout << __FUNCTION__ << " " << path << " using mmap reads" << std::endl;
                                }
                                IOOptions op;
                                result->reset(new ErasureFsRandomAccessFile(get_prefixes(), ec_, path, options, dbg));
                                return IOStatus::OK();
                        }
                        catch (const IOStatus& status) {
                                std::cout << "NewRnd " << path << " failed " << status.ToString() << std::endl;
                                return IOError(std::string(__FUNCTION__) + " ErasureFsRandomAccessFile err", path, ENOENT);
                        }
                }

                virtual IOStatus OpenWritableFile(const std::string& path,
                                                  const FileOptions& options, bool reopen,
                                                  std::unique_ptr<FSWritableFile>* result,
                                                  IODebugContext* dbg) {
                        // std::cout << __FUNCTION__ << " " << path << " reopen " << reopen << std::endl;
                        try {
                                uint64_t old_size = 0;
                                if (reopen) {
                                        [[maybe_unused]] auto ret = FileExists(path, IOOptions(), dbg);
                                        assert(ret.ok());
                                        ret = GetFileSize(path, IOOptions(), &old_size, dbg);
                                        assert(ret.ok());

                                        // std::cout << __FUNCTION__ << " reopen "  << path << " size " << old_size << std::endl;
                                }
                                else {
                                        DeleteFile(path, IOOptions(), dbg);
                                }

                                if (not reopen) { assert(old_size == 0); }
                                
                                result->reset(new ErasureFsWritableFile(get_prefixes(), ec_, path, options, old_size, dbg));

                                if (not reopen) {
                                        [[maybe_unused]] uint64_t s = (*result)->GetFileSize(IOOptions(), dbg);
                                        assert(s == 0);
                                }
                                else {
                                        [[maybe_unused]] uint64_t s = (*result)->GetFileSize(IOOptions(), dbg);
                                        [[maybe_unused]] uint64_t s2;
                                        auto ret = GetFileSize(path, IOOptions(), &s2, dbg);
                                        assert(ret.ok());
                                        if (s != old_size) {
                                                std::cout << __FUNCTION__ << " " << path
                                                          << " reopen old size " << old_size
                                                          << " new size " << s
                                                          << " fs size " << s2 << std::endl;
                                        }
                                        assert(s == old_size);
                                }

                                return IOStatus::OK();
                        }
                        catch (const IOStatus& status) {
                                std::cout << "NewWri " << path << " failed " << status.ToString() << std::endl;
                                if (status.IsNoSpace()) {
                                        return status;
                                }
                                return IOError("NewWri ", path, ENOENT);
                        }

                }

                
                IOStatus NewWritableFile(const std::string& path, const FileOptions& options, std::unique_ptr<FSWritableFile>* result,
                                         IODebugContext* dbg) override {
                        return OpenWritableFile(path, options, false, result, dbg);
                }

                IOStatus ReopenWritableFile(const std::string& path,
                                            const FileOptions& options,
                                            std::unique_ptr<FSWritableFile>* result,
                                            IODebugContext* dbg) override {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;
                        return OpenWritableFile(path, options, true, result, dbg);
                }

                
                IOStatus NewDirectory(const std::string& path, const IOOptions& options,
                                      std::unique_ptr<FSDirectory>* result, IODebugContext* dbg) override {
                        try {
                                result->reset(new ErasureFsDirectory(get_prefixes(), path, options, dbg));
                                return IOStatus::OK();
                        }
                        catch (const IOStatus& status) {
                                std::cout << "NewDir " << path << " failed " << status.ToString() << std::endl;
                                return status;
                        }
                }

                IOStatus GetChildren(const std::string& path, const IOOptions& options, std::vector<std::string>* result,
                                     IODebugContext* dbg) override {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;
                        // if some files end with _000000 -> then remove them and merge
                        result->clear();
                        std::set<std::string> dirs;
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& prefix = p.second;
                                std::vector<std::string> result_tmp;
                                auto ret = fs->GetChildren(make_path(prefix, path), options, &result_tmp, dbg);
                                // std::cout << __FUNCTION__ << " " << make_path(prefix, path) << " " << ret.ToString() << std::endl;
                                if (not ret.ok()) {
                                        return ret;
                                }
                                for (const auto& d: result_tmp) {
                                        if (is_parity_file(d)) {
                                                continue;
                                        }

                                        std::optional<std::string> remove_d = remove_path_offset(d);
                                        if (remove_d.has_value()) {
                                                dirs.insert(remove_d.value());
                                        }
                                        else {
                                                dirs.insert(d);
                                        }
                                }
                        }
                        
                        *result = std::vector<std::string>(dirs.begin(), dirs.end());
                        
                        // std::cout << __FUNCTION__ << " " << path << " OK ";
                        // for (const auto& d: dirs) {
                        //         std::cout << d << " ";
                        // }
                        // std::cout << std::endl;
                        return IOStatus::OK();
                }

                IOStatus FileExists(const std::string& path, const IOOptions& options, IODebugContext* dbg) override {
                        assert(ec_.get_m() == prefixes_.size());
                        {
                                // check file
                                std::size_t frag_index = physical_fragment_index(path, ec_.get_m(), ec_.get_k(), 0);
                                const auto& fs = prefixes_.at(frag_index).first;
                                const auto& prefix = prefixes_.at(frag_index).second;
                                auto ret_f = fs->FileExists(make_path_offset(prefix, path, 0), options, dbg);
                                // std::cout << __FUNCTION__ << " " << fs->Name() << " file " << make_path_offset(prefix, path, 0) <<" " << ret_f.ToString() << std::endl;
                                if (ret_f.ok()) {
                                        return IOStatus::OK();
                                }
                        }

                        // check dir
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& prefix = p.second ;
                                auto ret_d = fs->FileExists(make_path(prefix, path), options, dbg);
                                // std::cout << __FUNCTION__ << " " << fs->Name() << " dir " << make_path(prefix, path) <<" " << ret_d.ToString() << std::endl;
                                if (ret_d.ok()) {
                                        return IOStatus::OK();
                                }
                        }
                        return IOStatus::NotFound();
                }
                IOStatus DeleteFile(const std::string& path, const IOOptions& options, IODebugContext* dbg) override {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;

                        if (path.empty()) {
                                return IOError(__FUNCTION__, path + " is empty!", ENOENT);
                        }

                        // std::vector<std::string> children;
                        // if (GetChildren(path, options, &children, dbg).ok()) {
                        //         std::cout << __FUNCTION__ << " " << __LINE__ << " EISDIR " << path << " GetChildren" << std::endl;
                        //         return IOError(__FUNCTION__, path, EISDIR);
                        // }

                        if (path.at(path.size() - 1) == '/') {
                                // std::cout << __FUNCTION__ << " " << __LINE__ << " EISDIR " << path << " end with /" << std::endl;
                                return IOError(__FUNCTION__, path, EISDIR);
                        }
                        
                        if (not FileExists(path, options, dbg).ok()) {
                                // std::cout << __FUNCTION__ << " " << __LINE__ << " ENOENT " << path << " FileExists" << std::endl;
                                return IOError(__FUNCTION__, path, ENOENT);
                        }
                        
                        assert(ec_.get_m() == prefixes_.size());                        
                        for (uint64_t cur = 0; ; cur += fragment_length) {
                                std::size_t frag_index = physical_fragment_index(path, ec_.get_m(), ec_.get_k(), cur);
                                const auto& fs = prefixes_.at(frag_index).first;
                                const auto& prefix = prefixes_.at(frag_index).second;
                                auto ret = fs->FileExists(make_path_offset(prefix, path, cur), options, dbg);
                                if (ret == IOStatus::NotFound()) {
                                        // FileExists return notfound if it is not found
                                        break;
                                }
                                else if (not ret.ok()) {
                                        // std::cout << __FUNCTION__ << " " << __LINE__ << " " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                ret = fs->DeleteFile(make_path_offset(prefix, path, cur), options, dbg);
                                if (not ret.ok()) {
                                        // std::cout << __FUNCTION__ << " DeleteFile " << make_path_offset(prefix, path, cur) << " " << ret.ToString() << std::endl;
                                        return ret;
                                }
                                for (auto p = 1U; p <= ec_.get_p(); p++) {
                                        fs->DeleteFile(make_path_offset(prefix, path, cur, p), options, dbg);
                                }

                        }
                        return IOStatus::OK();
                }

                IOStatus CreateDir(const std::string& path, const IOOptions& options, IODebugContext* dbg) override {
                        std::cout << __FUNCTION__ << " " << path << std::endl;
                        std::vector<IOStatus> rets;
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& prefix = p.second ;
                                auto ret = fs->CreateDir(make_path(prefix, path), options, dbg);
                                // if (not ret.ok()) {
                                //         std::cout << __FUNCTION__ << " "<< fs->Name() << " " << make_path(prefix, path) << " " << ret.ToString() << std::endl;
                                // }
                                rets.push_back(std::move(ret));
                        }

                        decltype(rets)::iterator itr;
                        if ((itr = std::find_if(rets.begin(), rets.end(), [](const auto& s) { return not s.ok(); })) != rets.end()) {
                                return *itr;
                        }
                        else {
                                return IOStatus::OK();
                        }
                }

                IOStatus CreateDirIfMissing(const std::string& path, const IOOptions& options, IODebugContext* dbg) override {
                        std::vector<IOStatus> rets;
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& prefix = p.second;
                                auto ret = fs->CreateDirIfMissing(make_path(prefix, path), options, dbg);
                                // if (not ret.ok()) {
                                //         std::cout << __FUNCTION__ << " "<< fs->Name() << " " << make_path(prefix, path) << std::endl;                                                       }
                                rets.push_back(std::move(ret));
                        }
                        assert(rets.size() == get_prefixes().size());
                        assert(not rets.empty());
                        std::vector<IOStatus>::iterator r;
                        if ((r = std::find_if(rets.begin(), rets.end(), [](const auto& s) { return not s.ok(); })) != rets.end()) {
                                return *r;
                        }
                        else {
                                return IOStatus::OK();
                        }
                }
                IOStatus DeleteDir(const std::string& path, const IOOptions& options, IODebugContext* dbg) override {
                        std::vector<IOStatus> rets;
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& prefix = p.second;
                                auto ret = fs->DeleteDir(make_path(prefix, path), options, dbg);
                                // std::cout << __FUNCTION__ << " " << fs->Name() << " " << make_path(prefix, path) << " " << ret.ToString() << std::endl;
                                rets.push_back(std::move(ret));
                        }
                        assert(rets.size() == get_prefixes().size());
                        assert(not rets.empty());
                        auto found = std::find_if(rets.begin(), rets.end(), [](const IOStatus& s) { return not s.ok(); });
                        auto ret = (found != rets.end()) ? *found : IOStatus::OK();
                        // std::cout << __FUNCTION__ << " " << path << " " << ret.ToString() << std::endl;
                        return ret;

                }
                IOStatus GetFileSize(const std::string& path, const IOOptions& options, uint64_t* size,
                                     IODebugContext* dbg) override {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;

                        auto ret = FileExists(path, options, dbg);
                        if (not ret.ok()) {
                                // stat syscall returns IO error not file not found
                                *size = 0;
                                return IOError("File not found GetFileSize", path, ENOENT);
                        }
                        
                        uint64_t acc = 0;
                        for (size_t cur = 0; ; cur += fragment_length) {
                                std::size_t frag_index = physical_fragment_index(path, ec_.get_m(), ec_.get_k(), cur);
                                
                                const auto& fs = prefixes_.at(frag_index).first;
                                const auto& prefix = prefixes_.at(frag_index).second;
                                uint64_t s = 0;
                                auto encoded_fragment_path = make_path_offset(prefix, path, cur);
                                ret = fs->GetFileSize(encoded_fragment_path, options, &s, dbg);
                                if (ret.IsIOError()) {
                                        // std::cout << __FUNCTION__ << " " << fs->Name() << " "
                                        //           << encoded_fragment_path << " " << ret.ToString() << " break" << std::endl;    
                                        break;
                                }
                                else if (not ret.ok()) {
                                        // std::cout << __FUNCTION__ << " " << path << " " << ret.ToString() << std::endl;    
                                        return ret;
                                }
                                // std::cout << __FUNCTION__ << " " << fs->Name() << " " << encoded_fragment_path << " found + size " << s << std::endl;
                                acc += s;
                        }
                        *size = acc;
                        // std::cout << __FUNCTION__ << " " << path << " " << *size << std::endl;
                        return IOStatus::OK();
                }

                IOStatus GetFileModificationTime(const std::string& path, const IOOptions& options,
                                                 uint64_t* file_mtime, IODebugContext* dbg) override {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;

                        bool isdir = false;
                        auto ret = IsDirectory(path, options, &isdir, dbg);
                        if (ret.ok() and isdir) {
                                uint64_t last_mtime = 0;                                
                                for (const auto& p: get_prefixes()) {
                                        const auto& fs = p.first;
                                        const auto& prefix = p.second;
                                        uint64_t m;
                                        ret = fs->GetFileModificationTime(make_path(prefix, path), options, &m, dbg);
                                        if (ret.ok()) {
                                                last_mtime = std::max(last_mtime, m);
                                        }
                                }
                                assert(last_mtime != 0);
                                *file_mtime = last_mtime;
                                return IOStatus::OK();
                        }

                        uint64_t last_mtime = 0;
                        for (size_t cur = 0; ; cur += fragment_length) {
                                std::size_t frag_index = physical_fragment_index(path, ec_.get_m(), ec_.get_k(), cur);
                                const auto& fs = prefixes_.at(frag_index).first;
                                const auto& prefix = prefixes_.at(frag_index).second;
                                
                                uint64_t m = 0;
                                ret = fs->GetFileModificationTime(make_path_offset(prefix, path, cur), options, &m, dbg);
                                if (ret.IsIOError()) {
                                        break;
                                }
                                else if (not ret.ok()) {
                                        // std::cout << __FUNCTION__ << " " << path << " " << ret.ToString() << std::endl;
                                        continue;
                                }
                                last_mtime = std::max(last_mtime, m);
                        }
                        *file_mtime = last_mtime;
                        if (last_mtime == 0) {
                                return IOStatus::NotSupported(std::string(__FUNCTION__) + " " + path);
                        }
                        return IOStatus::OK();
                }
                IOStatus RenameFile(const std::string& src, const std::string& target, const IOOptions& options,
                                    IODebugContext* dbg) override {
                        // std::cout << __FUNCTION__ << " " << src << " to " << target << std::endl;
                        bool src_isdir = false;
                        auto ret = IsDirectory(src, options, &src_isdir, dbg);
                        if (not ret.ok()) {
                                std::cout << "IsDirectory " << src << " " << ret.ToString() << std::endl;
                        }

                        bool target_isdir = false;
                        ret = IsDirectory(target, options, &target_isdir, dbg);
                        if (not ret.ok()) {
                                std::cout << "IsDirectory " << src << " " << ret.ToString() << std::endl;
                        }

                        if (src_isdir) {
                                // std::cout << __FUNCTION__ << " Directory Mode src " << src << " to " << target << std::endl;
                                for (const auto& p: get_prefixes()) {
                                        const auto& fs = p.first;
                                        const auto& prefix = p.second;
                                        ret = fs->RenameFile(make_path(prefix, src), make_path(prefix, target), options, dbg);
                                        // std::cout << __FUNCTION__ << " sub " << make_path(prefix, src) << " "
                                        //           << make_path(prefix, target) << " " << ret.ToString() << std::endl;
                                        if (ret == IOStatus::NotFound()) {
                                                // ufrop does not suport directory; so src is not found in some fs
                                                continue;
                                        }
                                        if (not ret.ok()) {
                                                return ret;
                                        }
                                }
                                return IOStatus::OK();
                        }
                        

                        // std::cout << __FUNCTION__ << " " << src << " isdir = " << src_isdir << " to " << target << " isdir = " << target_isdir << std::endl;
                        assert(not src_isdir);
                        // assert(not target_isdir);
                                
                        std::vector<IOStatus> rets;
                        for (uint64_t cur = 0; ; cur += fragment_length) {
                                std::size_t src_frag_index = physical_fragment_index(src, ec_.get_m(), ec_.get_k(), cur);
                                const auto& src_fs = prefixes_.at(src_frag_index).first;
                                const auto& src_prefix = prefixes_.at(src_frag_index).second;
                                std::size_t target_frag_index = physical_fragment_index(target, ec_.get_m(), ec_.get_k(), cur);
                                const auto& target_fs = prefixes_.at(target_frag_index).first;
                                const auto& target_prefix = prefixes_.at(target_frag_index).second;
                                
                                // std::cout << "Rename from " << make_path_offset(src_prefix, src, cur) << " to " << make_path_offset(target_prefix, target, cur) << std::endl;
                                if (false and (strcmp(target_fs->Name(), src_fs->Name()) == 0) and
                                    (strcmp(target_fs->Name(), "PosixFileSystem") == 0)) {
                                        ret = target_fs->RenameFile(make_path_offset(src_prefix, src, cur), make_path_offset(target_prefix, target, cur), options, dbg);
                                        rets.push_back(std::move(ret));
                                        if (not rets.back().ok()) {
                                                break;
                                        }
                                }
                                else {
                                        std::unique_ptr<FSSequentialFile> src_seq_fragment;
                                        FileOptions fop;
                                        ret = src_fs->NewSequentialFile(make_path_offset(src_prefix, src, cur), fop, &src_seq_fragment, dbg);
                                        rets.push_back(std::move(ret));
                                        if (not rets.back().ok()) {
                                                // std::cout << __FUNCTION__ << " " << __LINE__ << " src open newseq " << make_path_offset(src_prefix, src, cur) << " " << rets.back().ToString() << std::endl;
                                                break;
                                        }
                                        std::unique_ptr<FSWritableFile> target_writable_fragment;
                                        ret = target_fs->NewWritableFile(make_path_offset(target_prefix, target, cur), fop, &target_writable_fragment, dbg);
                                        rets.push_back(std::move(ret));
                                        if (not rets.back().ok()) {
                                                // std::cout << __FUNCTION__ << " " << __LINE__ << " target open newwrite " << make_path_offset(target_prefix, target, cur) << " " << rets.back().ToString() << std::endl;
                                                break;
                                        }

                                        std::vector<char> buf(fragment_length);
                                        assert(buf.size() == fragment_length);
                                        Slice s;
                                        ret = src_seq_fragment->Read(fragment_length, options, &s, buf.data(), dbg);
                                        rets.push_back(std::move(ret));
                                        if (not rets.back().ok()) {
                                                std::cout << __FUNCTION__ << " " << __LINE__ << " src read " << make_path_offset(src_prefix, src, cur) << " " << rets.back().ToString() << std::endl;                                                
                                                break;
                                        }

                                        assert(s.size() <= fragment_length);
                                        ret = target_writable_fragment->Append(s, options, dbg);
                                        rets.push_back(std::move(ret));
                                        if (not rets.back().ok()) {
                                                std::cout << __FUNCTION__ << " " << __LINE__ << " target append " << make_path_offset(target_prefix, target, cur) << " " << rets.back().ToString() << std::endl;                                                
                                                break;
                                        }
                                        
                                        // delete rename from file
                                        ret = src_fs->DeleteFile(make_path_offset(src_prefix, src, cur), options, dbg);
                                        rets.push_back(std::move(ret));
                                        if (not rets.back().ok()) {
                                                std::cout << __FUNCTION__ << " " << __LINE__ << " src delete " << make_path_offset(src_prefix, src, cur) << " " << rets.back().ToString() << std::endl;
                                                break;
                                        }

                                        // std::cout << __FUNCTION__ << make_path_offset(src_prefix, src, cur) << " to "
                                        //           << make_path_offset(target_prefix, target, cur) << " cpy " << s.size() << std::endl;

                                        for (auto p = 1U; p <= ec_.get_p(); p++) {
                                                src_fs->DeleteFile(make_path_offset(src_prefix, src, cur, p), options, dbg);
                                        }
                                        
                                }
                                
                        }
                        // std::cout << __FUNCTION__ << " RET " << src << " to " << target << " " << rets.at(0).ToString() << std::endl;
                        //assert(not FileExists(src, options, dbg).ok());
                        //assert(FileExists(target, options, dbg).ok());
                        return rets.at(0);
                }
                IOStatus LockFile(const std::string& path,
                                  const IOOptions& options, FileLock** lock,
                                  IODebugContext* dbg) override {
                        //mkdir(path.substr(0, path.size() - 4).c_str(), 0777);
                        //return FileSystem::Default()->LockFile(path, options, lock, dbg);
                        try {
                                *lock = new ErasureFsFileLock(get_prefixes(), ec_, path, options, dbg);
                                // std::cout << __FUNCTION__ << " " <<  path << " " << *lock << std::endl;                                
                                return IOStatus::OK();
                        }
                        catch (const IOStatus& s) {
                                return s;
                        }
                }
                IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                                    IODebugContext* dbg) override {
                        //return FileSystem::Default()->UnlockFile(lock, options, dbg);
                        ErasureFsFileLock* my_lock = static_cast<ErasureFsFileLock*>(lock);
                        // std::cout << __FUNCTION__ << " " << lock << std::endl;                                
                        
                        auto ret = my_lock->UnlockFile(options, dbg);
                        delete my_lock;
                        return ret;
                }

                IOStatus GetTestDirectory(const IOOptions& options,
                                          std::string* result,
                                          IODebugContext* dbg) override {
                        const auto fs = get_prefixes().at(0).first;
                        auto ret = fs->GetTestDirectory(options, result, dbg);
                        // std::cout << __FUNCTION__ << " " << *result << std::endl;
                        auto ret2 = CreateDirIfMissing(*result, options, dbg);

                        if (ret == ret2) {
                                return ret;
                        }
                        else if (not ret.ok()) {
                                return ret;
                        }
                        else {
                                return ret2;
                        }
                        
                }

                IOStatus GetAbsolutePath(const std::string& path,
                                         [[maybe_unused]] const IOOptions& options,
                                         std::string* output_path,
                                         [[maybe_unused]] IODebugContext* dbg) override {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;
                        
                        if (path[0] == '/') {
                                *output_path = path;
                                return IOStatus::OK();
                        }
                        
                        std::vector<std::string> output_paths;
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& prefix = p.second;
                                std::string output_path_tmp;
                                auto ret = fs->GetAbsolutePath(make_path(prefix, path), options, &output_path_tmp, dbg);
                                if (not ret.ok()) {
                                        return ret;
                                }
                                assert(output_path_tmp.size() >= prefix.size());
                                output_paths.push_back(std::move(output_path_tmp));
                        }
                        assert(std::equal(output_paths.begin() + 1, output_paths.end(), output_paths.begin()));
                        *output_path = output_paths.at(0);
                        return IOStatus::OK();
                }

                IOStatus IsDirectory(const std::string& path,
                                     const IOOptions& options, bool* is_dir,
                                     IODebugContext* dbg) override {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;
                        // if file then not file
                        bool is_dir_acc = false;
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& prefix = p.second;
                                bool is_dir_tmp;
                                auto ret = fs->IsDirectory(make_path(prefix, path), options, &is_dir_tmp, dbg);
                                // std::cout << __FUNCTION__ << " " << fs->Name() << " " << path << " " << ret.ToString() << std::endl;
                                if (ret.ok()) {
                                        is_dir_acc |= is_dir_tmp;
                                }
                        }
                        *is_dir = is_dir_acc;
                        return IOStatus::OK();
                }
                IOStatus GetFreeSpace(const std::string& path,
                                      const IOOptions& options,
                                      uint64_t* diskfree,
                                      IODebugContext* dbg) override {
                        // std::cout << __FUNCTION__ << " " << path << std::endl;                        
                        uint64_t ret_space = 0;
                        for (const auto& p: get_prefixes()) {
                                const auto& fs = p.first;
                                const auto& prefix = p.second;
                                uint64_t s = 0;
                                auto ret = fs->GetFreeSpace(make_path(prefix, path), options, &s, dbg);
                                if (ret.ok()) {
                                        ret_space += s;
                                }
                        }                        
                        *diskfree = ret_space;
                        return IOStatus::OK();
                }
                IOStatus LinkFile(const std::string& src, const std::string& target,
                                  const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) override {
                        std::cout << __FUNCTION__ << " "  << src << " to " << target << std::endl;
                        return IOStatus::NotSupported("LinkFile " + src + " to " + target);
                }
                
        };


        /*
          erasurefs://fdpring:///dev/dev/ng0n1;posix:///mnt/hoge
          fs1 fs2 fs3 : m = 3が決定
          そのうえで 0 =< k < m を指定
          デフォルト p = 1
          k = m - p = 2
          ec_m = total storage
          ec_p = default 1
          ec_k = ec_m - ec_p
        */
        FactoryFunc<FileSystem> erasurefs_reg =
                ObjectLibrary::Default()->AddFactory<FileSystem>(
                                                                 ObjectLibrary::PatternEntry("erasurefs").AddSeparator("://", false),
                                                                 [](const std::string& uri, std::unique_ptr<FileSystem>* f,
                                                                    std::string* errmsg) {
                                                                         std::cout << __FUNCTION__ << " " << uri << std::endl;
                                                                         const std::string pat = "erasurefs://";
                                                                         if (uri.size() <= pat.size()) {
                                                                                 *errmsg = "Invalid uri";
                                                                                 f->reset();
                                                                                 return f->get();
                                                                         }
                                                                         const std::string& sub = uri.substr(pat.size());
                                                                         const std::vector<std::string>& subs = split(sub, ';');
                                                                         f->reset(new ErasureFileSystem(subs));
                                                                         return f->get();
                                                                 });

        
        // std::shared_ptr<FileSystem> FileSystem::Default() {
        //         char* e = getenv("TEST_FS_URI");
        //         std::string uri(e);
        //         const std::string pat = "erasurefs://";
        //         assert(uri.size() > pat.size());
        //         std::string sub = uri.substr(pat.size());
        //         std::vector<std::string> subs = split(sub, ':');
        //         STATIC_AVOID_DESTRUCTION(std::shared_ptr<FileSystem>, instance)
        //                 (std::make_shared<ErasureFileSystem>(subs));
        //         return instance;
        // }
}  // namespace ROCKSDB_NAMESPACE

