### The code in this repository is provided solely for research purposes, and you are welcome to use it as you see fit. 
### what is this plugin?
ErasureFs is a plugin for [RocksDB](https://github.com/facebook/rocksdb) that utilize multiple Flexible Data Placement (FDP) SSDs.
It splits data, adds erasure correction codes, and stores them on multiple SSDs.
It uses [ufrop](UFROP_URI) as backend to interact with FDP SSDs.


### limitation
Currently, it only supports normal data store and load, not decryption in the event of drive loss.

### how to build

install required libraries.

    $ sudo apt install build-essential git liburing-dev libnvme-dev libboost-dev libisal-dev 

build `db_bench` with ErasureFs and ufrop plugin enabled.

    $ git clone https://github.com/facebook/rocksdb.git
    $ cd rocksdb
    $ git clone -b erasurefs https://github.com/kioxia-jp/ufrop.git plugin/ufrop
    $ git clone https://github.com/kioxia-jp/erasurefs.git plugin/erasurefs
    $ DEBUG_LEVEL=0 LIB_MODE=shared ROCKSDB_PLUGINS="erasurefs ufrop" make -j32 db_bench


### How to use
ErasureFs uses NVMe character device directly (such as `/dev/ng0n1`).
If you want to run RocksDB applications with ErasureFs enabled with user privileges, it is necessary for them to be able to access the character devices.

    $ chmod 777 /dev/ng0n1 /dev/ng1n1 /dev/ng2n1

To enable FDP, run applications with `USE_FDP` environment. To specify the target SSDs, `--fs_uri=erasurefs://` is needed.

    $ USE_FDP=true USE_DISCARD=true ./db_bench --fs_uri="erasurefs://fdpring:///dev/ng0n1;fdpring:///dev/ng1n1;fdpring:///dev/ng2n1" --benchmarks=fillrandom


### License
ErasureFs is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in t
he root directory).
You may select, at your option, one of the above-listed licenses.
