#include <iostream>
#include <chrono>
#include <vector>
#include <string>
#include <random>
#include "lsm/db.h"
#include "lsm/options.h"

using namespace lsm;

int main() {
    std::string dbname = "bench_db";
    
    #ifdef _WIN32
    system(("rmdir /S /Q " + dbname).c_str());
    #else
    system(("rm -rf " + dbname).c_str());
    #endif

    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 4 * 1024 * 1024; // 4MB

    DB* db;
    Status s = DB::Open(options, dbname, &db);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
        return 1;
    }

    const int num_entries = 100000;
    std::string value(100, 'x'); // 100 bytes value
    
    std::cout << "Starting benchmark with " << num_entries << " entries..." << std::endl;

    // 1. Sequential Writes
    WriteOptions wo;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_entries; ++i) {
        char key[100];
        snprintf(key, sizeof(key), "%016d", i);
        db->Put(wo, key, value);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "Sequential Writes: " << diff.count() << " seconds ("
              << (num_entries / diff.count()) << " ops/sec)" << std::endl;

    // 2. Random Reads
    ReadOptions ro;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, num_entries - 1);

    int found = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_entries; ++i) {
        char key[100];
        snprintf(key, sizeof(key), "%016d", dis(gen));
        std::string val;
        if (db->Get(ro, key, &val).ok()) {
            found++;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    std::cout << "Random Reads:      " << diff.count() << " seconds ("
              << (num_entries / diff.count()) << " ops/sec), found " << found << std::endl;

    // 3. Sequential Range Scan
    start = std::chrono::high_resolution_clock::now();
    Iterator* iter = db->NewIterator(ro);
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        count++;
    }
    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    std::cout << "Sequential Scan:   " << diff.count() << " seconds ("
              << (count / diff.count()) << " ops/sec), scanned " << count << std::endl;
    delete iter;

    delete db;
    // Keep the DB directory for inspection if wanted, or clean it
    
    return 0;
}
