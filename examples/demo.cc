#include <iostream>
#include <string>
#include "lsm/db.h"
#include "lsm/options.h"

int main() {
    lsm::Options options;
    options.create_if_missing = true;
    
    lsm::DB* db = nullptr;
    lsm::Status status = lsm::DB::Open(options, "demo_db", &db);
    
    if (!status.ok()) {
        std::cerr << "Unable to open database: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "Successfully opened demo_db" << std::endl;

    lsm::WriteOptions write_options;
    status = db->Put(write_options, "language", "C++17");
    if (status.ok()) {
        std::cout << "Put -> language: C++17" << std::endl;
    }

    lsm::ReadOptions read_options;
    std::string value;
    status = db->Get(read_options, "language", &value);
    if (status.ok()) {
        std::cout << "Get <- language: " << value << std::endl;
    }

    status = db->Delete(write_options, "language");
    if (status.ok()) {
        std::cout << "Delete -> language" << std::endl;
    }

    status = db->Get(read_options, "language", &value);
    if (status.IsNotFound()) {
        std::cout << "Get <- language: <Not Found>" << std::endl;
    }

    db->Put(write_options, "key1", "val1");
    db->Put(write_options, "key3", "val3");
    db->Put(write_options, "key2", "val2");

    std::cout << "\nScanning database:" << std::endl;
    lsm::Iterator* it = db->NewIterator(read_options);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << it->key().ToString() << ": " << it->value().ToString() << std::endl;
    }
    
    delete it;
    delete db;
    
    return 0;
}
