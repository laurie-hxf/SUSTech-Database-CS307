//
// Created by laurie on 25-4-24.
//
#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <mutex>
#include <chrono>
#include <pqxx/pqxx>

const std::vector<std::string> sql_files = {
    "../sql/company.sql","../sql/supply_center.sql", "../sql/contract.sql","../sql/salesman.sql", "../sql/product.sql",
"../sql/orders.sql","../sql/product_model.sql"
};

void load_sql_file(pqxx::connection& conn, const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open " << filepath << std::endl;
        return;
    }

    std::string line;
    while (std::getline(file, line)) {
        try {
            pqxx::work txn(conn);
            txn.exec(line);
            txn.commit();
        } catch (const std::exception& e) {
            std::cerr << "Failed to execute line in " << filepath << ": " << e.what() << std::endl;
        }
    }
}

void import_serial() {
    try {
        pqxx::connection conn("dbname=cs307_project1_test user=checker password=123456 hostaddr=127.0.0.1 port=5432");

        for (const auto& file : sql_files) {
            std::cout << "Loading: " << file << std::endl;
            load_sql_file(conn, file);
        }
        auto end = std::chrono::high_resolution_clock::now();

    } catch (const std::exception& e) {
        std::cerr << "Database error (serial): " << e.what() << std::endl;
    }
}

void import_parallel() {
    try {
        pqxx::connection conn("dbname=cs307_project1_test user=checker password=123456 hostaddr=127.0.0.1 port=5432");

        std::mutex db_mutex;
        std::vector<std::thread> threads;

        for (const auto& file : sql_files) {
            threads.emplace_back([&conn, &file, &db_mutex]() {
                std::lock_guard<std::mutex> lock(db_mutex);
                std::cout << "Thread loading: " << file << std::endl;
                load_sql_file(conn, file);
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

    } catch (const std::exception& e) {
        std::cerr << "Database error (parallel): " << e.what() << std::endl;
    }
}

int main() {
    std::cout << "Choose import mode:\n1. Serial\n2. Parallel\n> ";
    int choice;
    std::cin >> choice;
    auto start = std::chrono::high_resolution_clock::now();
    switch (choice) {
        case 1:
            import_serial();
            break;
        case 2:
            import_parallel();
            break;
        default:
            std::cout << "Invalid choice." << std::endl;
            break;
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Execution time: " << 1.0 * duration.count() / 1000 << " s" << std::endl;

    return 0;
}
