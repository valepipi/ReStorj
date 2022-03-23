#include <iostream>
#include <thread>
#include <unordered_map>

#include "storj/data_manager.h"

storj::data_manager *manager;
bool running = false;

void thread_scanner_func()
{
    while (running) {
        // 扫描出需要修复的 segments
        std::tuple<std::vector<std::string>, std::vector<int>, std::vector<int>> tuple = manager->scan_corrupted_segments();
        std::vector<std::string> &segment_ids = std::get<0>(tuple);
        std::vector<int> &ks = std::get<1>(tuple);
        std::vector<int> &rs = std::get<2>(tuple);
        // segments 排序
        storj::data_manager::sort_segments(segment_ids, ks, rs);
        // 修复 segments
        for (const auto &segment_id: segment_ids) {
            manager->repair_segment(segment_id);
        }

        // 等待 30s
        std::this_thread::sleep_for(std::chrono::seconds(30));
    }
}

int main()
{
    manager = new storj::data_manager();
    running = true;
    std::thread thread_scanner(thread_scanner_func);
    thread_scanner.join();
    delete manager;
    return 0;
}
