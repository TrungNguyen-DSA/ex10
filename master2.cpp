#include <iostream>
#include <vector>
#include <queue>
#include <map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <fstream>
#include <climits>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include "protocol.hpp"

#define PORT 8080
#define MAX_EVENTS 1024

// ==========================================
// THAY ĐỔI CẤU HÌNH THÍ NGHIỆM TẠI ĐÂY
const int SCHEDULER_MODE = 3;    // 1 = FIFO, 2 = Round Robin, 3 = Least Loaded
const int TOTAL_TEST_TASKS = 100; // Yêu cầu Experiment 1: 100 Tác vụ
// ==========================================

auto exp_start_time = std::chrono::high_resolution_clock::now();
std::map<int, std::chrono::high_resolution_clock::time_point> task_arrival_times;
double total_response_time = 0;
int completed_tasks_count = 0;

std::mutex state_mutex;
std::queue<Packet> task_queue;
std::condition_variable cv;

std::map<int, int> worker_sockets;       
std::map<int, time_t> worker_heartbeats; 
std::map<int, std::map<int, Packet>> worker_tasks; 

size_t rr_index = 0; 

// --- LUỒNG ĐIỀU PHỐI CHÍNH XÁC SỬ DỤNG SWITCH...CASE ---
void scheduler_thread() {
    while (true) {
        std::unique_lock<std::mutex> lock(state_mutex);
        cv.wait(lock, [] { return !task_queue.empty() && !worker_sockets.empty(); });
        
        int best_worker = -1;
        std::vector<int> active_workers;
        for (auto const& [w_id, _] : worker_sockets) {
            active_workers.push_back(w_id);
        }

        // Hiện thực hóa mục 3.3 bằng cấu trúc Switch...Case
        switch (SCHEDULER_MODE) {
            case 1: {
                // FIFO: Lấy trực tiếp worker đầu tiên trong danh sách hoạt động
                best_worker = active_workers.front();
                break;
            }
            case 2: {
                // Round Robin: Chia xoay vòng tịnh tiến theo vòng tròn
                best_worker = active_workers[rr_index % active_workers.size()];
                rr_index++;
                break;
            }
            case 3: {
                // Least Loaded: Quét tìm Worker đang gánh ít Task nhất
                size_t min_load = SIZE_MAX;
                for (int w_id : active_workers) {
                    if (worker_tasks[w_id].size() < min_load) {
                        min_load = worker_tasks[w_id].size();
                        best_worker = w_id;
                    }
                }
                break;
            }
        }

        if (best_worker != -1) {
            Packet task = task_queue.front();
            task_queue.pop();
            
            send(worker_sockets[best_worker], &task, sizeof(Packet), 0);
            worker_tasks[best_worker][task.task_id] = task;
        }
        lock.unlock();
    }
}

void heartbeat_monitor() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::lock_guard<std::mutex> lock(state_mutex);
        time_t now = std::time(nullptr);
        for (auto it = worker_heartbeats.begin(); it != worker_heartbeats.end();) {
            if (now - it->second > 6) { 
                int dead_id = it->first;
                std::cout << "[Alert] Worker " << dead_id << " Timeout.\n";
                close(worker_sockets[dead_id]);
                for (auto const& [t_id, pkt] : worker_tasks[dead_id]) {
                    task_queue.push(pkt);
                }
                worker_sockets.erase(dead_id); worker_tasks.erase(dead_id);
                it = worker_heartbeats.erase(it);
                cv.notify_one(); 
            } else { ++it; }
        }
    }
}

int main() {
    std::thread t_sched(scheduler_thread);
    std::thread t_heart(heartbeat_monitor);

    // Sinh 100 tác vụ trộn đều cả 4 thể loại (A, B, C, D) theo gợi ý mục 5
    std::thread t_generator([]() {
        std::this_thread::sleep_for(std::chrono::seconds(4)); // Chờ các worker kết nối ổn định
        std::lock_guard<std::mutex> lock(state_mutex);
        exp_start_time = std::chrono::high_resolution_clock::now();
        
        for(int i = 1; i <= TOTAL_TEST_TASKS; ++i) {
            Packet p{}; p.type = TASK_SUBMIT; p.task_id = i;
            
            // Xoay vòng tạo đều cả 4 loại Task khác nhau
            if (i % 4 == 1) {
                p.operation = PRIME_COUNTING; p.data = 200000; // Khối lượng tính toán Task A
            } else if (i % 4 == 2) {
                p.operation = MATRIX_MULTIPLICATION; p.data = 250; // Khối lượng tính toán Task B
            } else if (i % 4 == 3) {
                p.operation = MONTE_CARLO_PI; p.data = 30000000; // Khối lượng tính toán Task C
            } else {
                p.operation = WORD_COUNT; p.data = 1000000; // Khối lượng tính toán Task D
            }
            
            task_queue.push(p);
            task_arrival_times[p.task_id] = std::chrono::high_resolution_clock::now(); // Ghi mốc thời gian Task đến
        }
        std::cout << "[System] " << TOTAL_TEST_TASKS << " Mix Tasks (A, B, C, D) pushed to Ready Queue.\n";
        cv.notify_all();
    });

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(PORT);
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)); listen(server_fd, SOMAXCONN);

    int epoll_fd = epoll_create1(0); struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN; ev.data.fd = server_fd; epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::cout << "Master Active. Mode: " << SCHEDULER_MODE << "\n";

    while (true) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int n = 0; n < nfds; ++n) {
            if (events[n].data.fd == server_fd) {
                int client_sock = accept(server_fd, nullptr, nullptr);
                ev.events = EPOLLIN; ev.data.fd = client_sock;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_sock, &ev);
            } else {
                int client_fd = events[n].data.fd; Packet pkt;
                ssize_t bytes = recv(client_fd, &pkt, sizeof(Packet), 0);
                
                if (bytes <= 0) {
                    close(client_fd); epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
                    continue;
                }

                std::lock_guard<std::mutex> lock(state_mutex);
                if (pkt.type == REGISTER) {
                    worker_sockets[pkt.worker_id] = client_fd;
                    worker_heartbeats[pkt.worker_id] = std::time(nullptr);
                    cv.notify_one();
                } else if (pkt.type == HEARTBEAT) {
                    worker_heartbeats[pkt.worker_id] = std::time(nullptr);
                } else if (pkt.type == RESULT) {
                    worker_tasks[pkt.worker_id].erase(pkt.task_id);
                    completed_tasks_count++;

                    // Tính toán Response Time của Task này cụ thể
                    auto now = std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double> resp_dur = now - task_arrival_times[pkt.task_id];
                    total_response_time += resp_dur.count();
                    
                    if (completed_tasks_count == TOTAL_TEST_TASKS) {
                        std::chrono::duration<double> total_execution_time = now - exp_start_time;
                        double avg_response_time = total_response_time / TOTAL_TEST_TASKS;
                        
                        std::cout << "\n=========== THÍ NGHIỆM HOÀN THÀNH ===========\n";
                        std::cout << "Thuật toán (Mode)            : " << SCHEDULER_MODE << "\n";
                        std::cout << "Số lượng Worker tham gia     : " << worker_sockets.size() << "\n";
                        std::cout << "Execution Time (Tổng thời gian): " << total_execution_time.count() << " giây\n";
                        std::cout << "Average Response Time        : " << avg_response_time << " giây\n";
                        std::cout << "==============================================\n\n";
                        
                        // Xuất dữ liệu thẳng ra file CSV
                        std::ofstream file("experiment_report.csv", std::ios::app);
                        if (file.is_open()) {
                            // Cấu trúc cột: Thuật toán, Số lượng Worker, Tổng thời gian, Thời gian phản hồi TB
                            file << SCHEDULER_MODE << "," << worker_sockets.size() << "," 
                                 << total_execution_time.count() << "," << avg_response_time << "\n";
                            file.close();
                            std::cout << "[Report] Số liệu đã lưu vào experiment_report.csv\n";
                        }
                    }
                }
            }
        }
    }
    t_sched.join(); t_heart.join(); t_generator.join();
    return 0;
}
