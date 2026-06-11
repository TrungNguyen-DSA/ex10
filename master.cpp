#include <iostream>
#include <vector>
#include <queue>
#include <map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <fstream>
#include <cstring>
#include <climits>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include "protocol.hpp"

#define PORT 8080
#define MAX_EVENTS 1024

// --- CẤU HÌNH THỬ NGHIỆM ---
// CHÚ Ý: Đổi số này để test các thuật toán: 1 = FIFO, 2 = Round Robin, 3 = Least Loaded
int SCHEDULER_MODE = 3; 
const int TOTAL_TEST_TASKS = 40; // Tổng số tác vụ chạy thử nghiệm

// --- ĐO LƯỜNG HIỆU NĂNG ---
auto exp_start_time = std::chrono::high_resolution_clock::now();
std::map<int, std::chrono::high_resolution_clock::time_point> task_arrival_times;
double total_response_time = 0;
int completed_tasks_count = 0;

// --- QUẢN LÝ ĐA LUỒNG & TRẠNG THÁI ---
std::mutex state_mutex;
std::queue<Packet> task_queue;
std::condition_variable cv;

std::map<int, int> worker_sockets;       // worker_id -> socket_fd
std::map<int, time_t> worker_heartbeats; // worker_id -> timestamp nhịp tim cuối
std::map<int, std::map<int, Packet>> worker_tasks; // worker_id -> (task_id -> Packet) dùng để cứu hộ cứu nạn Task

size_t rr_index = 0; // Biến đếm xoay vòng cho Round Robin

// --- LUỒNG LẬP LỊCH CHÍNH (SWITCH...CASE) ---
void scheduler_thread() {
    while (true) {
        std::unique_lock<std::mutex> lock(state_mutex);
        // Ngủ đông cho đến khi có Task trong hàng đợi VÀ có ít nhất 1 Worker trực tuyến
        cv.wait(lock, [] { return !task_queue.empty() && !worker_sockets.empty(); });
        
        int best_worker = -1;
        std::vector<int> active_workers;
        for (auto const& [w_id, _] : worker_sockets) {
            active_workers.push_back(w_id);
        }

        // Hiện thực hóa Mục 3.3 của đề bài qua cấu trúc switch...case
        switch (SCHEDULER_MODE) {
            case 1: // Thuật toán FIFO
                best_worker = active_workers.front();
                break;
                
            case 2: // Thuật toán Round Robin (Xoay vòng công bằng)
                best_worker = active_workers[rr_index % active_workers.size()];
                rr_index++;
                break;
                
            case 3: // Thuật toán Least Loaded (Tối ưu nhất - Ít việc nhất)
                size_t min_load = SIZE_MAX;
                for (int w_id : active_workers) {
                    if (worker_tasks[w_id].size() < min_load) {
                        min_load = worker_tasks[w_id].size();
                        best_worker = w_id;
                    }
                }
                break;
        }

        if (best_worker != -1) {
            Packet task = task_queue.front();
            task_queue.pop();
            
            // Đẩy việc qua mạng cho Worker
            send(worker_sockets[best_worker], &task, sizeof(Packet), 0);
            
            // Ghi nhận: Worker này đang xử lý Task này
            worker_tasks[best_worker][task.task_id] = task;
            
            std::cout << "[Scheduler] [Mode " << SCHEDULER_MODE << "] Sent Task " << task.task_id 
                      << " -> Worker " << best_worker << " (Load: " << worker_tasks[best_worker].size() << ")\n";
        }
        lock.unlock();
    }
}

// --- LUỒNG GIÁM SÁT NHỊP TIM & PHỤC HỒI LỖI (FAULT TOLERANCE) ---
void heartbeat_monitor() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::lock_guard<std::mutex> lock(state_mutex);
        
        time_t now = std::time(nullptr);
        for (auto it = worker_heartbeats.begin(); it != worker_heartbeats.end();) {
            if (now - it->second > 6) { // Nếu quá 6 giây mất tín hiệu nhịp tim
                int dead_id = it->first;
                std::cout << "\n[CRITICAL] Worker " << dead_id << " DETECTED DEAD (Timeout)!\n";
                
                close(worker_sockets[dead_id]);
                
                // Cơ chế phục hồi lỗi: Rút toàn bộ task chưa làm xong của worker chết ném trả lại hàng đợi
                for (auto const& [task_id, pkt] : worker_tasks[dead_id]) {
                    task_queue.push(pkt);
                    std::cout << "[Recovery] Rescued Task " << task_id << " -> Returned to READY queue.\n";
                }
                
                worker_sockets.erase(dead_id);
                worker_tasks.erase(dead_id);
                it = worker_heartbeats.erase(it);
                cv.notify_one(); // Đánh thức scheduler điều phối lại việc vừa cứu
            } else {
                ++it;
            }
        }
    }
}

// --- LUỒNG CHÍNH: EPOLL NETWORK EVENT LOOP ---
int main() {
    std::thread t_sched(scheduler_thread);
    std::thread t_heart(heartbeat_monitor);

    // Luồng tự động sinh tác vụ nặng để phục vụ thí nghiệm
    std::thread t_generator([]() {
        std::this_thread::sleep_for(std::chrono::seconds(4));
        std::lock_guard<std::mutex> lock(state_mutex);
        exp_start_time = std::chrono::high_resolution_clock::now();
        
        for(int i = 1; i <= TOTAL_TEST_TASKS; ++i) {
            Packet p{};
            p.type = TASK_SUBMIT; p.task_id = i;
            if (i % 2 == 0) {
                p.operation = PRIME_COUNTING; p.data = 400000; // Đếm số nguyên tố đến 400,000
            } else {
                p.operation = MONTE_CARLO_PI; p.data = 60000000; // 60 triệu mẫu thử Pi
            }
            task_queue.push(p);
            task_arrival_times[p.task_id] = std::chrono::high_resolution_clock::now();
        }
        std::cout << "[System] " << TOTAL_TEST_TASKS << " CPU-bound tasks pushed to queue.\n";
        cv.notify_all();
    });

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(PORT);
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, SOMAXCONN);

    int epoll_fd = epoll_create1(0);
    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN; ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::cout << "Master server listening using Linux Epoll. Mode configured: " << SCHEDULER_MODE << "\n";

    while (true) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int n = 0; n < nfds; ++n) {
            if (events[n].data.fd == server_fd) {
                int client_sock = accept(server_fd, nullptr, nullptr);
                ev.events = EPOLLIN; ev.data.fd = client_sock;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_sock, &ev);
            } else {
                int client_fd = events[n].data.fd;
                Packet pkt;
                ssize_t bytes = recv(client_fd, &pkt, sizeof(Packet), 0);
                
                if (bytes <= 0) {
                    close(client_fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
                    continue;
                }

                std::lock_guard<std::mutex> lock(state_mutex);
                if (pkt.type == REGISTER) {
                    worker_sockets[pkt.worker_id] = client_fd;
                    worker_heartbeats[pkt.worker_id] = std::time(nullptr);
                    std::cout << "[Network] Worker " << pkt.worker_id << " is online.\n";
                    cv.notify_one();
                } 
                else if (pkt.type == HEARTBEAT) {
                    worker_heartbeats[pkt.worker_id] = std::time(nullptr);
                } 
                else if (pkt.type == RESULT) {
                    worker_tasks[pkt.worker_id].erase(pkt.task_id);
                    completed_tasks_count++;
                    
                    auto now = std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double> resp_time = now - task_arrival_times[pkt.task_id];
                    total_response_time += resp_time.count();
                    
                    std::cout << "[Master] Progress: " << completed_tasks_count << "/" << TOTAL_TEST_TASKS 
                              << " | Task " << pkt.task_id << " resolved by Worker " << pkt.worker_id << "\n";
                    
                    if (completed_tasks_count == TOTAL_TEST_TASKS) {
                        std::chrono::duration<double> total_time = now - exp_start_time;
                        double avg_resp_time = total_response_time / TOTAL_TEST_TASKS;
                        
                        std::cout << "\n======= ALL TASKS FINISHED =======" << "\n";
                        std::cout << "Total Execution Time: " << total_time.count() << " seconds\n";
                        std::cout << "Average Response Time: " << avg_resp_time << " seconds\n";
                        std::cout << "==================================\n\n";
                        
                        std::ofstream file("experiment_results.csv", std::ios::app);
                        if (file.is_open()) {
                            file << SCHEDULER_MODE << "," << worker_sockets.size() << "," 
                                 << total_time.count() << "," << avg_resp_time << "\n";
                            file.close();
                            std::cout << "[Report] Appended metrics to experiment_results.csv\n";
                        }
                    }
                }
            }
        }
    }
    t_sched.join(); t_heart.join(); t_generator.join();
    return 0;
}
