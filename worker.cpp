#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <unistd.h>
#include <arpa/inet.h>
#include "protocol.hpp"

// Thuật toán 1: Vòng lặp kiểm tra và đếm số nguyên tố từ 2 đến N
uint64_t count_primes(uint64_t n) {
    uint64_t count = 0;
    for (uint64_t i = 2; i <= n; ++i) {
        bool is_prime = true;
        for (uint64_t j = 2; j * j <= i; ++j) {
            if (i % j == 0) { is_prime = false; break; }
        }
        if (is_prime) count++;
    }
    return count;
}

// Thuật toán 2: Ước lượng số Pi bằng cách gieo điểm ngẫu nhiên Monte Carlo
uint64_t monte_carlo_pi(uint64_t samples) {
    uint64_t inside_circle = 0;
    std::mt19937 gen(std::random_device{}());
    std::uniform_real_distribution<double> dis(0.0, 1.0);

    for (uint64_t i = 0; i < samples; ++i) {
        double x = dis(gen);
        double y = dis(gen);
        if (x * x + y * y <= 1.0) inside_circle++;
    }
    return inside_circle;
}

// Luồng ngầm tự động bắn tín hiệu Heartbeat định kỳ 2 giây/lần
void heartbeat_sender(int fd, int w_id) {
    Packet hb{}; hb.type = HEARTBEAT; hb.worker_id = w_id;
    while (true) {
        send(fd, &hb, sizeof(Packet), 0);
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

int main(int argc, char const *argv[]) {
    if (argc != 2) { std::cerr << "Usage: ./worker <worker_id>\n"; return 1; }
    int worker_id = std::stoi(argv[1]);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serv_addr{}; serv_addr.sin_family = AF_INET; serv_addr.sin_port = htons(8080);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Connection to Master failed.\n"; return -1;
    }

    // Đăng ký danh tính
    Packet reg{}; reg.type = REGISTER; reg.worker_id = worker_id;
    send(sock, &reg, sizeof(Packet), 0);

    // Kích hoạt luồng Heartbeat ngầm
    std::thread(heartbeat_sender, sock, worker_id).detach();
    std::cout << "Worker " << worker_id << " online and listening for compute tasks...\n";

    Packet task;
    while (recv(sock, &task, sizeof(Packet), 0) > 0) {
        if (task.type == TASK_SUBMIT) {
            std::cout << "--> Computing Task " << task.task_id << " (Type: " << task.operation << ")...\n";
            uint64_t out_data = 0;
            
            if (task.operation == PRIME_COUNTING) {
                out_data = count_primes(task.data);
            } else if (task.operation == MONTE_CARLO_PI) {
                out_data = monte_carlo_pi(task.data);
            }

            // Gửi trả kết quả
            Packet res{}; res.type = RESULT; res.worker_id = worker_id; res.task_id = task.task_id; res.data = out_data;
            send(sock, &res, sizeof(Packet), 0);
            std::cout << "    Task " << task.task_id << " done.\n";
        }
    }
    close(sock);
    return 0;
}
