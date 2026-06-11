#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <vector>
#include <unistd.h>
#include <arpa/inet.h>
#include "protocol.hpp"

// Task A: Đếm số nguyên tố từ 2 đến N
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

// Task B: Nhân ma trận vuông kích thước N x N (Giả lập tính toán nặng)
uint64_t matrix_multiplication(uint64_t n) {
    if (n > 500) n = 500; // Giới hạn tránh tràn bộ nhớ
    std::vector<std::vector<int>> A(n, std::vector<int>(n, 1));
    std::vector<std::vector<int>> B(n, std::vector<int>(n, 2));
    std::vector<std::vector<int>> C(n, std::vector<int>(n, 0));

    for (uint64_t i = 0; i < n; ++i) {
        for (uint64_t j = 0; j < n; ++j) {
            for (uint64_t k = 0; k < n; ++k) {
                C[i][j] += A[i][k] * B[k][j];
            }
        }
    }
    return C[0][0]; // Trả về 1 phần tử đại diện để kiểm tra tính chính xác
}

// Task C: Tính số Pi bằng Monte Carlo
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

// Task D: Giả lập Word Count trên một văn bản ảo có số lượng từ tương đương N
uint64_t word_count(uint64_t virtual_words) {
    uint64_t hash_sum = 0;
    // Giả lập chuỗi từ lặp đi lặp lại để đẩy CPU tính toán chuỗi toán học nặng
    std::string mock_word = "DistributedSystemOperatingSystem";
    for (uint64_t i = 0; i < virtual_words; ++i) {
        for (char c : mock_word) {
            hash_sum += c + (i % 7);
        }
    }
    return hash_sum; // Trả về kết quả băm từ làm checksum
}

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
        std::cerr << "Master offline.\n"; return -1;
    }

    Packet reg{}; reg.type = REGISTER; reg.worker_id = worker_id;
    send(sock, &reg, sizeof(Packet), 0);

    std::thread(heartbeat_sender, sock, worker_id).detach();
    std::cout << "Worker " << worker_id << " online.\n";

    Packet task;
    while (recv(sock, &task, sizeof(Packet), 0) > 0) {
        if (task.type == TASK_SUBMIT) {
            std::cout << "--> Computing Task " << task.task_id << " (OpType: " << task.operation << ")...\n";
            uint64_t out_val = 0;

            // Rẽ nhánh xử lý 4 thuật toán dựa trên pkt.operation
            if (task.operation == PRIME_COUNTING) {
                out_val = count_primes(task.data);
            } else if (task.operation == MATRIX_MULTIPLICATION) {
                out_val = matrix_multiplication(task.data);
            } else if (task.operation == MONTE_CARLO_PI) {
                out_val = monte_carlo_pi(task.data);
            } else if (task.WORD_COUNT) {
                out_val = word_count(task.data);
            }

            Packet res{}; res.type = RESULT; res.worker_id = worker_id; res.task_id = task.task_id; res.data = out_val;
            send(sock, &res, sizeof(Packet), 0);
        }
    }
    close(sock);
    return 0;
}
