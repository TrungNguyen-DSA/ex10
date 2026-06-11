#ifndef PROTOCOL_HPP
#define PROTOCOL_HPP

#include <cstdint>

// Định nghĩa các loại thông điệp hệ thống
enum MessageType : uint8_t {
    REGISTER = 1,     // Worker đăng ký với Master
    TASK_SUBMIT = 2,  // Master giao việc cho Worker
    RESULT = 3,       // Worker trả kết quả cho Master
    HEARTBEAT = 4     // Worker báo hiệu còn sống
};

// Định nghĩa các loại thuật toán tính toán nặng (CPU-Intensive)
enum OpType : uint32_t {
    PRIME_COUNTING = 1, // Thuật toán đếm số nguyên tố từ 1 đến N
    MONTE_CARLO_PI = 2  // Thuật toán tính số Pi bằng phương pháp Monte Carlo
};

// Cấu trúc gói tin chuẩn gửi qua Socket (Cố định đúng 21 Bytes)
struct __attribute__((packed)) Packet {
    uint8_t type;         // Loại thông điệp (MessageType)
    uint32_t worker_id;   // ID của Worker liên quan
    uint32_t task_id;     // ID định danh duy nhất của tác vụ
    uint32_t operation;   // Phép toán cần xử lý (OpType)
    uint64_t data;        // Đầu vào (ví dụ số N) hoặc Đầu ra (Kết quả tính)
};

#endif
