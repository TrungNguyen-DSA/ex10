#ifndef PROTOCOL_HPP
#define PROTOCOL_HPP

#include <cstdint>

enum MessageType : uint8_t {
    REGISTER = 1,
    TASK_SUBMIT = 2,
    RESULT = 3,
    HEARTBEAT = 4
};

// ĐẦY ĐỦ 4 LOẠI TASK THEO ĐỀ BÀI
enum OpType : uint32_t {
    PRIME_COUNTING = 1,       // Task A
    MATRIX_MULTIPLICATION = 2, // Task B
    MONTE_CARLO_PI = 3,        // Task C
    WORD_COUNT = 4             // Task D
};

struct __attribute__((packed)) Packet {
    uint8_t type;         
    uint32_t worker_id;   
    uint32_t task_id;     
    uint32_t operation;   // Ăn theo OpType (1, 2, 3, hoặc 4)
    uint64_t data;        // Tham số đầu vào (N, Kích thước ma trận, Số mẫu...) hoặc Kết quả số
};

#endif
