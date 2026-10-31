#include "miniray/client.hpp"
#include <iostream>

int main(int argc, char** argv) {
  try {
    if (argc > 5) throw std::invalid_argument("usage: distributed-compute [coordinator] [task-type] [payload] [task-id]");
    miniray::MiniRayClient runtime(argc > 1 ? argv[1] : "localhost:50051");
    auto task = runtime.Submit(argc > 2 ? argv[2] : "fibonacci", argc > 3 ? argv[3] : "40", 2, true, argc > 4 ? argv[4] : "");
    std::cout << "submitted " << task.id() << std::endl;
    auto result = task.Wait(std::chrono::seconds(120));
    if (result.status() != miniray::rpc::TASK_STATUS_COMPLETED) throw std::runtime_error(result.error_message());
    std::cout << "result=" << result.result() << " attempt=" << result.attempt_id() << " worker=" << result.worker_id() << '\n';
  } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
