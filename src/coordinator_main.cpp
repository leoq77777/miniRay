#include "miniray/cli.hpp"
#include "miniray/coordinator.hpp"
#include <iostream>

int main(int argc, char** argv) {
  try {
    miniray::Arguments args(argc, argv, {"listen", "policy", "max-pending", "heartbeat-timeout-ms"});
    miniray::CoordinatorOptions options;
    auto policy = args.Get("policy", "least-loaded");
    if (policy != "least-loaded" && policy != "round-robin") throw std::invalid_argument("policy must be least-loaded or round-robin");
    options.round_robin = policy == "round-robin";
    options.max_pending = args.Number("max-pending", 1024);
    options.heartbeat_timeout = std::chrono::milliseconds(args.Number("heartbeat-timeout-ms", 2500));
    miniray::CoordinatorService service(options);
    grpc::ServerBuilder builder;
    auto address = args.Get("listen", "0.0.0.0:50051");
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server) throw std::runtime_error("cannot listen on " + address);
    std::cout << "Coordinator ready " << address << " policy=" << policy << std::endl;
    miniray::WaitForSignal();
    service.Stop();
    server->Shutdown();
    server->Wait();
  } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
