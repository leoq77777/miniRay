#include "miniray/cli.hpp"
#include "miniray/worker.hpp"
#include <iostream>

int main(int argc, char** argv) {
  try {
    miniray::Arguments args(argc, argv, {"coordinator", "listen", "advertise", "capacity", "heartbeat-ms"});
    miniray::WorkerOptions options;
    options.coordinator = args.Get("coordinator", "localhost:50051");
    auto listen = args.Get("listen", "0.0.0.0:50052");
    options.address = args.Get("advertise", "localhost:50052");
    options.capacity = args.Number("capacity", 2);
    options.heartbeat_interval = std::chrono::milliseconds(args.Number("heartbeat-ms", 500));
    miniray::TaskRegistry registry;
    miniray::RegisterBuiltins(registry);
    miniray::WorkerService service(options, registry);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server) throw std::runtime_error("cannot listen on " + listen);
    service.Start();
    std::cout << "Worker ready " << service.id() << " address=" << options.address << " capacity=" << options.capacity << std::endl;
    miniray::WaitForSignal();
    service.Stop();
    server->Shutdown();
    server->Wait();
  } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
