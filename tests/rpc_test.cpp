#include "miniray/coordinator.hpp"
#include "miniray/client.hpp"
#include "miniray/worker.hpp"
#include <gtest/gtest.h>
#include <future>
using namespace miniray;
using namespace std::chrono_literals;
namespace {
std::unique_ptr<grpc::Server> Serve(grpc::Service& service, int& port) {
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  builder.RegisterService(&service);
  return builder.BuildAndStart();
}
std::string Address(int port) { return "127.0.0.1:" + std::to_string(port); }
}
TEST(Rpc, ExecutionExceptionsBackpressureAndClientTimeout) {
  CoordinatorService coordinator({16, 800ms, false});
  int cp = 0; auto server = Serve(coordinator, cp); ASSERT_TRUE(server);
  TaskRegistry registry; RegisterBuiltins(registry);
  std::atomic<int> executing{0}, peak{0}, calls{0};
  registry.RegisterTask("checked", [&](const std::string& input) {
    int n = ++executing;
    int p = peak.load(); while (n > p && !peak.compare_exchange_weak(p, n)) {}
    std::this_thread::sleep_for(30ms);
    --executing; ++calls; return input;
  });
  registry.RegisterTask("throw", [](const std::string&) -> std::string { throw std::runtime_error("boom"); });
  WorkerOptions options; options.coordinator = Address(cp); options.capacity = 2; options.heartbeat_interval = 50ms;
  WorkerService actual(options, registry);
  int wp = 0; auto ws = Serve(actual, wp); ASSERT_TRUE(ws);
  actual.Start(Address(wp));
  MiniRayClient client(Address(cp));
  std::vector<TaskFuture> tasks;
  for (int i = 0; i < 12; ++i) tasks.push_back(client.Submit("checked", std::to_string(i)));
  for (int i = 0; i < 12; ++i) EXPECT_EQ(tasks[i].Get(5s), std::to_string(i));
  EXPECT_LE(peak, 2); EXPECT_EQ(calls, 12);
  EXPECT_EQ(client.Submit("fibonacci", "40").Get(), "102334155");
  auto poison = client.Submit("throw", "", 1).Wait();
  EXPECT_EQ(poison.status(), rpc::TASK_STATUS_FAILED); EXPECT_EQ(poison.attempt_id(), 1u);
  auto unknown = client.Submit("missing", "").Wait();
  EXPECT_EQ(unknown.status(), rpc::TASK_STATUS_FAILED); EXPECT_EQ(unknown.attempt_id(), 0u);
  auto slow = client.Submit("sleep", "100");
  EXPECT_THROW(slow.Get(5ms), RpcError);
  EXPECT_EQ(client.Lookup(slow.id()).Get(3s), "100");
  EXPECT_THROW(client.Lookup("does-not-exist").Get(), RpcError);
  auto stable = client.Submit("echo", "stable", 2, true, "stable-id");
  EXPECT_EQ(client.Submit("echo", "stable", 2, true, "stable-id").id(), stable.id());
  EXPECT_EQ(stable.Get(), "stable");
  actual.Stop(); ws->Shutdown(); ws->Wait();
  coordinator.Stop(); server->Shutdown(); server->Wait();
}
TEST(Rpc, WorkerRejectsExcessAndDeduplicatesDispatch) {
  TaskRegistry registry; RegisterBuiltins(registry);
  std::promise<void> entered, release;
  auto unblock = release.get_future().share();
  std::atomic<int> calls{0};
  registry.RegisterTask("block", [&](const std::string&) { ++calls; entered.set_value(); unblock.wait(); return "ok"; });
  CoordinatorService coordinator;
  int cp = 0; auto cs = Serve(coordinator, cp); ASSERT_TRUE(cs);
  WorkerOptions options; options.coordinator = Address(cp); options.capacity = 1;
  WorkerService worker(options, registry); worker.Start();
  rpc::DispatchTaskRequest r; rpc::DispatchTaskResponse s;
  r.mutable_task()->set_task_id("one"); r.mutable_task()->set_task_type("block");
  ASSERT_TRUE(worker.DispatchTask(nullptr, &r, &s).ok()); EXPECT_TRUE(s.accepted());
  entered.get_future().wait();
  s.Clear(); worker.DispatchTask(nullptr, &r, &s); EXPECT_TRUE(s.accepted());
  r.mutable_task()->set_task_id("two"); s.Clear(); worker.DispatchTask(nullptr, &r, &s); EXPECT_FALSE(s.accepted());
  release.set_value(); worker.Stop(); EXPECT_EQ(calls, 1);
  coordinator.Stop(); cs->Shutdown(); cs->Wait();
}
TEST(Rpc, BackpressureAndShutdownWakeLongPoll) {
  CoordinatorService coordinator({1, 800ms, true});
  int port = 0; auto server = Serve(coordinator, port); ASSERT_TRUE(server);
  MiniRayClient client(Address(port));
  auto task = client.Submit("echo", "waiting");
  try {
    client.Submit("echo", "overflow");
    FAIL() << "expected admission backpressure";
  } catch (const RpcError& error) {
    EXPECT_EQ(error.code, grpc::StatusCode::RESOURCE_EXHAUSTED);
  }
  auto waiting = std::async(std::launch::async, [&] { return task.Wait(3s); });
  coordinator.Stop();
  ASSERT_EQ(waiting.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(waiting.get().status(), rpc::TASK_STATUS_FAILED);
  server->Shutdown(); server->Wait();
}
namespace {
class ReportingCoordinator final : public rpc::CoordinatorService::Service {
 public:
  explicit ReportingCoordinator(int failures) : failures_(failures) {}
  std::atomic<int> reports{0};
  grpc::Status SendHeartbeat(grpc::ServerContext*, const rpc::HeartbeatRequest*, rpc::HeartbeatResponse* response) override {
    response->set_ack(true); return grpc::Status::OK;
  }
  grpc::Status ReportResult(grpc::ServerContext*, const rpc::ReportResultRequest*, rpc::ReportResultResponse* response) override {
    if (++reports <= failures_) return {grpc::StatusCode::UNAVAILABLE, "injected reporting failure"};
    response->set_accepted(true); return grpc::Status::OK;
  }
 private:
  int failures_;
};
}
TEST(Rpc, ReportingRetriesAndPersistentFailureIsolatesWorker) {
  for (int failures : {2, 100}) {
    ReportingCoordinator coordinator(failures);
    int cp = 0; auto cs = Serve(coordinator, cp); ASSERT_TRUE(cs);
    TaskRegistry registry;
    std::atomic<int> calls{0};
    registry.RegisterTask("counted", [&](const auto& input) { ++calls; return input; });
    WorkerOptions options; options.coordinator = Address(cp); options.capacity = 1; options.heartbeat_interval = 20ms;
    WorkerService worker(options, registry); worker.Start();
    rpc::DispatchTaskRequest request; rpc::DispatchTaskResponse response;
    request.mutable_task()->set_task_id("one"); request.mutable_task()->set_task_type("counted");
    worker.DispatchTask(nullptr, &request, &response); EXPECT_TRUE(response.accepted());
    auto end = Clock::now() + 5s;
    int expected = failures == 2 ? 3 : 10;
    while (coordinator.reports < expected && Clock::now() < end) std::this_thread::sleep_for(10ms);
    EXPECT_EQ(coordinator.reports, expected);
    // The last failed reporting call has one final 100ms backoff before isolation.
    if (failures == 100) {
      std::this_thread::sleep_for(150ms);
      request.mutable_task()->set_task_id("two"); response.Clear();
      worker.DispatchTask(nullptr, &request, &response); EXPECT_FALSE(response.accepted());
    }
    worker.Stop(); EXPECT_EQ(calls, 1);
    cs->Shutdown(); cs->Wait();
  }
}
