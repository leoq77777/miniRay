#include "miniray/state.hpp"
#include "miniray/registry.hpp"
#include <gtest/gtest.h>
#include <future>
#include <thread>
using namespace miniray;
using namespace std::chrono_literals;
namespace {
rpc::SubmitTaskRequest Request(std::string id = "task", uint32_t retries = 2) {
  rpc::SubmitTaskRequest r;
  r.set_task_id(id); r.set_task_type("echo"); r.set_payload("hello"); r.set_retryable(true); r.set_max_retries(retries);
  return r;
}
void Submit(CoordinatorState& state, std::string id = "task", uint32_t retries = 2) {
  rpc::SubmitTaskResponse response;
  ASSERT_TRUE(state.Submit(Request(id, retries), response).ok());
  ASSERT_TRUE(response.accepted());
}
void Heartbeat(CoordinatorState& state, std::string id = "w", uint32_t capacity = 1, Clock::time_point now = Clock::now()) {
  rpc::HeartbeatRequest r; rpc::HeartbeatResponse s;
  r.set_worker_id(id); r.set_worker_address("localhost:50052"); r.set_capacity(capacity);
  ASSERT_TRUE(state.Heartbeat(r, s, now).ok());
}
rpc::GetTaskResultResponse Get(CoordinatorState& state, std::string id = "task") {
  rpc::GetTaskResultRequest r; rpc::GetTaskResultResponse s; r.set_task_id(id);
  EXPECT_TRUE(state.Get(r, s).ok()); return s;
}
bool Report(CoordinatorState& state, const Dispatch& d, rpc::TaskStatus status = rpc::TASK_STATUS_COMPLETED, bool retryable = false) {
  rpc::ReportResultRequest r; rpc::ReportResultResponse s;
  r.set_task_id(d.task.task_id()); r.set_attempt_id(d.task.attempt_id()); r.set_worker_id(d.worker_id);
  r.set_status(status); r.set_result("answer"); r.set_error_message("error"); r.set_retryable(retryable);
  state.Report(r, s); return s.accepted();
}
}
TEST(Registry, RegistrationValidationAndReentrantExecution) {
  TaskRegistry registry;
  EXPECT_THROW(registry.RegisterTask("", [](auto& x) { return x; }), std::invalid_argument);
  EXPECT_THROW(registry.Execute("missing", ""), std::invalid_argument);
  registry.RegisterTask("register", [&](const auto& x) {
    registry.RegisterTask("nested", [](const auto& input) { return input; }); return registry.Execute("nested", x);
  });
  EXPECT_EQ(registry.Execute("register", "ok"), "ok");
  RegisterBuiltins(registry);
  EXPECT_EQ(registry.Execute("fibonacci", "93"), "12200160415121876738");
  EXPECT_THROW(registry.Execute("fibonacci", "94"), std::invalid_argument);
  EXPECT_THROW(registry.Execute("sleep", "-1"), std::invalid_argument);
}
TEST(Scheduler, RoundRobinSkipsSaturatedAndDead) {
  RoundRobinScheduler scheduler;
  std::vector<WorkerNode> workers{{"a", "", 1, 0, true}, {"b", "", 2, 0, true}, {"c", "", 2, 0, false}};
  EXPECT_EQ(scheduler.SelectWorker(workers), "a");
  EXPECT_EQ(scheduler.SelectWorker(workers), "b");
  workers[0].active_tasks = 1;
  EXPECT_EQ(scheduler.SelectWorker(workers), "b");
  workers[1].active_tasks = 2;
  EXPECT_TRUE(scheduler.SelectWorker(workers).empty());
  EXPECT_TRUE(scheduler.SelectWorker({}).empty());
}
TEST(Scheduler, LeastLoadedNormalizesAndTiesById) {
  LeastLoadedScheduler scheduler;
  std::vector<WorkerNode> workers{{"b", "", 4, 1, true}, {"a", "", 2, 1, true}};
  EXPECT_EQ(scheduler.SelectWorker(workers), "b");
  workers[0].active_tasks = 2;
  EXPECT_EQ(scheduler.SelectWorker(workers), "a");
  workers[0].capacity = 0; workers[1].is_healthy = false;
  EXPECT_TRUE(scheduler.SelectWorker(workers).empty());
}
TEST(State, AdmissionIdempotencyAndCapacity) {
  CoordinatorState state({1, 100ms, false});
  Submit(state); Submit(state);
  rpc::SubmitTaskResponse response;
  EXPECT_EQ(state.Submit(Request("second"), response).error_code(), grpc::StatusCode::RESOURCE_EXHAUSTED);
  auto conflict = Request(); conflict.set_payload("different");
  EXPECT_EQ(state.Submit(conflict, response).error_code(), grpc::StatusCode::ALREADY_EXISTS);
  Heartbeat(state);
  auto dispatch = state.NextDispatch(); ASSERT_TRUE(dispatch);
  EXPECT_FALSE(state.NextDispatch());
  EXPECT_TRUE(Report(state, *dispatch));
  EXPECT_FALSE(Report(state, *dispatch));
  state.Dispatched(*dispatch, false);
  EXPECT_EQ(Get(state).status(), rpc::TASK_STATUS_COMPLETED);
  Submit(state, "second");
}
TEST(State, WorkerLossFencesStaleAndWrongWorkerResults) {
  CoordinatorState state({3, 100ms, false});
  auto now = Clock::now();
  Submit(state); Heartbeat(state, "a", 1, now);
  auto first = state.NextDispatch(); ASSERT_TRUE(first);
  state.Dispatched(*first, true);
  EXPECT_EQ(Get(state).status(), rpc::TASK_STATUS_RUNNING);
  state.Expire(now + 101ms);
  EXPECT_EQ(Get(state).status(), rpc::TASK_STATUS_PENDING);
  EXPECT_EQ(Get(state).attempt_id(), 1u);
  EXPECT_FALSE(Report(state, *first));
  Heartbeat(state, "b", 1, now + 102ms);
  auto second = state.NextDispatch(); ASSERT_TRUE(second);
  auto wrong = *second; wrong.worker_id = "impostor";
  EXPECT_FALSE(Report(state, wrong));
  auto future = *second; future.task.set_attempt_id(99);
  EXPECT_FALSE(Report(state, future));
  EXPECT_TRUE(Report(state, *second));
  EXPECT_FALSE(Report(state, *first));
  EXPECT_EQ(Get(state).result(), "answer");
}
TEST(State, RetryBudgetAndNonRetryableTasks) {
  CoordinatorState state;
  Submit(state, "task", 1); Heartbeat(state);
  auto first = state.NextDispatch(); ASSERT_TRUE(first);
  state.Dispatched(*first, false);
  Heartbeat(state, "replacement");
  auto second = state.NextDispatch(); ASSERT_TRUE(second);
  EXPECT_EQ(second->task.attempt_id(), 1u);
  EXPECT_TRUE(Report(state, *second, rpc::TASK_STATUS_FAILED, true));
  EXPECT_EQ(Get(state).status(), rpc::TASK_STATUS_FAILED);
  EXPECT_FALSE(state.NextDispatch());
  rpc::SubmitTaskResponse s;
  auto request = Request("no-retry"); request.set_retryable(false);
  ASSERT_TRUE(state.Submit(request, s).ok());
  auto d = state.NextDispatch(); ASSERT_TRUE(d);
  state.Dispatched(*d, false);
  EXPECT_EQ(Get(state, "no-retry").status(), rpc::TASK_STATUS_FAILED);
}
TEST(State, ApplicationFailureCanBeTerminal) {
  CoordinatorState state; Submit(state); Heartbeat(state);
  auto d = state.NextDispatch(); ASSERT_TRUE(d);
  EXPECT_TRUE(Report(state, *d, rpc::TASK_STATUS_FAILED));
  EXPECT_EQ(Get(state).status(), rpc::TASK_STATUS_FAILED);
  EXPECT_FALSE(state.NextDispatch());
}
TEST(State, HeartbeatDoesNotEraseReservationsAndDeadIncarnationCannotRejoin) {
  CoordinatorState state({4, 100ms, false});
  auto now = Clock::now(); Submit(state); Submit(state, "two"); Heartbeat(state, "a", 1, now);
  ASSERT_TRUE(state.NextDispatch());
  Heartbeat(state, "a", 1, now + 10ms);
  EXPECT_FALSE(state.NextDispatch());
  state.Expire(now + 111ms);
  rpc::HeartbeatRequest r; rpc::HeartbeatResponse s;
  r.set_worker_id("a"); r.set_worker_address("localhost:50052"); r.set_capacity(1);
  EXPECT_EQ(state.Heartbeat(r, s).error_code(), grpc::StatusCode::FAILED_PRECONDITION);
}
TEST(State, ConcurrentAdmissionsAreBoundedAndShutdownWakesWaiters) {
  CoordinatorState state({8, 100ms, false});
  std::atomic<int> accepted{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 32; ++i) threads.emplace_back([&, i] {
    rpc::SubmitTaskResponse response;
    if (state.Submit(Request(std::to_string(i)), response).ok()) ++accepted;
  });
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(accepted, 8);
  state.Stop();
  rpc::SubmitTaskResponse response;
  EXPECT_EQ(state.Submit(Request("new"), response).error_code(), grpc::StatusCode::UNAVAILABLE);
}
TEST(State, ValidationAndUnknownTask) {
  CoordinatorState state;
  rpc::SubmitTaskRequest r; rpc::SubmitTaskResponse s;
  EXPECT_EQ(state.Submit(r, s).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  rpc::GetTaskResultRequest get; rpc::GetTaskResultResponse result;
  EXPECT_EQ(state.Get(get, result).error_code(), grpc::StatusCode::NOT_FOUND);
}
TEST(State, RecoveryReservationsRemainBoundedWhenAllWorkersFail) {
  CoordinatorState state({4, 100ms, false});
  auto now = Clock::now();
  Heartbeat(state, "a", 2, now); Heartbeat(state, "b", 2, now);
  for (int i = 0; i < 4; ++i) Submit(state, std::to_string(i));
  for (int i = 0; i < 4; ++i) ASSERT_TRUE(state.NextDispatch());
  EXPECT_FALSE(state.NextDispatch());
  state.Expire(now + 101ms);
  rpc::SubmitTaskResponse response;
  EXPECT_EQ(state.Submit(Request("overflow"), response).error_code(), grpc::StatusCode::RESOURCE_EXHAUSTED);
  Heartbeat(state, "replacement", 2);
  for (int i = 0; i < 4; ++i) {
    auto d = state.NextDispatch(); ASSERT_TRUE(d);
    EXPECT_EQ(d->task.attempt_id(), 1u); EXPECT_TRUE(Report(state, *d));
  }
  for (int i = 0; i < 4; ++i) EXPECT_EQ(Get(state, std::to_string(i)).status(), rpc::TASK_STATUS_COMPLETED);
}
TEST(State, FailedDispatchUsesAnotherWorker) {
  CoordinatorState state({4, 100ms, false});
  Heartbeat(state, "a", 2); Heartbeat(state, "b", 1);
  Submit(state, "first"); Submit(state, "second"); Submit(state, "third");
  auto first = state.NextDispatch(); ASSERT_TRUE(first); EXPECT_EQ(first->worker_id, "a");
  auto second = state.NextDispatch(); ASSERT_TRUE(second); EXPECT_EQ(second->worker_id, "b");
  state.Dispatched(*first, false);
  EXPECT_TRUE(Report(state, *second));
  auto third = state.NextDispatch(); ASSERT_TRUE(third);
  EXPECT_EQ(third->worker_id, "b");
  EXPECT_TRUE(Report(state, *third));
  auto recovered = state.NextDispatch(); ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered->worker_id, "b");
  EXPECT_EQ(recovered->task.attempt_id(), 1u);
  EXPECT_TRUE(Report(state, *recovered));
  EXPECT_FALSE(Report(state, *first));
}

TEST(State, FailedDispatchPreservesOtherActiveAttempts) {
  CoordinatorState state;
  Heartbeat(state, "a", 2);
  Submit(state, "first"); Submit(state, "second");
  auto first = state.NextDispatch(); ASSERT_TRUE(first);
  auto second = state.NextDispatch(); ASSERT_TRUE(second);
  state.Dispatched(*first, false);
  EXPECT_FALSE(state.NextDispatch());
  EXPECT_TRUE(Report(state, *second));
  EXPECT_EQ(Get(state, "second").status(), rpc::TASK_STATUS_COMPLETED);
  Heartbeat(state, "replacement");
  auto retry = state.NextDispatch(); ASSERT_TRUE(retry);
  EXPECT_EQ(retry->worker_id, "replacement");
  EXPECT_TRUE(Report(state, *retry));
}

TEST(State, RecoveryQueuePreemptsQueuedBacklogOnWorkerCrash) {
  CoordinatorState state({16, 100ms, false});
  auto now = Clock::now();
  Heartbeat(state, "a", 1, now);
  Heartbeat(state, "b", 1, now);
  for (int i = 0; i < 8; ++i) {
    rpc::SubmitTaskResponse s;
    ASSERT_TRUE(state.Submit(Request("t" + std::to_string(i)), s, now + std::chrono::milliseconds(i)).ok());
  }
  auto d0 = state.NextDispatch(now + 10ms); ASSERT_TRUE(d0); EXPECT_EQ(d0->task.task_id(), "t0");
  auto d1 = state.NextDispatch(now + 10ms); ASSERT_TRUE(d1); EXPECT_EQ(d1->task.task_id(), "t1");
  state.Dispatched(*d0, true, now + 10ms);
  state.Dispatched(*d1, true, now + 10ms);
  Heartbeat(state, "b", 1, now + 80ms);
  state.Expire(now + 105ms);
  EXPECT_EQ(Get(state, "t0").status(), rpc::TASK_STATUS_PENDING);
  EXPECT_EQ(Get(state, "t0").attempt_id(), 1u);
  EXPECT_TRUE(Report(state, *d1));
  auto preempted = state.NextDispatch(now + 106ms);
  ASSERT_TRUE(preempted);
  EXPECT_EQ(preempted->task.task_id(), "t0");
  EXPECT_EQ(preempted->task.attempt_id(), 1u);
  EXPECT_EQ(preempted->worker_id, "b");
}

TEST(State, FastQueueMitigatesHeadOfLineWithAgingAntiStarvation) {
  CoordinatorOptions opts;
  opts.fast_task_threshold = 10ms;
  opts.aging_threshold = 50ms;
  CoordinatorState state(opts);
  auto now = Clock::now();
  Heartbeat(state, "w", 1, now);
  auto submit_typed = [&](const std::string& id, const std::string& type, Clock::time_point t) {
    rpc::SubmitTaskRequest req = Request(id);
    req.set_task_type(type);
    rpc::SubmitTaskResponse resp;
    ASSERT_TRUE(state.Submit(req, resp, t).ok());
  };
  auto report_exec = [&](const Dispatch& d, double exec_ms, Clock::time_point t) {
    rpc::ReportResultRequest r; rpc::ReportResultResponse s;
    r.set_task_id(d.task.task_id()); r.set_attempt_id(d.task.attempt_id()); r.set_worker_id(d.worker_id);
    r.set_status(rpc::TASK_STATUS_COMPLETED); r.set_result("ok"); r.set_execution_ms(exec_ms);
    state.Report(r, s, t);
    ASSERT_TRUE(s.accepted());
  };
  submit_typed("warm-fast", "fast_op", now);
  auto wf = state.NextDispatch(now); ASSERT_TRUE(wf);
  report_exec(*wf, 2.0, now + 2ms);
  submit_typed("warm-slow", "slow_op", now + 3ms);
  auto ws = state.NextDispatch(now + 3ms); ASSERT_TRUE(ws);
  report_exec(*ws, 40.0, now + 43ms);

  // Occupy the single worker slot so subsequent submissions queue up.
  submit_typed("holder", "fast_op", now + 50ms);
  auto holder = state.NextDispatch(now + 50ms); ASSERT_TRUE(holder);

  submit_typed("slow-1", "slow_op", now + 51ms);
  submit_typed("fast-1", "fast_op", now + 52ms);
  report_exec(*holder, 2.0, now + 55ms);

  // Before aging_threshold (50ms), fast-1 bypasses slow-1.
  auto next1 = state.NextDispatch(now + 55ms); ASSERT_TRUE(next1);
  EXPECT_EQ(next1->task.task_id(), "fast-1");

  // Once slow-1 has waited >= 50ms (51ms -> 105ms), aging promotes slow-1 ahead of newly submitted fast-2.
  submit_typed("fast-2", "fast_op", now + 100ms);
  report_exec(*next1, 2.0, now + 105ms);
  auto next2 = state.NextDispatch(now + 105ms); ASSERT_TRUE(next2);
  EXPECT_EQ(next2->task.task_id(), "slow-1");
}

TEST(State, SpeculativeExecutionRacesStragglerFencesLoserAndConservesSlots) {
  CoordinatorOptions opts;
  opts.max_pending = 8;
  opts.heartbeat_timeout = 500ms;
  opts.enable_speculation = true;
  opts.min_profile_samples = 4;
  opts.min_speculation_delay = 10ms;
  opts.speculation_multiplier = 1.5;
  CoordinatorState state(opts);
  auto now = Clock::now();
  Heartbeat(state, "a", 1, now);
  Heartbeat(state, "b", 1, now);

  // Train profile on 4 fast completions (4ms each -> P95 = 4ms, threshold = max(10ms, 6ms) = 10ms).
  for (int i = 0; i < 4; ++i) {
    rpc::SubmitTaskResponse resp;
    ASSERT_TRUE(state.Submit(Request("warm-" + std::to_string(i)), resp, now).ok());
    auto d = state.NextDispatch(now); ASSERT_TRUE(d);
    rpc::ReportResultRequest r; rpc::ReportResultResponse s;
    r.set_task_id(d->task.task_id()); r.set_attempt_id(d->task.attempt_id()); r.set_worker_id(d->worker_id);
    r.set_status(rpc::TASK_STATUS_COMPLETED); r.set_result("warm"); r.set_execution_ms(4.0);
    state.Report(r, s, now + 4ms);
    ASSERT_TRUE(s.accepted());
  }

  now += 20ms;
  rpc::SubmitTaskResponse resp;
  ASSERT_TRUE(state.Submit(Request("hedged-task"), resp, now).ok());
  auto orig = state.NextDispatch(now); ASSERT_TRUE(orig);
  EXPECT_EQ(orig->worker_id, "a");
  EXPECT_EQ(orig->task.attempt_id(), 0u);
  state.Dispatched(*orig, true, now);

  // Too early for speculation at +5ms.
  EXPECT_FALSE(state.NextDispatch(now + 5ms));

  // At +12ms (>= 10ms threshold) with empty queue, launches speculative replica on anti-affine worker "b".
  auto hedge = state.NextDispatch(now + 12ms);
  ASSERT_TRUE(hedge);
  EXPECT_EQ(hedge->task.task_id(), "hedged-task");
  EXPECT_EQ(hedge->task.attempt_id(), 1u);
  EXPECT_EQ(hedge->worker_id, "b");
  state.Dispatched(*hedge, true, now + 12ms);
  // No 3rd replica is spawned.
  EXPECT_FALSE(state.NextDispatch(now + 30ms));

  // Hedge replica on "b" finishes first!
  {
    rpc::ReportResultRequest r; rpc::ReportResultResponse s;
    r.set_task_id("hedged-task"); r.set_attempt_id(1); r.set_worker_id("b");
    r.set_status(rpc::TASK_STATUS_COMPLETED); r.set_result("fast-winner"); r.set_execution_ms(4.0);
    state.Report(r, s, now + 16ms);
    EXPECT_TRUE(s.accepted());
  }
  auto cancels = state.DrainCancels();
  ASSERT_EQ(cancels.size(), 1u);
  EXPECT_EQ(cancels[0].task_id, "hedged-task");
  EXPECT_EQ(cancels[0].attempt_id, 0u);
  EXPECT_EQ(cancels[0].worker_id, "a");

  // Worker "a"'s slot stays reserved while its running handler drains (slot_freed = false).
  state.CancelAck(cancels[0], false);
  ASSERT_TRUE(state.Submit(Request("follow-1"), resp, now + 17ms).ok());
  ASSERT_TRUE(state.Submit(Request("follow-2"), resp, now + 17ms).ok());
  auto f1 = state.NextDispatch(now + 17ms); ASSERT_TRUE(f1);
  EXPECT_EQ(f1->worker_id, "b");
  EXPECT_FALSE(state.NextDispatch(now + 17ms)); // "a" still draining cancelled attempt 0!

  // Late report from "a" is fenced (accepted=false), preserves winner result, and releases "a"'s slot.
  {
    rpc::ReportResultRequest r; rpc::ReportResultResponse s;
    r.set_task_id("hedged-task"); r.set_attempt_id(0); r.set_worker_id("a");
    r.set_status(rpc::TASK_STATUS_COMPLETED); r.set_result("stale-loser"); r.set_execution_ms(50.0);
    state.Report(r, s, now + 50ms);
    EXPECT_FALSE(s.accepted());
  }
  EXPECT_EQ(Get(state, "hedged-task").result(), "fast-winner");
  EXPECT_EQ(Get(state, "hedged-task").attempt_id(), 1u);
  auto f2 = state.NextDispatch(now + 51ms); ASSERT_TRUE(f2);
  EXPECT_EQ(f2->worker_id, "a");
}

TEST(State, SpeculativeSiblingSurvivesSingleWorkerExpiry) {
  CoordinatorOptions opts;
  opts.heartbeat_timeout = 100ms;
  opts.enable_speculation = true;
  opts.min_profile_samples = 2;
  opts.min_speculation_delay = 10ms;
  CoordinatorState state(opts);
  auto now = Clock::now();
  Heartbeat(state, "a", 1, now);
  Heartbeat(state, "b", 1, now);
  for (int i = 0; i < 2; ++i) {
    rpc::SubmitTaskResponse resp;
    ASSERT_TRUE(state.Submit(Request("w" + std::to_string(i)), resp, now).ok());
    auto d = state.NextDispatch(now); ASSERT_TRUE(d);
    rpc::ReportResultRequest r; rpc::ReportResultResponse s;
    r.set_task_id(d->task.task_id()); r.set_attempt_id(d->task.attempt_id()); r.set_worker_id(d->worker_id);
    r.set_status(rpc::TASK_STATUS_COMPLETED); r.set_result("ok"); r.set_execution_ms(3.0);
    state.Report(r, s, now + 3ms);
  }
  now += 10ms;
  rpc::SubmitTaskResponse resp;
  ASSERT_TRUE(state.Submit(Request("race"), resp, now).ok());
  auto orig = state.NextDispatch(now); ASSERT_TRUE(orig); EXPECT_EQ(orig->worker_id, "a");
  state.Dispatched(*orig, true, now);
  auto hedge = state.NextDispatch(now + 15ms); ASSERT_TRUE(hedge); EXPECT_EQ(hedge->worker_id, "b");
  state.Dispatched(*hedge, true, now + 15ms);

  // Refresh "b", let "a" expire: task must stay RUNNING on "b" without re-entering pending queue.
  Heartbeat(state, "b", 1, now + 80ms);
  state.Expire(now + 105ms);
  EXPECT_EQ(Get(state, "race").status(), rpc::TASK_STATUS_RUNNING);
  EXPECT_EQ(Get(state, "race").worker_id(), "b");
  EXPECT_FALSE(state.NextDispatch(now + 106ms));
  EXPECT_TRUE(Report(state, *hedge));
  EXPECT_EQ(Get(state, "race").status(), rpc::TASK_STATUS_COMPLETED);
}
