#pragma once
#include "miniray.pb.h"
#include "miniray/common.hpp"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace miniray {

struct alignas(64) ShmHeader {
  static constexpr uint32_t kMagic = 0x4D524159u; // "MRAY"
  static constexpr uint32_t kVersion = 1u;
  uint32_t magic = kMagic;
  uint32_t version = kVersion;
  uint64_t payload_size = 0;
  uint32_t crc32 = 0;
  std::atomic<uint32_t> sealed{0}; // 0 = WRITING, 1 = SEALED
  uint32_t creator_pid = 0;
  uint8_t reserved[36]{};
};
static_assert(sizeof(ShmHeader) == 64, "ShmHeader must be 64-byte cacheline aligned");

// Immutable read-only view backed either by a zero-copy POSIX mmap region or an inline byte buffer.
class ObjectView {
 public:
  ObjectView() = default;
  static ObjectView FromInline(std::string data);
  static ObjectView FromShmMapping(void* mapped_base, size_t mapped_bytes, size_t payload_size);

  std::span<const std::byte> bytes() const noexcept {
    return ptr_ ? std::span<const std::byte>(ptr_, size_) : std::span<const std::byte>{};
  }
  std::string_view view() const noexcept {
    return ptr_ ? std::string_view(reinterpret_cast<const char*>(ptr_), size_) : std::string_view{};
  }
  size_t size() const noexcept { return size_; }
  bool empty() const noexcept { return size_ == 0; }
  bool is_zero_copy_shm() const noexcept { return zero_copy_shm_; }
  uintptr_t raw_address() const noexcept { return reinterpret_cast<uintptr_t>(ptr_); }

 private:
  std::shared_ptr<void> owner_;
  const std::byte* ptr_ = nullptr;
  size_t size_ = 0;
  bool zero_copy_shm_ = false;
};

class ObjectStore {
 public:
  using RemotePullFn = std::function<std::string(const rpc::ObjectRef&)>;

  explicit ObjectStore(std::string host_id = LocalHostId(), std::string manifest_path = DefaultManifestPath());
  ~ObjectStore();

  ObjectStore(const ObjectStore&) = delete;
  ObjectStore& operator=(const ObjectStore&) = delete;

  const std::string& host_id() const noexcept { return host_id_; }

  // Creates a sealed, immutable POSIX shared memory segment (< 31-byte name for Darwin/Linux compatibility).
  rpc::ObjectRef Put(const std::string& object_id, std::string_view payload, const std::string& owner_worker_id = "",
                     const std::string& owner_address = "");

  // Zero-copy read-only mmap directly from an existing POSIX shared memory segment.
  static ObjectView MapReadOnly(const std::string& shm_name, uint64_t expected_size, uint32_t expected_crc32,
                                bool verify_crc = false);

  // Resolves an ObjectRef: local store -> same-host zero-copy mmap -> cross-host Singleflight pull.
  ObjectView Resolve(const rpc::ObjectRef& ref, const RemotePullFn& pull_fn, bool* pulled_remotely = nullptr);

  // Reads a locally stored object (used by WorkerService::PullObject streaming server).
  std::optional<ObjectView> GetLocal(const std::string& object_id, uint32_t* out_crc32 = nullptr) const;

  bool Evict(const std::string& object_id);
  void EvictAll();

  // Scans the manifest file under advisory flock and unlinks segments left behind by SIGKILLed processes.
  static size_t SweepOrphans(const std::string& manifest_path = DefaultManifestPath());
  static std::string DefaultManifestPath();

  // Streams an object via WorkerService::PullObject from owner_address / replica_addresses with CRC32 verification.
  static std::string PullFromRemoteWorkers(const rpc::ObjectRef& ref,
                                           std::chrono::milliseconds timeout = std::chrono::seconds(30));

 private:
  struct LocalEntry {
    std::string shm_name;
    uint64_t size_bytes = 0;
    uint32_t crc32 = 0;
  };

  std::string MakeShmName(const std::string& object_id);
  void RecordManifest(const std::string& shm_name) const;

  std::string host_id_;
  std::string manifest_path_;
  uint64_t store_nonce_ = 0;
  std::atomic<uint64_t> seq_{0};
  mutable std::mutex mutex_;
  std::unordered_map<std::string, LocalEntry> local_objects_;
  std::unordered_map<std::string, std::shared_future<LocalEntry>> inflight_pulls_;
};

}  // namespace miniray
