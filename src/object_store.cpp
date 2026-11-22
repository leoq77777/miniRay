#include "miniray/object_store.hpp"
#include "miniray.grpc.pb.h"
#include <cerrno>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <iomanip>
#include <sstream>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace miniray {
namespace {
struct ShmUnmapper {
  void* base = nullptr;
  size_t len = 0;
  ShmUnmapper(void* b, size_t l) noexcept : base(b), len(l) {}
  ShmUnmapper(const ShmUnmapper&) = delete;
  ShmUnmapper& operator=(const ShmUnmapper&) = delete;
  ~ShmUnmapper() {
    if (base && base != MAP_FAILED && len > 0) ::munmap(base, len);
  }
};

uint64_t Fnv1a64(std::string_view data, uint64_t seed = 14695981039346656037ull) noexcept {
  uint64_t h = seed;
  for (unsigned char c : data) {
    h ^= c;
    h *= 1099511628211ull;
  }
  return h;
}
}  // namespace

ObjectView ObjectView::FromInline(std::string data) {
  ObjectView v;
  auto holder = std::make_shared<std::string>(std::move(data));
  v.ptr_ = reinterpret_cast<const std::byte*>(holder->data());
  v.size_ = holder->size();
  v.owner_ = std::move(holder);
  v.zero_copy_shm_ = false;
  return v;
}

ObjectView ObjectView::FromShmMapping(void* mapped_base, size_t mapped_bytes, size_t payload_size) {
  ObjectView v;
  auto holder = std::make_shared<ShmUnmapper>(mapped_base, mapped_bytes);
  v.ptr_ = static_cast<const std::byte*>(mapped_base) + sizeof(ShmHeader);
  v.size_ = payload_size;
  v.owner_ = std::move(holder);
  v.zero_copy_shm_ = true;
  return v;
}

std::string ObjectStore::DefaultManifestPath() {
  return "/tmp/miniray_shm_manifest_" + std::to_string(static_cast<uint64_t>(::getuid())) + ".log";
}

ObjectStore::ObjectStore(std::string host_id, std::string manifest_path)
    : host_id_(std::move(host_id)), manifest_path_(std::move(manifest_path)) {
  store_nonce_ = Fnv1a64(UniqueId()) ^ static_cast<uint64_t>(::getpid());
}

ObjectStore::~ObjectStore() { EvictAll(); }

// Produces "/mr_" + 24 hex chars = 28 chars (< 31-byte PSHMNAMLEN limit on macOS Darwin).
std::string ObjectStore::MakeShmName(const std::string& object_id) {
  uint64_t s = ++seq_;
  uint64_t h1 = Fnv1a64(object_id, store_nonce_);
  uint64_t h2 = Fnv1a64(std::to_string(s), h1 ^ 0x9E3779B97F4A7C15ull);
  std::ostringstream out;
  out << "/mr_" << std::hex << std::setfill('0') << std::setw(16) << h1 << std::setw(8)
      << static_cast<uint32_t>(h2 & 0xFFFFFFFFu);
  return out.str();
}

void ObjectStore::RecordManifest(const std::string& shm_name) const {
  if (manifest_path_.empty()) return;
  int fd = ::open(manifest_path_.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0600);
  if (fd < 0) return;
  (void)::flock(fd, LOCK_EX);
  std::string line = std::to_string(static_cast<uint64_t>(::getpid())) + " " + shm_name + "\n";
  (void)::write(fd, line.data(), line.size());
  (void)::flock(fd, LOCK_UN);
  ::close(fd);
}

rpc::ObjectRef ObjectStore::Put(const std::string& object_id, std::string_view payload,
                                const std::string& owner_worker_id, const std::string& owner_address) {
  if (object_id.empty()) throw std::invalid_argument("empty object_id");
  const uint32_t crc = Crc32(payload);
  const size_t total_bytes = sizeof(ShmHeader) + payload.size();

  {
    std::lock_guard lock(mutex_);
    auto existing = local_objects_.find(object_id);
    if (existing != local_objects_.end()) {
      rpc::ObjectRef ref;
      ref.set_object_id(object_id);
      ref.set_size_bytes(existing->second.size_bytes);
      ref.set_crc32(existing->second.crc32);
      ref.set_owner_worker_id(owner_worker_id);
      ref.set_owner_address(owner_address);
      ref.set_host_id(host_id_);
      ref.set_shm_name(existing->second.shm_name);
      return ref;
    }
  }

  std::string shm_name;
  int fd = -1;
  for (int attempt = 0; attempt < 8; ++attempt) {
    shm_name = MakeShmName(object_id);
    fd = ::shm_open(shm_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd >= 0) break;
    if (errno != EEXIST) {
      throw std::runtime_error("shm_open(O_CREAT) failed for " + shm_name + ": " + std::strerror(errno));
    }
  }
  if (fd < 0) throw std::runtime_error("unable to allocate unique POSIX shared memory name");

  if (::ftruncate(fd, static_cast<off_t>(total_bytes)) != 0) {
    int err = errno;
    ::close(fd);
    ::shm_unlink(shm_name.c_str());
    throw std::runtime_error("ftruncate failed on shm segment: " + std::string(std::strerror(err)));
  }

  void* base = ::mmap(nullptr, total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  int map_err = errno;
  ::close(fd);
  if (base == MAP_FAILED) {
    ::shm_unlink(shm_name.c_str());
    throw std::runtime_error("mmap(RW) failed on shm segment: " + std::string(std::strerror(map_err)));
  }

  auto* header = new (base) ShmHeader{};
  header->magic = ShmHeader::kMagic;
  header->version = ShmHeader::kVersion;
  header->payload_size = payload.size();
  header->crc32 = crc;
  header->creator_pid = static_cast<uint32_t>(::getpid());
  if (!payload.empty()) {
    std::memcpy(static_cast<std::byte*>(base) + sizeof(ShmHeader), payload.data(), payload.size());
  }
  header->sealed.store(1u, std::memory_order_release);
  ::munmap(base, total_bytes);

  RecordManifest(shm_name);

  {
    std::lock_guard lock(mutex_);
    auto [it, inserted] = local_objects_.emplace(object_id, LocalEntry{shm_name, payload.size(), crc});
    if (!inserted) {
      ::shm_unlink(shm_name.c_str());
      shm_name = it->second.shm_name;
    }
  }

  rpc::ObjectRef ref;
  ref.set_object_id(object_id);
  ref.set_size_bytes(payload.size());
  ref.set_crc32(crc);
  ref.set_owner_worker_id(owner_worker_id);
  ref.set_owner_address(owner_address);
  ref.set_host_id(host_id_);
  ref.set_shm_name(shm_name);
  return ref;
}

ObjectView ObjectStore::MapReadOnly(const std::string& shm_name, uint64_t expected_size, uint32_t expected_crc32,
                                    bool verify_crc) {
  if (shm_name.empty()) throw std::invalid_argument("empty shm_name");
  int fd = ::shm_open(shm_name.c_str(), O_RDONLY, 0);
  if (fd < 0) {
    throw std::runtime_error("shm_open(O_RDONLY) failed for " + shm_name + ": " + std::strerror(errno));
  }
  struct stat st {};
  if (::fstat(fd, &st) != 0) {
    int err = errno;
    ::close(fd);
    throw std::runtime_error("fstat failed on shm segment: " + std::string(std::strerror(err)));
  }
  const size_t total_bytes = sizeof(ShmHeader) + static_cast<size_t>(expected_size);
  // Note: On macOS Darwin, st.st_size is rounded up to PAGE_SIZE (16 KiB), so check >= rather than ==.
  if (st.st_size < 0 || static_cast<size_t>(st.st_size) < total_bytes) {
    ::close(fd);
    throw std::runtime_error("shm segment smaller than expected header + payload");
  }
  void* base = ::mmap(nullptr, total_bytes, PROT_READ, MAP_SHARED, fd, 0);
  int map_err = errno;
  ::close(fd);
  if (base == MAP_FAILED) {
    throw std::runtime_error("mmap(PROT_READ) failed on shm segment: " + std::string(std::strerror(map_err)));
  }
  const auto* header = static_cast<const ShmHeader*>(base);
  if (header->magic != ShmHeader::kMagic || header->version != ShmHeader::kVersion ||
      header->sealed.load(std::memory_order_acquire) != 1u || header->payload_size != expected_size ||
      header->crc32 != expected_crc32) {
    ::munmap(base, total_bytes);
    throw std::runtime_error("corrupt, unsealed, or mismatched shm object header");
  }
  auto view = ObjectView::FromShmMapping(base, total_bytes, static_cast<size_t>(expected_size));
  if (verify_crc && Crc32(view.bytes()) != expected_crc32) {
    throw std::runtime_error("CRC32 mismatch on mapped shm object");
  }
  return view;
}

std::optional<ObjectView> ObjectStore::GetLocal(const std::string& object_id, uint32_t* out_crc32) const {
  LocalEntry entry;
  {
    std::lock_guard lock(mutex_);
    auto it = local_objects_.find(object_id);
    if (it == local_objects_.end()) return std::nullopt;
    entry = it->second;
  }
  if (out_crc32) *out_crc32 = entry.crc32;
  return MapReadOnly(entry.shm_name, entry.size_bytes, entry.crc32, false);
}

ObjectView ObjectStore::Resolve(const rpc::ObjectRef& ref, const RemotePullFn& pull_fn, bool* pulled_remotely) {
  if (pulled_remotely) *pulled_remotely = false;
  {
    std::lock_guard lock(mutex_);
    auto local_it = local_objects_.find(ref.object_id());
    if (local_it != local_objects_.end()) {
      return MapReadOnly(local_it->second.shm_name, local_it->second.size_bytes, local_it->second.crc32, false);
    }
  }
  // Fast path: Same-host zero-copy POSIX shared memory mapping.
  if (!ref.host_id().empty() && ref.host_id() == host_id_ && !ref.shm_name().empty()) {
    try {
      return MapReadOnly(ref.shm_name(), ref.size_bytes(), ref.crc32(), false);
    } catch (...) {
      // Fall through to remote pull if segment was evicted locally while a remote replica exists.
    }
  }

  if (!pull_fn) throw std::runtime_error("remote pull required but no pull_fn provided for " + ref.object_id());

  // Cross-host Singleflight deduplication: concurrent callers for the same object_id share one pull.
  std::shared_future<LocalEntry> shared;
  std::shared_ptr<std::promise<LocalEntry>> leader_promise;
  {
    std::lock_guard lock(mutex_);
    auto local_it = local_objects_.find(ref.object_id());
    if (local_it != local_objects_.end()) {
      return MapReadOnly(local_it->second.shm_name, local_it->second.size_bytes, local_it->second.crc32, false);
    }
    auto flight_it = inflight_pulls_.find(ref.object_id());
    if (flight_it != inflight_pulls_.end()) {
      shared = flight_it->second;
    } else {
      leader_promise = std::make_shared<std::promise<LocalEntry>>();
      shared = leader_promise->get_future().share();
      inflight_pulls_.emplace(ref.object_id(), shared);
    }
  }

  if (leader_promise) {
    try {
      std::string bytes = pull_fn(ref);
      if (bytes.size() != ref.size_bytes()) {
        throw std::runtime_error("pulled object size mismatch for " + ref.object_id());
      }
      if (Crc32(bytes) != ref.crc32()) {
        throw std::runtime_error("pulled object CRC32 mismatch for " + ref.object_id());
      }
      auto stored = Put(ref.object_id(), bytes);
      LocalEntry entry{stored.shm_name(), stored.size_bytes(), stored.crc32()};
      {
        std::lock_guard lock(mutex_);
        inflight_pulls_.erase(ref.object_id());
      }
      leader_promise->set_value(entry);
      if (pulled_remotely) *pulled_remotely = true;
      return MapReadOnly(entry.shm_name, entry.size_bytes, entry.crc32, false);
    } catch (...) {
      {
        std::lock_guard lock(mutex_);
        inflight_pulls_.erase(ref.object_id());
      }
      leader_promise->set_exception(std::current_exception());
      throw;
    }
  }

  LocalEntry entry = shared.get();
  return MapReadOnly(entry.shm_name, entry.size_bytes, entry.crc32, false);
}

bool ObjectStore::Evict(const std::string& object_id) {
  std::string shm_name;
  {
    std::lock_guard lock(mutex_);
    auto it = local_objects_.find(object_id);
    if (it == local_objects_.end()) return false;
    shm_name = std::move(it->second.shm_name);
    local_objects_.erase(it);
  }
  ::shm_unlink(shm_name.c_str());
  return true;
}

void ObjectStore::EvictAll() {
  std::vector<std::string> names;
  {
    std::lock_guard lock(mutex_);
    names.reserve(local_objects_.size());
    for (auto& [id, entry] : local_objects_) names.push_back(std::move(entry.shm_name));
    local_objects_.clear();
  }
  for (const auto& name : names) ::shm_unlink(name.c_str());
}

size_t ObjectStore::SweepOrphans(const std::string& manifest_path) {
  if (manifest_path.empty()) return 0;
  int fd = ::open(manifest_path.c_str(), O_CREAT | O_RDWR, 0600);
  if (fd < 0) return 0;
  (void)::flock(fd, LOCK_EX);
  struct stat st {};
  std::string content;
  if (::fstat(fd, &st) == 0 && st.st_size > 0) {
    content.resize(static_cast<size_t>(st.st_size));
    ssize_t n = ::pread(fd, content.data(), content.size(), 0);
    if (n > 0) content.resize(static_cast<size_t>(n));
    else content.clear();
  }

  std::istringstream in(content);
  std::ostringstream surviving;
  size_t swept = 0;
  uint64_t pid = 0;
  std::string shm_name;
  while (in >> pid >> shm_name) {
    bool creator_dead = (pid != static_cast<uint64_t>(::getpid())) &&
                        (::kill(static_cast<pid_t>(pid), 0) == -1 && errno == ESRCH);
    if (creator_dead) {
      if (::shm_unlink(shm_name.c_str()) == 0) ++swept;
      continue;
    }
    int probe = ::shm_open(shm_name.c_str(), O_RDONLY, 0);
    if (probe < 0 && errno == ENOENT) {
      continue; // Already cleanly unlinked.
    }
    if (probe >= 0) ::close(probe);
    surviving << pid << " " << shm_name << "\n";
  }

  std::string out = surviving.str();
  (void)::ftruncate(fd, 0);
  if (!out.empty()) (void)::pwrite(fd, out.data(), out.size(), 0);
  (void)::flock(fd, LOCK_UN);
  ::close(fd);
  return swept;
}

std::string ObjectStore::PullFromRemoteWorkers(const rpc::ObjectRef& ref, std::chrono::milliseconds timeout) {
  std::vector<std::string> endpoints;
  if (!ref.owner_address().empty()) endpoints.push_back(ref.owner_address());
  for (const auto& addr : ref.replica_addresses()) {
    if (!addr.empty() && std::find(endpoints.begin(), endpoints.end(), addr) == endpoints.end()) {
      endpoints.push_back(addr);
    }
  }
  if (endpoints.empty()) {
    throw std::runtime_error("no remote worker address available to pull object " + ref.object_id());
  }

  std::string last_error = "unreachable";
  for (const auto& addr : endpoints) {
    auto stub = rpc::WorkerService::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
    grpc::ClientContext context;
    Deadline(context, timeout);
    rpc::PullObjectRequest req;
    req.set_object_id(ref.object_id());
    auto reader = stub->PullObject(&context, req);
    std::string buffer;
    buffer.reserve(static_cast<size_t>(ref.size_bytes()));
    rpc::ObjectChunk chunk;
    while (reader->Read(&chunk)) {
      buffer.append(chunk.data());
      if (buffer.size() > ref.size_bytes()) break;
    }
    auto status = reader->Finish();
    if (!status.ok()) {
      last_error = status.error_message();
      continue;
    }
    if (buffer.size() != ref.size_bytes()) {
      last_error = "size mismatch during PullObject";
      continue;
    }
    if (Crc32(buffer) != ref.crc32()) {
      last_error = "CRC32 mismatch during PullObject";
      continue;
    }
    return buffer;
  }
  throw std::runtime_error("failed to pull object " + ref.object_id() + ": " + last_error);
}

}  // namespace miniray
