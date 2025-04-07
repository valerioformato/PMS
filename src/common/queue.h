//
// Created by Valerio Formato on 05/06/21.
//

#ifndef PMS_QUEUE_H
#define PMS_QUEUE_H

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>

#include <mutex>
#include <queue>

namespace PMS {

constexpr std::size_t const hardware_constructive_interference_size{2 * sizeof(std::max_align_t)};

template <typename T> class ts_queue {
public:
  constexpr explicit ts_queue(const std::size_t capacity = UINT16_MAX)
      : m_capacity(capacity), m_allocator(std::allocator<container_t<T>>()), m_head(0ul), m_tail(0ul) {
    m_container = m_allocator.allocate(m_capacity + 1);
    for (const auto &i : std::views::iota(0ul, m_capacity)) {
      new (&m_container[i]) container_t<T>();
    }
  }

  constexpr ts_queue(const ts_queue &) = delete;
  constexpr ts_queue &operator=(const ts_queue &) = delete;

  ~ts_queue(void) noexcept {
    for (const auto &i : std::views::iota(0ul, m_capacity)) {
      m_container[i].~container_t();
    }
    m_allocator.deallocate(m_container, m_capacity + 1);
  }

  constexpr void push(const T &v) noexcept { emplace(v); }
  constexpr void push(std::convertible_to<T> auto &&v) noexcept { emplace(std::forward<decltype(v)>(v)); }

  constexpr std::vector<T> consume_all() noexcept {
    std::vector<T> res;
    res.reserve(m_capacity);

    while (!empty()) {
      res.push_back(pop());
    }

    return res;
  }

  constexpr T &front() noexcept {
    const auto head{m_head.load(std::memory_order::acquire)};
    return m_container[head % m_capacity].as_ref();
  }

  bool empty() const noexcept {
    const auto head{m_head.load(std::memory_order::acquire)};
    const auto tail{m_tail.load(std::memory_order::acquire)};
    return head == tail;
  }

  constexpr T pop() noexcept {
    std::optional<T> res;

    const auto tail{m_tail.fetch_add(1, std::memory_order::acquire)};
    const auto head{(tail / m_capacity) * 2 + 1};
    auto *container{&m_container[tail % m_capacity]};

    while (true) {
      const auto now{container->m_idx_.load(std::memory_order::acquire)};
      if (now == head) {
        break;
      }
      container->m_idx_.wait(now, std::memory_order::relaxed);
    }

    res = std::move(container->move());
    container->destruct();

    container->m_idx_.store(head + 1, std::memory_order::release);
    container->m_idx_.notify_all();

    return std::move(*res);
  }

  template <typename... Args> constexpr void emplace(Args &&...args) noexcept {
    const auto tail{m_head.fetch_add(1, std::memory_order::acquire)};
    const auto head{(tail / m_capacity) * 2};
    auto *container{&m_container[tail % m_capacity]};

    while (true) {
      const auto now{container->m_idx_.load(std::memory_order::acquire)};
      if (now == head) {
        break;
      }
      container->m_idx_.wait(now, std::memory_order::relaxed);
    }

    container->construct(std::forward<Args>(args)...);

    container->m_idx_.store(head + 1, std::memory_order::release);
    container->m_idx_.notify_all();
  }

protected:
  template <typename V> struct container_t {
  public:
    ~container_t() noexcept {
      if (m_idx_.load(std::memory_order::acquire)) {
        destruct();
      }
    }

    template <typename... Args> constexpr void construct(Args &&...args) noexcept {
      new (&storage_t) V(std::forward<Args>(args)...);
    }
    constexpr void destruct() noexcept { reinterpret_cast<V *>(&storage_t)->~V(); }
    constexpr V &&move() noexcept { return reinterpret_cast<V &&>(storage_t); }
    constexpr V &as_ref() noexcept { return reinterpret_cast<V &>(storage_t); }

    alignas(hardware_constructive_interference_size) std::atomic_size_t mutable m_idx_{0};

  private:
    typename std::aligned_storage_t<sizeof(V), alignof(V)> storage_t;
  };

private:
  const std::size_t m_capacity;
  std::allocator<container_t<T>> m_allocator;
  container_t<T> *m_container;

  alignas(hardware_constructive_interference_size) std::atomic_size_t m_head;
  alignas(hardware_constructive_interference_size) std::atomic_size_t m_tail;
};

namespace old {
template <typename T> class ts_queue {
public:
  T &front() {
    std::lock_guard lock{m_mutex};
    return m_queue.front();
  };

  const T &front() const {
    std::lock_guard lock{m_mutex};
    return m_queue.front();
  };

  T &back() {
    std::lock_guard lock{m_mutex};
    return m_queue.back();
  };

  const T &back() const {
    std::lock_guard lock{m_mutex};
    return m_queue.back();
  };

  bool empty() const {
    std::lock_guard lock{m_mutex};
    return m_queue.empty();
  };

  size_t size() const {
    std::lock_guard lock{m_mutex};
    return m_queue.size();
  };

  void push(const T &value) {
    std::lock_guard lock{m_mutex};
    m_queue.push(value);
  }

  void push(T &&value) {
    std::lock_guard lock{m_mutex};
    m_queue.push(std::move(value));
  }

  template <class... Args> void emplace(Args &&...args) {
    std::lock_guard lock{m_mutex};
    m_queue.emplace(std::forward<Args>(args)...);
  }

  void pop() {
    std::lock_guard lock{m_mutex};
    m_queue.pop();
  };

  T consume() {
    std::lock_guard lock{m_mutex};
    T result = m_queue.front();
    m_queue.pop();
    return result;
  }

  std::vector<T> consume_all() {
    std::lock_guard lock{m_mutex};
    std::vector<T> result;
    while (!m_queue.empty()) {
      result.push_back(m_queue.front());
      m_queue.pop();
    }
    return result;
  }

private:
  mutable std::mutex m_mutex;
  std::queue<T> m_queue;
};
} // namespace old
} // namespace PMS

#endif
