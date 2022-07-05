//
// Created by Valerio Formato on 05/06/21.
//

#ifndef PMS_QUEUE_H
#define PMS_QUEUE_H

#include <mutex>
#include <queue>

namespace PMS {

template <typename T> class ts_queue {
public:
  T &front() {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    return m_queue.front();
  };

  const T &front() const {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    return m_queue.front();
  };

  T &back() {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    return m_queue.back();
  };

  const T &back() const {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    return m_queue.back();
  };

  bool empty() const {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    return m_queue.empty();
  };

  size_t size() const {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    return m_queue.size();
  };

  void push(const T &value) {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    m_queue.push(value);
  }

  void push(T &&value) {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    m_queue.push(std::move(value));
  }

  template <class... Args> void emplace(Args &&...args) {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    m_queue.emplace(std::forward<Args>(args)...);
  }

  void pop() {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    m_queue.pop();
  };

  T consume() {
    std::lock_guard<decltype(m_mutex)>{m_mutex};
    T result = front();
    pop();
    return result;
  }

private:
  mutable std::mutex m_mutex;
  std::queue<T> m_queue;
};

} // namespace PMS

#endif
