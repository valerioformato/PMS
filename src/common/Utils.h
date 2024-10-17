//
// Created by Valerio Formato on 15/12/21.
//

#ifndef PMS_UTILS_H
#define PMS_UTILS_H

#include <charconv>
#include <chrono>
#include <string_view>

#include <fmt/format.h>

#include <boost/outcome.hpp>
namespace outcome = boost::outcome_v2;

namespace PMS {
template <typename T> using ErrorOr = outcome::result<T>;
}

namespace PMS::Utils {
template <class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

inline std::chrono::seconds ParseTimeString(const std::string_view tString) {
  static constexpr std::string_view digits = "0123456789";
  std::chrono::seconds result{};

  size_t startPos = 0;
  auto tokenPos = tString.find_first_not_of(digits, startPos);
  if (tokenPos == std::string_view::npos) {
    throw(std::runtime_error(fmt::format("{} is not a valid time string", tString)));
  }

  while (tokenPos != std::string_view::npos) {
    unsigned int timeVal = 0;
    auto [ptr, ec] = std::from_chars(std::next(begin(tString), startPos), std::next(begin(tString), tokenPos), timeVal);

    if (ptr == std::next(begin(tString), startPos)) {
      throw(std::runtime_error(fmt::format("{} is not a valid time string", ptr)));
    }

    switch (tString[tokenPos]) {
    case 's':
      result += std::chrono::seconds{timeVal};
      break;
    case 'm':
      result += std::chrono::minutes{timeVal};
      break;
    case 'h':
      result += std::chrono::hours{timeVal};
      break;
    case 'd':
      result += std::chrono::hours{24 * timeVal};
      break;
    default:
      throw(std::runtime_error(fmt::format("Invalid time duration: {}", tString[tokenPos])));
    }

    startPos = tokenPos + 1;
    tokenPos = tString.find_first_not_of(digits, startPos);
  }

  return result;
}
} // namespace PMS::Utils

#endif // PMS_UTILS_H
