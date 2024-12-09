//
// Created by Valerio Formato on 15/12/21.
//

#ifndef PMS_UTILS_H
#define PMS_UTILS_H

#include <charconv>
#include <chrono>
#include <string_view>
#include <type_traits>

#include <fmt/format.h>

#include <boost/outcome.hpp>
namespace outcome = boost::outcome_v2;

// adapted from https://github.com/SerenityOS/serenity/blob/master/AK/Try.h
// FIXME: At some point we should use the AK implementation or at least make this one more robust
#define TRY(expression)                                                                                                \
  ({                                                                                                                   \
    auto &&_temporary_result = (expression);                                                                           \
    static_assert(!std::is_lvalue_reference_v<std::remove_cvref_t<decltype(_temporary_result)>::value_type>,           \
                  "Do not return a reference from a fallible expression");                                             \
    if (_temporary_result.has_error()) [[unlikely]]                                                                    \
      return _temporary_result.error();                                                                                \
    _temporary_result.value();                                                                                         \
  })

#define TRY_REPEATED(expression, max_tries)                                                                            \
  ({                                                                                                                   \
    std::remove_cvref_t<decltype(max_tries)> tries{0};                                                                 \
    auto &&_temporary_result = (expression);                                                                           \
    static_assert(!std::is_lvalue_reference_v<std::remove_cvref_t<decltype(_temporary_result)>::value_type>,           \
                  "Do not return a reference from a fallible expression");                                             \
    while (_temporary_result.has_error() && tries < max_tries) {                                                       \
      _temporary_result = (expression);                                                                                \
      ++tries;                                                                                                         \
    }                                                                                                                  \
    if (_temporary_result.has_error()) [[unlikely]]                                                                    \
      return _temporary_result;                                                                                        \
    _temporary_result.value();                                                                                         \
  })

namespace PMS {
struct Error {
  Error() = default;
  Error(std::error_code code, std::string_view msg) : m_code(code), m_msg(msg) {}
  Error(std::errc code, std::string_view msg) : m_code(std::make_error_code(code)), m_msg(msg) {}

  const std::string_view Message() const { return m_msg; }
  std::error_code Code() const { return m_code; }

private:
  std::error_code m_code;
  std::string m_msg;
};
static_assert(std::is_default_constructible_v<Error>, "PMS::Error must be default constructible");

inline std::error_code make_error_code(Error e) { return e.Code(); }
static_assert(outcome::trait::is_error_code_available_v<Error>, "Error must have a make_error_code function");

inline void outcome_throw_as_system_error_with_payload(const ::PMS::Error &error) {
  BOOST_OUTCOME_THROW_EXCEPTION(std::system_error(error.Code())); // NOLINT
}
} // namespace PMS

namespace PMS {
template <typename T> using ErrorOr = outcome::result<T, Error>;
} // namespace PMS

namespace PMS::Utils {
template <class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

inline uint64_t CurrentTimeToMillisSinceEpoch() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline std::vector<std::string_view> TokenizeString(const std::string_view str, const char delimiter) {
  std::vector<std::string_view> tokens;
  size_t start = 0;
  size_t end = str.find(delimiter);

  while (end != std::string_view::npos) {
    tokens.emplace_back(str.substr(start, end - start));
    start = end + 1;
    end = str.find(delimiter, start);
  }

  tokens.emplace_back(str.substr(start));
  return tokens;
}

inline std::vector<std::string> TokenizeString(const std::string str, const char delimiter) {
  std::vector<std::string> tokens;
  size_t start = 0;
  size_t end = str.find(delimiter);

  while (end != std::string::npos) {
    tokens.emplace_back(str.substr(start, end - start));
    start = end + 1;
    end = str.find(delimiter, start);
  }

  tokens.emplace_back(str.substr(start));
  return tokens;
}

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
