#ifndef PMS_COMMON_ENUMARRAY_H
#define PMS_COMMON_ENUMARRAY_H

#include <magic_enum.hpp>

template <typename ContainedClass, typename Enum>
class EnumArray : public std::array<ContainedClass, magic_enum::enum_count<Enum>()> {
public:
  using BaseType = std::array<ContainedClass, magic_enum::enum_count<Enum>()>;

  // constructors
  EnumArray() = default;
  EnumArray(const EnumArray<ContainedClass, Enum> &c) : BaseType(c) {}
  EnumArray(const std::array<ContainedClass, magic_enum::enum_count<Enum>()> &values) : BaseType(values) {}
  EnumArray(std::array<ContainedClass, magic_enum::enum_count<Enum>()> &&values) : BaseType(values) {}
  template <typename... V> EnumArray(V &&...vals) : BaseType{{std::forward<V>(vals)...}} {};

  // assignment
  EnumArray<ContainedClass, Enum> &operator=(const EnumArray<ContainedClass, Enum> &lhs) {
    BaseType::operator=(lhs);
    return *this;
  }

  // access
  ContainedClass &operator[](const Enum &index) {
    return BaseType::operator[](static_cast<std::underlying_type_t<Enum>>(index));
  }
  const ContainedClass &operator[](const Enum &index) const {
    return BaseType::operator[](static_cast<std::underlying_type_t<Enum>>(index));
  }
  ContainedClass &at(const Enum &index) { return BaseType::at(static_cast<std::underlying_type_t<Enum>>(index)); }
  const ContainedClass &at(const Enum &index) const {
    return BaseType::at(static_cast<std::underlying_type_t<Enum>>(index));
  }
};
#endif
