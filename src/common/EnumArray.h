template <typename ContainedClass, typename Enum, unsigned int EnumSize>
class EnumArray : public std::array<ContainedClass, EnumSize> {
public:
  using BaseType = std::array<ContainedClass, EnumSize>;

  // constructors
  EnumArray() = default;
  EnumArray(const EnumArray<ContainedClass, Enum, EnumSize> &c) : BaseType(c) {}
  EnumArray(const std::array<ContainedClass, EnumSize> &values) : BaseType(values) {}
  EnumArray(std::array<ContainedClass, EnumSize> &&values) : BaseType(values) {}
  template <typename... V> EnumArray(V &&... vals) : BaseType{{std::forward<V>(vals)...}} {};

  // assignment
  EnumArray<ContainedClass, Enum, EnumSize> &operator=(const EnumArray<ContainedClass, Enum, EnumSize> &lhs) {
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