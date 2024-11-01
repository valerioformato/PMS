namespace PMS::Tests::Utils {
template <class Range> inline bool RangeContains(const Range &range, const typename Range::value_type &value) {
  return std::find(range.begin(), range.end(), value) != range.end();
}
} // namespace PMS::Tests::Utils
