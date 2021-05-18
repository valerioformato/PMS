#ifndef PMS_DB_CREDTYPE_H
#define PMS_DB_CREDTYPE_H

namespace PMS {
namespace DB {
enum class CredType {
  None = 0,
  PWD,
  X509,
};
}
} // namespace PMS

#endif