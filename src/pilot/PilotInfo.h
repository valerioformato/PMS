//
// Created by vformato on 12/8/22.
//

#ifndef PMS_PILOTINFO_H
#define PMS_PILOTINFO_H

// c++ headers
#include <string>

// external headers
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace PMS::Pilot {
struct Info {
  boost::uuids::uuid uuid{boost::uuids::random_generator()()};

  std::string hostname{};
  std::string ip{};
  std::string os_version{};
};
} // namespace PMS::Pilot

#endif // PMS_PILOTINFO_H
