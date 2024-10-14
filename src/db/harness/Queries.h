#pragma once

#include <common/JsonUtils.h>
#include <variant>

namespace PMS::DB::Queries {
struct Find {
  json match;
  json filter;
};

struct FindOne {
  json match;
  json filter;
};

struct InsertOne {};
struct InsertMany {};

struct UpdateOne {};
struct UpdateMany {};

struct DeleteOne {};
struct DeleteMany {};

struct Count {};
struct Aggregate {};

using Query =
    std::variant<Find, FindOne, InsertOne, InsertMany, UpdateOne, UpdateMany, DeleteOne, DeleteMany, Count, Aggregate>;
} // namespace PMS::DB::Queries
