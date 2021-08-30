# PMS

A generic **P**ilot-**M**anagement-**S**ystem for distributed execution of jobs on multiple datacenter batch systems.

### Requirements
Most of the project dependencies are brought in by the project itself with the only exceptions of:
- **boost**:<br>Use your favourite source for installing boost. Just make sure you have these modules installed:<br>`system,thread,regex`
- **MongoDB C and C++ drivers**:<br>These must be compiled and installed beforehand or provided by some external binary repository. Check [here](https://github.com/mongodb/mongo-c-driver) and [here](https://github.com/mongodb/mongo-cxx-driver) for the source code.<br>Once you're done, pass `-Dbsoncxx_DIR=/your/path/to/mongocxx/installation/lib/cmake/bsoncxx-${version}
  -Dmongocxx_DIR=/your/path/to/mongocxx/installation/lib/cmake/mongocxx-${version}` during the project config step.
- **XRootD** (*will be optional*):<br>If you have XRootD installed then the correspondibg file transfer option will be enabled in the Pilot program. Unfortunately XRootD doesn't contemplate being built as part of a larger project, so it cannot be included via `FetchContent` and has to be installed manually. Once you're done pass `-DXROOTD_ROOT=/your/path/to/xrootd/installation/` during the project config step.