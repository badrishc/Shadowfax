// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <string>
#include <fstream>
#include <iostream>

#include "zipf.h"

#include "boost/program_options.hpp"

using namespace boost::program_options;

int main(int argc, char* argv[]) {
  options_description desc("Allowed options");
  desc.add_options()
    ("help", "Produce a help message and exit")
    ("skew", value<double>()->default_value(0.99),
       "Value of the zipfian skew (theta)")
    ("nKey", value<size_t>()->default_value(250 * 1000 * 1000),
       "Number of keys in the distribution")
    ("nReq", value<size_t>()->default_value(1 * 1000 * 1000 * 1000),
       "Number of requests that should be generated")
    ("keyf", value<std::string>()->default_value("ycsb_keys"),
       "Name of the file keys should be generated into")
    ("reqf", value<std::string>()->default_value("ycsb_reqs"),
       "Name of the file requests should be generated into")
  ;

  // Parse command line options into a variable map.
  variables_map vm;
  store(parse_command_line(argc, argv, desc), vm);
  notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  // Retrieve the parsed options from the variable map.
  double skew = vm["skew"].as<double>();
  size_t nKey = vm["nKey"].as<size_t>();
  size_t nReq = vm["nReq"].as<size_t>();
  std::string keyf = vm["keyf"].as<std::string>();
  std::string outf = vm["reqf"].as<std::string>();

  std::ofstream keyFile(keyf);
  fprintf(stderr, "Generating key distribution.........\n");
  for (uint64_t i = 0; i < nKey; i++) {
    keyFile.write(reinterpret_cast<char*>(&i), sizeof(i));
  }

  std::ofstream outputFile(outf);
  ZipfianGenerator gen(nKey, skew);
  fprintf(stderr, "Generating request distribution.....\n");

  for (auto i = 0; i < nReq; i++) {
    uint64_t key = gen.nextNumber();
    outputFile.write(reinterpret_cast<char*>(&key), sizeof(key));
  }

  return 0;
}
