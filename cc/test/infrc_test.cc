// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"
#include "network/infrc.h"

/// A simple test that allocates a server and client ConnectHandler. Doing this
/// to check if NIC resources get allocated correctly.
TEST(Infrc, ConnectHandler) {
  typedef transport::InfrcTransport T;

  T sTransport;
  std::string lIP("0.0.0.0");
  T::ConnectHandler server(22790, lIP, &sTransport);

  T cTransport;
  T::ConnectHandler client(0, lIP, &cTransport);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
