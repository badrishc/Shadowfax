// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"
#include "network/tcp.h"

/// A simple test that allocates a server and client ConnectHandler. Doing this
/// to check if resources get allocated correctly.
TEST(Tcp, ConnectHandler) {
  typedef transport::TcpTransport T;

  T sTransport, cTransport;

  std::string lIP("0.0.0.0");
  T::ConnectHandler sHandler(22790, lIP, &sTransport);
  T::ConnectHandler cHandler(0, lIP, &cTransport);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
