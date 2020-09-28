// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cmath>
#include <random>

/// Used to generate zipfian distributed random numbers where the distribution
/// is skewed toward the lower integers; e.g. 0 will be the most popular, 1 the
/// next most popular, etc.
///
/// This class implements the core algorithm from YCSB's ZipfianGenerator; it,
/// in turn, uses the algorithm from "Quickly Generating Billion-Record
/// Synthetic Databases", Jim Gray et al, SIGMOD 1994.
class ZipfianGenerator {
 public:
  /// Construct a generator.  This may be expensive if n is large.
  ///
  /// \param n
  ///    The generator will output random numbers between 0 and n-1.
  /// \param theta
  ///    The zipfian parameter where 0 < theta < 1 defines the skew; the
  ///    smaller the value the more skewed the distribution will be. Default
  ///    value of 0.99 comes from the YCSB default value.
  explicit ZipfianGenerator(uint64_t n, double theta = 0.99)
    : n(n)
    , theta(theta)
    , alpha(1 / (1 - theta))
    , zetan(zeta(n, theta))
    , eta((1 - std::pow(2.0 / static_cast<double>(n), 1 - theta)) /
          (1 - zeta(2, theta) / zetan))
    , rng()
  {
    std::random_device rd;
    rng.seed(rd());
  }

  /// Return the zipfian distributed random number between 0 and n-1.
  uint64_t nextNumber() {
    double u = static_cast<double>(rng()) /
               static_cast<double>(~0UL);
    double uz = u * zetan;
    if (uz < 1) return 0;
    if (uz < 1 + std::pow(0.5, theta)) return 1;
    return 0 + static_cast<uint64_t>(static_cast<double>(n) *
                                     std::pow(eta*u - eta + 1.0, alpha));
  }

 private:
  const uint64_t n;       // Range of numbers to be generated.
  const double theta;     // Parameter of the zipfian distribution.
  const double alpha;     // Special intermediate result used for generation.
  const double zetan;     // Special intermediate result used for generation.
  const double eta;       // Special intermediate result used for generation.
  std::mt19937 rng;       // Random number generator used for generation.

  /// Returns the nth harmonic number with parameter theta; e.g. H_{n,theta}.
  static double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 0; i < n; i++) sum = sum + 1.0/(std::pow(i+1, theta));
    return sum;
  }
};
