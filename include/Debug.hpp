#ifndef __DEBUG__
#define __DEBUG__

#include <Traits.hpp>
#include <iostream>

namespace Atomic {

class Debug {
  public:
    template <typename... Args> static void Trace(Args &&...args) {
        if constexpr (Traits<Debug>::Trace) {
            (std::cout << ... << args) << '\n';
        }
    }

    template <typename... Args> static void Info(Args &&...args) {
        if constexpr (Traits<Debug>::Info) {
            (std::cout << ... << args) << '\n';
        }
    }

    template <typename... Args> static void Error(bool condition, Args &&...args) {
        if constexpr (Traits<Debug>::Error) {
            if (condition) {
                std::cerr << "[ERROR] ";
                (std::cerr << ... << args) << '\n';
                std::abort();
            }
        }
    }
};

}; // namespace Atomic

#endif
