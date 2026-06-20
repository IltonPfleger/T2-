#ifndef __TRAITS__
#define __TRAITS__

namespace Atomic {

template <typename T> struct Traits;

class Topology;
class Debug;

template <> struct Traits<Topology> {
    static constexpr const char *Address    = "127.0.0.1";
    static constexpr uint32_t Port          = 5000;
    static constexpr uint32_t NumberOfNodes = 5;
};

template <> struct Traits<Debug> {
    static constexpr bool Trace = true;
    static constexpr bool Info  = true;
    static constexpr bool Error = true;
};

} // namespace Atomic

#endif
