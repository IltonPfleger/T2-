#ifndef __MESSAGE__
#define __MESSAGE__

namespace Atomic {

struct Message {
    enum class Type { REQUEST_VOTE, VOTE_REPLY, HEARTBEAT } type;
    int sender;
    uint64_t epoch;
    bool granted;
    unsigned char message[];
};

} // namespace Atomic

#endif
