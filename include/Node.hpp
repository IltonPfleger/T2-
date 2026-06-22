#include <Debug.hpp>
#include <Mutex.hpp>
#include <Socket.hpp>
#include <Thread.hpp>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <numeric>
#include <pthread.h>
#include <random>
#include <string>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
#include <vector>
namespace Atomic {
class Node {
    struct Packet {
        enum class Type { MULTICAST, PROPOSE, COMMIT } type;
        uintmax_t id;
        uintmax_t sender;
        uintmax_t order;
        uintmax_t length;
        unsigned char payload[];
    };
    struct Pending {
        std::vector<unsigned char> payload;
        uintmax_t origin        = 0;
        uintmax_t proposedOrder = 0;
        uintmax_t agreedOrder   = 0;
        bool delivered          = false;
    };
    struct Collecting {
        int payloadLength     = 0;
        int received          = 0;
        uintmax_t maxOrder    = 0;
        uintmax_t maxProposer = 0;
    };

  public:
    Node(uint32_t id)
        : id_(id),
          counter_(0),
          socket_(Traits<Topology>::Address, Traits<Topology>::Port + id),
          maxAgreed_(0),
          receiver_(receiver, this) {
        srand(time(nullptr) + id * 1000);
        Debug::Trace("New Node ", id, "!");
    }
    virtual ~Node()                                       = default;
    virtual void deliver(const void *payload, int length) = 0;
    static void *receiver(void *pointer) {
        Node *self = reinterpret_cast<Node *>(pointer);
        while (true) {
            size_t length;
            auto buffer  = self->socket_.receive(length);
            auto *packet = reinterpret_cast<Packet *>(buffer.get());
            self->receive(packet, length);
        }
        return nullptr;
    }
    void receive(const Packet *packet, int length) {
        switch (packet->type) {
            case Packet::Type::MULTICAST: onMulticast(packet); break;
            case Packet::Type::PROPOSE: onPropose(packet); break;
            case Packet::Type::COMMIT: onCommit(packet); break;
        }
    }
    bool broadcast(const void *payload, int length) {
        uintmax_t msgId = seed();
        std::vector<uint8_t> buffer(sizeof(Packet) + length);
        Packet *packet = reinterpret_cast<Packet *>(buffer.data());
        packet->type   = Packet::Type::MULTICAST;
        packet->sender = id_;
        packet->id     = msgId;
        packet->order  = 0;
        packet->length = length;
        std::memcpy(packet->payload, payload, length);
        {
            Mutex::Guard _(mutex_);
            collecting_[msgId].payloadLength = length;
        }
        std::vector<int> order(Traits<Topology>::NumberOfNodes);
        std::iota(order.begin(), order.end(), 0);
        std::shuffle(order.begin(), order.end(), std::default_random_engine(msgId));
        for (int i : order)
            send(i, packet, length + sizeof(Packet));
        return true;
    }
    void send(int target, const Packet *packet, int length) {
        if (packet->type == Packet::Type::MULTICAST) usleep(rand() % 300000);
        socket_.send(Traits<Topology>::Address, Traits<Topology>::Port + target, packet, length);
    }
    uintmax_t seed() { return (counter_++ << 32) | id_; }

  private:
    void onMulticast(const Packet *packet) {
        Mutex::Guard _(mutex_);
        std::printf("[P%u] CHEGADA | msg de P%ju chegou\n", id_, packet->sender);
        std::fflush(stdout);
        Pending &p = pending_[packet->id];
        p.origin   = packet->sender;
        p.payload.assign(packet->payload, packet->payload + packet->length);
        priorityCounter_ = std::max(priorityCounter_, maxAgreed_) + 1;
        p.proposedOrder  = priorityCounter_;
        doPropose(packet->sender, packet->id, p.proposedOrder);
    }
    void onPropose(const Packet *packet) {
        Mutex::Guard _(mutex_);
        auto it = collecting_.find(packet->id);
        if (it == collecting_.end()) return;
        Collecting &c = it->second;
        if (packet->order > c.maxOrder || (packet->order == c.maxOrder && packet->sender > c.maxProposer)) {
            c.maxOrder    = packet->order;
            c.maxProposer = packet->sender;
        }
        c.received++;
        if (c.received == Traits<Topology>::NumberOfNodes) {
            doCommit(packet->id, c.maxOrder);
            collecting_.erase(it);
        }
    }
    void onCommit(const Packet *packet) {
        Mutex::Guard _(mutex_);
        auto it = pending_.find(packet->id);
        if (it == pending_.end()) return;
        it->second.agreedOrder = packet->order;
        maxAgreed_             = std::max(maxAgreed_, it->second.agreedOrder);
        doDelivery();
    }
    void doDelivery() {
        while (true) {
            uintmax_t bestId    = 0;
            uintmax_t bestOrder = 0;
            bool found          = false;
            for (auto &kv : pending_) {
                Pending &p = kv.second;
                if (p.delivered) continue;
                if (p.agreedOrder == 0) return;
                if (!found || p.agreedOrder < bestOrder) {
                    found     = true;
                    bestOrder = p.agreedOrder;
                    bestId    = kv.first;
                }
            }
            if (!found) return;
            Pending &p  = pending_[bestId];
            p.delivered = true;
            deliver(p.payload.data(), p.payload.size());
        }
    }
    void doPropose(uintmax_t origin, uintmax_t msgId, uintmax_t order) {
        Packet packet;
        packet.type   = Packet::Type::PROPOSE;
        packet.id     = msgId;
        packet.sender = id_;
        packet.order  = order;
        packet.length = 0;
        send(origin, &packet, sizeof(Packet));
    }
    void doCommit(uintmax_t msgId, uintmax_t order) {
        Packet packet;
        packet.type   = Packet::Type::COMMIT;
        packet.id     = msgId;
        packet.sender = id_;
        packet.order  = order;
        packet.length = 0;
        for (int i = 0; i < Traits<Topology>::NumberOfNodes; i++) {
            send(i, &packet, sizeof(Packet));
        }
    }

  private:
    uint32_t id_;
    uint64_t counter_;
    uintmax_t priorityCounter_ = 0;
    uintmax_t maxAgreed_       = 0;
    Thread receiver_;
    Mutex mutex_;
    Socket socket_;
    std::map<uintmax_t, Pending> pending_;
    std::map<uintmax_t, Collecting> collecting_;
};

} // namespace Atomic
