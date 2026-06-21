#include <Debug.hpp>
#include <Mutex.hpp>
#include <Thread.hpp>
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <netinet/in.h>
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
        uintmax_t origin;
        uintmax_t proposedOrder;
        uintmax_t agreedOrder;
        bool delivered = false;
    };

    struct Collecting {
        int payloadLength;
        int received          = 0;
        uintmax_t maxOrder    = 0;
        uintmax_t maxProposer = 0;
    };

  public:
    Node(uint32_t id)
        : id_(id),
          counter_(0),
          maxAgreed_(0) {
        Debug::Trace("New Node ", id, "!");
        const char *address = Traits<Topology>::Address;
        int port            = Traits<Topology>::Port + id;
        sockfd_             = socket(AF_INET, SOCK_DGRAM, 0);
        Debug::Error(sockfd_ < 0, "Can't Create Node Socket!");
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(port);
        addr.sin_addr.s_addr = inet_addr(address);
        bind(sockfd_, (struct sockaddr *)&addr, sizeof(addr));
        running_ = true;
        new (&receiver_) Thread(receiver, this);
    }

    static void *receiver(void *pointer) {
        Node *self = reinterpret_cast<Node *>(pointer);
        struct sockaddr_in addr;
        socklen_t socklen = sizeof(addr);
        char buffer[4096];
        while (true) {
            int length = recvfrom(self->sockfd_, buffer, sizeof(buffer) - 1, 0, (struct sockaddr *)&addr, &socklen);
            if (!self->running_) break;
            Packet *packet = reinterpret_cast<Packet *>(buffer);
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

        uint8_t *buffer = reinterpret_cast<uint8_t *>(malloc(length + sizeof(Packet)));
        Packet *packet  = reinterpret_cast<Packet *>(buffer);
        packet->type    = Packet::Type::MULTICAST;
        packet->sender  = id_;
        packet->id      = msgId;
        packet->order   = 0;
        packet->length  = length;
        memcpy(packet->payload, payload, length);

        {
            Mutex::Guard guard(mutex_);
            Collecting &c   = collecting_[msgId];
            c.payloadLength = length;
        }

        for (int i = 0; i < Traits<Topology>::NumberOfNodes; i++) {
            send(i, packet, length + sizeof(Packet));
        }

        free(buffer);
        return true;
    }

    void send(int target, const Packet *packet, int length) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(Traits<Topology>::Port + target);
        addr.sin_addr.s_addr = inet_addr(Traits<Topology>::Address);
        sendto(sockfd_, packet, length, 0, (struct sockaddr *)&addr, sizeof(addr));
    }

    uintmax_t seed() { return (counter_++ << 32) | id_; }

  private:
    void onMulticast(const Packet *packet) {
        Mutex::Guard guard(mutex_);

        Pending &p = pending_[packet->id];
        p.origin   = packet->sender;
        p.payload.assign(packet->payload, packet->payload + packet->length);

        priorityCounter_ = std::max(priorityCounter_, maxAgreed_) + 1;
        p.proposedOrder  = priorityCounter_;
        p.agreedOrder    = 0;

        sendPropose(packet->sender, packet->id, p.proposedOrder);
    }

    void onPropose(const Packet *packet) {
        Mutex::Guard guard(mutex_);

        auto it = collecting_.find(packet->id);
        if (it == collecting_.end()) return;
        Collecting &c = it->second;

        if (packet->order > c.maxOrder || (packet->order == c.maxOrder && packet->sender > c.maxProposer)) {
            c.maxOrder    = packet->order;
            c.maxProposer = packet->sender;
        }

        c.received++;

        if (c.received == Traits<Topology>::NumberOfNodes) {
            sendCommit(packet->id, c.maxOrder);
            collecting_.erase(it);
        }
    }

    void onCommit(const Packet *packet) {
        Mutex::Guard guard(mutex_);

        auto it = pending_.find(packet->id);
        if (it == pending_.end()) return;
        Pending &p = it->second;

        p.agreedOrder = packet->order;
        maxAgreed_    = std::max(maxAgreed_, p.agreedOrder);

        deliverReady();
    }

    void deliverReady() {
        while (true) {
            uintmax_t bestId    = 0;
            uintmax_t bestOrder = 0;
            bool found          = false;

            for (auto &kv : pending_) {
                Pending &p = kv.second;
                if (p.delivered) continue;
                if (p.agreedOrder == 0) {
                    return;
                }
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

    virtual void deliver(const void *payload, int length) {
        int sender = *reinterpret_cast<const int *>(payload);
        Debug::Trace("Node ", id_, " delivers message from ", sender);
    }

    void sendPropose(uintmax_t origin, uintmax_t msgId, uintmax_t order) {
        Packet packet;
        packet.type   = Packet::Type::PROPOSE;
        packet.id     = msgId;
        packet.sender = id_;
        packet.order  = order;
        packet.length = 0;
        send(origin, &packet, sizeof(Packet));
    }

    void sendCommit(uintmax_t msgId, uintmax_t order) {
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

    uint32_t id_;
    uint64_t counter_;
    uintmax_t priorityCounter_ = 0;
    uintmax_t maxAgreed_       = 0;
    int sockfd_;
    bool running_;
    Thread receiver_;
    Mutex mutex_;

    std::map<uintmax_t, Pending> pending_;
    std::map<uintmax_t, Collecting> collecting_;
};

} // namespace Atomic
