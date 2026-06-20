#include <Debug.hpp>
#include <Message.hpp>
#include <Mutex.hpp>
#include <Sequencer.hpp>
#include <Thread.hpp>
#include <arpa/inet.h>
#include <chrono>
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
    enum class State { FOLLOWER, CANDIDATE, LEADER };

  public:
    Node(uint32_t id)
        : id_(id),
          election_timeout_(random_timeout()) {
        Debug::Trace(id);

        const char *address = Traits<Topology>::Address;
        int port            = Traits<Topology>::Port + id;

        sockfd_ = socket(AF_INET, SOCK_DGRAM, 0);
        Debug::Error(sockfd_ < 0, "Can't Create Node Socket!");

        struct sockaddr_in addr;
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(port);
        addr.sin_addr.s_addr = inet_addr(address);
        bind(sockfd_, (struct sockaddr *)&addr, sizeof(addr));

        new (&ticker_) Thread(ticker, this);
        new (&receiver_) Thread(receiver, this);
    }

    ~Node() { ticker_.join(); }

    static void *receiver(void *pointer) {
        Node *self = reinterpret_cast<Node *>(pointer);
        struct sockaddr_in addr;
        socklen_t socklen = sizeof(addr);
        char buffer[2048];
        while (1) {
            int length = recvfrom(self->sockfd_, buffer, sizeof(buffer) - 1, 0, (struct sockaddr *)&addr, &socklen);
            if (length > 0) {
                Message *message = reinterpret_cast<Message *>(buffer);
                self->receive(message, length);
            }
        }
        return nullptr;
    }

    static void *ticker(void *pointer) {
        Node *self = reinterpret_cast<Node *>(pointer);
        while (1) {
            usleep(100);

            self->check_election_timeout();

            if (self->state_ == State::LEADER) {
                // self->send_heartbeat();
            }
        }
        return nullptr;
    }

    void receive(const Message *message, int length) {
        switch (message->type) {
            case Message::Type::REQUEST_VOTE: handle_vote_request(message); break;
            case Message::Type::VOTE_REPLY: handle_vote_reply(message); break;
            case Message::Type::HEARTBEAT: handle_heartbeat(message); break;
        }
    }

    void handle_vote_request(const Message *message) {
        mutex_.acquire();

        if (message->epoch > epoch_) {
            epoch_     = message->epoch;
            state_     = State::FOLLOWER;
            voted_for_ = -1;
        }

        Message reply;
        reply.type   = Message::Type::VOTE_REPLY;
        reply.sender = id_;
        reply.epoch  = epoch_;

        bool granted = false;

        if (message->epoch == epoch_ && (voted_for_ == -1 || voted_for_ == message->sender)) {
            voted_for_        = message->sender;
            election_timeout_ = random_timeout();
            granted           = true;
        }

        reply.granted = granted;
        send(message->sender, &reply);
        mutex_.release();
    }

    void handle_vote_reply(const Message *message) {
        mutex_.acquire();

        if (state_ != State::CANDIDATE) {
            mutex_.release();
            return;
        }

        if (message->epoch > epoch_) {
            epoch_     = message->epoch;
            state_     = State::FOLLOWER;
            voted_for_ = -1;
            mutex_.release();
            return;
        }

        if (message->epoch == epoch_ && message->granted) {
            votes_received_++;

            int majority = (Traits<Topology>::NumberOfNodes / 2) + 1;

            if (votes_received_ == majority) {
                state_ = State::LEADER;
                Debug::Info("New Leader: ", id_, "! ", "For Epoch: ", epoch_);
            }
        }

        mutex_.release();
    }

    void handle_heartbeat(const Message *message) {
        mutex_.acquire();

        if (message->epoch >= epoch_) {
            epoch_            = message->epoch;
            state_            = State::FOLLOWER;
            voted_for_        = -1;
            election_timeout_ = random_timeout();
        }

        mutex_.release();
    }

    int random_timeout() { return 150 + (rand() % 150); }

    void send_heartbeat() {
        Message message;
        message.type   = Message::Type::HEARTBEAT;
        message.sender = id_;
        message.epoch  = epoch_;
        broadcast(&message);
    }

    void check_election_timeout() {
        mutex_.acquire();

        // CORREÇÃO: Se eu já sou o líder ativo, meu timeout de eleição não deve rodar
        if (state_ == State::LEADER) {
            mutex_.release();
            return;
        }

        if (election_timeout_-- == 0) {
            election_timeout_ = random_timeout();
            try_to_get_elected();
        }
        mutex_.release();
    }

    void try_to_get_elected() {
        state_          = State::CANDIDATE;
        voted_for_      = id_;
        votes_received_ = 1;
        epoch_++;

        Message message;
        message.type   = Message::Type::REQUEST_VOTE;
        message.sender = id_;
        message.epoch  = epoch_;
        broadcast(&message);
    }

    void send(int target, const Message *message, int length = sizeof(Message)) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(Traits<Topology>::Port + target);
        addr.sin_addr.s_addr = inet_addr(Traits<Topology>::Address);
        sendto(sockfd_, message, length, 0, (struct sockaddr *)&addr, sizeof(addr));
    }

    void broadcast(const Message *message, int length = sizeof(Message)) {
        for (int i = 0; i < Traits<Topology>::NumberOfNodes; i++) {
            if (i != id_) {
                send(i, message, length);
            }
        }
    }

  private:
    uint32_t id_;
    State state_ = State::FOLLOWER;

    int sockfd_;
    int voted_for_        = -1;
    int votes_received_   = 0;
    uint64_t epoch_       = 0;
    int election_timeout_ = 0;

    Thread ticker_;
    Thread receiver_;
    Mutex mutex_;
};

} // namespace Atomic
