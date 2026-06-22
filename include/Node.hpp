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
        srand(time(nullptr) + id * 1000);

        Debug::Trace("New Node ", id, "!");

        // "127.0.0.1"
        const char *address = Traits<Topology>::Address;
        // 5000 + id
        int port            = Traits<Topology>::Port + id;

        sockfd_             = socket(AF_INET, SOCK_DGRAM, 0);
        // aborta se falhou
        Debug::Error(sockfd_ < 0, "Can't Create Node Socket!");

        // monta o endereço do socket
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(port);
        addr.sin_addr.s_addr = inet_addr(address);

        bind(sockfd_, (struct sockaddr *)&addr, sizeof(addr));

        running_ = true;
        
        // escreve o objeto Thread no espaço já alocado de receiver_
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
        uintmax_t msgId = seed(); // id_ = ID único global por mensagem

        // aloca buffer na heap: espaço do header Packet + payload
        uint8_t *buffer = reinterpret_cast<uint8_t *>(malloc(length + sizeof(Packet)));
        Packet *packet  = reinterpret_cast<Packet *>(buffer);

        // preenche o header
        packet->type    = Packet::Type::MULTICAST;
        packet->sender  = id_;
        packet->id      = msgId;
        packet->order   = 0;
        packet->length  = length;

        // copia o Bid pra logo depois do header do buffer
        memcpy(packet->payload, payload, length);

        // registra que estamos esperando proposals pra esse msgId
        // dentro do mutex e antes de enviar - evita race com onPropose
        {
            Mutex::Guard guard(mutex_);
            Collecting &c   = collecting_[msgId];
            c.payloadLength = length;
        }

        // envia pra todos os N nós (incluindo si mesmo)
        // (em ordem aleatória)
        std::vector<int> order;
        for (int i = 0; i < Traits<Topology>::NumberOfNodes; i++) order.push_back(i);
        std::shuffle(order.begin(), order.end(), std::default_random_engine(msgId));

        for (int i : order)
            send(i, packet, length + sizeof(Packet));

        free(buffer);
        return true;
    }

    void send(int target, const Packet *packet, int length) {
        // delay aleatório só em pacotes MULTICAST
        // simula latência de rede diferente pra cada destino
        if (packet->type == Packet::Type::MULTICAST) {
            usleep(rand() % 300000); // até 300ms de delay
        }

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
        // tudo aqui é protegido
        Mutex::Guard guard(mutex_);

        // log de chegada
        std::printf("[P%u] CHEGADA | msg de P%ju chegou\n", id_, packet->sender);
        std::fflush(stdout);

        // cria entrada em pending pra essa mensagem
        Pending &p = pending_[packet->id];
        p.origin   = packet->sender;
        // copia o bid
        p.payload.assign(packet->payload, packet->payload + packet->length);

        // PROPOSTA: maior que tudo já acordado e que a última proposta desse nó
        priorityCounter_ = std::max(priorityCounter_, maxAgreed_) + 1;
        p.proposedOrder  = priorityCounter_;
        
        // sem mensagem acordada ainda
        p.agreedOrder    = 0;

        // manda PROPOSE só pro sender original
        sendPropose(packet->sender, packet->id, p.proposedOrder);
    }

    void onPropose(const Packet *packet) {
        Mutex::Guard guard(mutex_);

        // se não encontrar em collecting_, ignora
        // segurança (não deveria acontecer)
        auto it = collecting_.find(packet->id);
        if (it == collecting_.end()) return;
        Collecting &c = it->second;

        // atualiza o máximo -> desempate pelo maior sender ID
        if (packet->order > c.maxOrder || (packet->order == c.maxOrder && packet->sender > c.maxProposer)) {
            c.maxOrder    = packet->order;
            c.maxProposer = packet->sender;
        }
        c.received++;

        // só age quando chegou PROPOSE de todos os N nós
        if (c.received == Traits<Topology>::NumberOfNodes) {
            sendCommit(packet->id, c.maxOrder); // manda o máximo pra todos
            collecting_.erase(it);              // não precisa mais coletar
        }
    }

    void onCommit(const Packet *packet) {
        Mutex::Guard guard(mutex_);

        // se o commit chega antes do multicast, ignora 
        auto it = pending_.find(packet->id);
        if (it == pending_.end()) return;
        Pending &p = it->second;

        // grava a ordem final decidida pelo sender
        p.agreedOrder = packet->order;

        // atualiza o teto global de ordens já acordadas
        maxAgreed_    = std::max(maxAgreed_, p.agreedOrder);

        // toda vez que uma ordem é acordada, tenta entregar o que for possível
        deliverReady();
    }

    void deliverReady() {
        // loop porque uma entrega pode desbloquear outra em sequência:
        // ex: msgs com ordem 3 e 4 chegam commit ao msm tempo
        // entrega a 3, volta pro loop, entrega 4
        while (true) {
            uintmax_t bestId    = 0;
            uintmax_t bestOrder = 0;
            bool found          = false;

            for (auto &kv : pending_) {
                Pending &p = kv.second;

                // já entregue, pula
                if (p.delivered) continue;
                
                // Regra do ISIS:
                // se qualquer mensagem ainda não tem ordem acordada:
                // para tudo: (ela pode receber uma ordem menor que as que já têm commit)
                // entregar antes seria violar a ordem total garantida pelo algoritmo
                if (p.agreedOrder == 0) {
                    return;
                }
                // busca a de menor ordem entre as prontas
                if (!found || p.agreedOrder < bestOrder) {
                    found     = true;
                    bestOrder = p.agreedOrder;
                    bestId    = kv.first;
                }
            }

            // todas já entregues
            if (!found) return;

            // marca como entregue antes de chamar o deliever()
            Pending &p  = pending_[bestId];
            p.delivered = true;
            deliver(p.payload.data(), p.payload.size());
            // loop: pode ter mais pra entregar em sequência
        }
    }

    virtual void deliver(const void *payload, int length) {
        Debug::Trace("Node ", id_, " delivers message of length ", length);
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
