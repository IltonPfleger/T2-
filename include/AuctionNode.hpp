#ifndef __AUCTION_NODE__
#define __AUCTION_NODE__

#include <Node.hpp>
#include <cstdio>
#include <cstring>
#include <vector>

namespace Auction {

struct Bid {
    int bidderId;
    int amount;
    int round;
};

class AuctionNode : public Atomic::Node {
  public:
    explicit AuctionNode(int id)
        : Atomic::Node(id),
          id_(id),
          deliverySequence_(0),
          highestBid_(0),
          winnerId_(-1) {}

    void bid(int amount, int round) {
        Bid bid;
        bid.bidderId = id_;
        bid.amount   = amount;
        bid.round    = round;

        std::printf("[P%d] Enviando lance: R$ %d | tentativa %d\n", id_, amount, round);
        std::fflush(stdout);

        broadcast(&bid, sizeof(Bid));
    }

  private:
    void deliver(const void *payload, int length) override {
        if (length != sizeof(Bid)) {
            std::printf("[P%d] Mensagem inválida recebida. Tamanho: %d\n", id_, length);
            std::fflush(stdout);
            return;
        }

        Bid bid;
        std::memcpy(&bid, payload, sizeof(Bid));

        deliverySequence_++;

        history_.push_back(bid);

        if (bid.amount > highestBid_) {
            highestBid_ = bid.amount;
            winnerId_   = bid.bidderId;
        }

        std::printf("[P%d] ENTREGA #%02d | Participante P%d deu lance R$ %d | Maior atual: R$ %d por P%d\n", id_,
                    deliverySequence_, bid.bidderId, bid.amount, highestBid_, winnerId_);

        std::fflush(stdout);
    }

  private:
    int id_;
    int deliverySequence_;
    int highestBid_;
    int winnerId_;
    std::vector<Bid> history_;
};

} // namespace Auction

#endif
