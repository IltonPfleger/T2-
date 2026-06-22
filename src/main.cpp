#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include <time.h>

#include <AuctionNode.hpp>
#include <Traits.hpp>

using namespace Atomic;
using namespace Auction;

class Payload {
  public:
    Payload(int id) {
        srand(time(nullptr) + id * 100);

        AuctionNode node(id);

        usleep(1000000);

        // Lances definidos apenas para facilitar a demonstracao.
        int bids[Traits<Topology>::NumberOfNodes][3] = {
            {100, 180, 250},
            {120, 210, 300},
            {150, 240, 330},
            {130, 270, 360},
            {160, 290, 390}
        };

        for (int round = 0; round < 3; round++) {
            usleep(200000 + (rand() % 700000));

            int amount = bids[id][round];

            node.submitBid(amount, round + 1);
        }

        sleep(8);

        std::printf("[P%d] Finalizando participante do leilão.\n", id);
        std::fflush(stdout);

        _exit(0);
    }

    ~Payload() = default;

    operator int() { return 0; }
};

int main(int argc, char *argv[]) {
    pid_t pids[Traits<Topology>::NumberOfNodes];

    for (int i = 0; i < Traits<Topology>::NumberOfNodes; i++) {
        pid_t pid = fork();

        if (pid == 0) {
            return Payload(i);
        }

        pids[i] = pid;
    }

    for (pid_t pid : pids) {
        waitpid(pid, nullptr, 0);
    }

    return 0;
}