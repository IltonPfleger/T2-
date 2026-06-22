#include <fstream>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <string>

#include <AuctionNode.hpp>
#include <Traits.hpp>

using namespace Atomic;
using namespace Auction;

class Launcher {
  public:
    Launcher(int id) {
        srand(time(nullptr) ^ (getpid() << 16) ^ id);

        AuctionNode node(id);

        usleep(5'000'000);

        for (int round = 0; round < 5; round++) {
            int bid = 100 + rand() % 401;

            std::printf("[P%d] Rodada %d -> lance %d\n", id, round, bid);
            std::fflush(stdout);

            node.bid(bid, round);
        }

        usleep(5'000'000);

        std::fflush(stdout);

        exit(0);
    }

    ~Launcher() = default;

    operator int() { return 0; }
};

std::vector<std::string> ENTREGA(int id) {
    std::vector<std::string> sequencia;
    std::ifstream arquivo("logs/P" + std::to_string(id) + ".log");
    std::string linha;

    while (std::getline(arquivo, linha)) {
        size_t pos = linha.find("ENTREGA");
        if (pos != std::string::npos) {
            sequencia.push_back(linha.substr(pos));
        }
    }
    return sequencia;
}

int main() {
    constexpr int N = Traits<Topology>::NumberOfNodes;

    system("mkdir -p logs");

    pid_t pids[N];

    for (int i = 0; i < N; i++) {
        pid_t pid = fork();

        if (pid == 0) {
            char cmd[256];
            snprintf(cmd, sizeof(cmd), "tee logs/P%d.log", i);
            FILE *tee = popen(cmd, "w");

            if (tee != nullptr) {
                dup2(fileno(tee), STDOUT_FILENO);
            }

            return Launcher(i);
        }

        pids[i] = pid;
    }

    for (int i = 0; i < N; i++) {
        waitpid(pids[i], nullptr, 0);
    }

    printf("\n=== VERIFICANDO ORDEM DE ENTREGA ===\n");

    bool ok        = true;
    auto reference = ENTREGA(0);

    for (int i = 1; i < N; i++) {
        if (ENTREGA(i) != reference) {
            printf("Diferença encontrada entre P0 e P%d\n", i);
            ok = false;
        }
    }

    if (ok) {
        printf("ORDEM IDENTICA EM TODOS OS NOS — Atomic Broadcast OK\n");
    } else {
        printf("ORDENS DIFERENTES — FALHA\n");
    }

    return 0;
}
