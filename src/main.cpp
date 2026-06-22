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

        // semente é diferente por processo pra garantir
        // delays aleatórios independentes entre nós
        srand(time(nullptr) + id * 100);
        
        // cria o nó do leilão com o ID desse processo
        // (abre socket e sobe a thread receptora)
        AuctionNode node(id);

        // espera 1 segundo pra todos os nós terminarem de inicializar
        // antes de qualquer lance ser enviado
        usleep(1000000);

        // tabela fixa de lances: cada linha é um nó, cada coluna é uma rodada
        // ex:
        // bids[0] = {100, 180, 250} → P0 lança 100, depois 180, depois 250
        // bids[1] = {120, 210, 300} → P1 lança 120, depois 210, depois 300
        int bids[Traits<Topology>::NumberOfNodes][3] = {
            {100, 180, 250},
            {120, 210, 300},
            {150, 240, 330},
            {130, 270, 360},
            {160, 290, 390}
        };

        // todos enviam sem delay
        for (int round = 0; round < 3; round++) {
            node.submitBid(bids[id][round], round + 1);
        }

        // espera 8 segundos pra garantir que todas 
        // as mensagens em voo sejam entregues antes de finalizar
        sleep(8);

        std::printf("[P%d] Finalizando participante do leilão.\n", id);
        std::fflush(stdout);

        _exit(0);
    }

    ~Payload() = default;

    operator int() { return 0; }
};

int main() {

    // cria as pastas de logs caso ainda não existam
    system("mkdir -p logs seqs");

    // array pra guardar os PIDs
    pid_t pids[Traits<Topology>::NumberOfNodes];

    // cria N processos filhos via fork()
    // id -> no construtor do payload
    // o pai nunca entra no if, só coleta o PID e continua o loop
    for (int i = 0; i < Traits<Topology>::NumberOfNodes; i++) {
        pid_t pid = fork();
        
        // filho entra aqui
        if (pid == 0) {

            // redireciona o stdcout do filho pro pra arquivos log
            char logfile[64];
            snprintf(logfile, sizeof(logfile), "logs/P%d.log", i);
            freopen(logfile, "w", stdout);

            return Payload(i);
    }
        
        // pai coleta o PID e segue o loop
        pids[i] = pid;
    }

    // espera todos terminarem
    for (pid_t pid : pids) {
        waitpid(pid, nullptr, 0);
    }

    // PARA DEMONSTRAR QUE TODAS SÃO IGUAIS
    // lê cada log (remove sem o [PX])
    system("grep 'ENTREGA' logs/P0.log | sed 's/\\[P[0-9]*\\] //' > seqs/seq0.txt");
    system("grep 'ENTREGA' logs/P1.log | sed 's/\\[P[0-9]*\\] //' > seqs/seq1.txt");
    system("grep 'ENTREGA' logs/P2.log | sed 's/\\[P[0-9]*\\] //' > seqs/seq2.txt");
    system("grep 'ENTREGA' logs/P3.log | sed 's/\\[P[0-9]*\\] //' > seqs/seq3.txt");
    system("grep 'ENTREGA' logs/P4.log | sed 's/\\[P[0-9]*\\] //' > seqs/seq4.txt");

    // Compara todos com P0
    printf("\n=== VERIFICANDO ORDEM DE ENTREGA ===\n");
    int diff01 = system("diff -q seqs/seq0.txt seqs/seq1.txt");
    int diff02 = system("diff -q seqs/seq0.txt seqs/seq2.txt");
    int diff03 = system("diff -q seqs/seq0.txt seqs/seq3.txt");
    int diff04 = system("diff -q seqs/seq0.txt seqs/seq4.txt");

    // printa a diferença (DEVEM SER IGUAIS)
    if (diff01 == 0 && diff02 == 0 && diff03 == 0 && diff04 == 0) {
        printf("ORDEM IDENTICA EM TODOS OS NOS — Atomic Broadcast OK\n");
    } else {
        printf("ORDENS DIFERENTES — FALHA\n");
    }

    return 0;
}