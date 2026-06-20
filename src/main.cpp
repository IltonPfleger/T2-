#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include <Node.hpp>
#include <Traits.hpp>

using namespace Atomic;

class Payload {
  public:
    Payload(int id) {
        Node _(id);
        while (1)
            ;
    }

    ~Payload() = default;

    operator int() { return 0; }
};

int main(int argc, char *argv[]) {
    pid_t pids[Traits<Topology>::NumberOfNodes];

    for (int i = 0; i < Traits<Topology>::NumberOfNodes; i++) {
        pid_t pid = fork();

        // CHILD - PAYLOAD NODE
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
