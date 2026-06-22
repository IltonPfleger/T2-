#ifndef __SOCKET__
#define __SOCKET__

#include <arpa/inet.h>
#include <memory>
#include <netinet/in.h>
#include <string.h>

namespace Atomic {

class Socket {
  public:
    Socket(const char *address, int port) {
        sockfd_ = socket(AF_INET, SOCK_DGRAM, 0);

        Debug::Error(sockfd_ < 0, "Can't Create Socket!");

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(port);
        addr.sin_addr.s_addr = inet_addr(address);
        bind(sockfd_, (struct sockaddr *)&addr, sizeof(addr));
    }

    ~Socket() { close(sockfd_); }

    std::unique_ptr<unsigned char[]> receive(size_t &received) {
        sockaddr_in addr;
        socklen_t socklen = sizeof(addr);

        constexpr size_t length = 4096;
        auto buffer             = std::make_unique<unsigned char[]>(length);
        auto *raw               = reinterpret_cast<char *>(buffer.get());

        received = recvfrom(sockfd_, raw, length, 0, reinterpret_cast<sockaddr *>(&addr), &socklen);

        if (received <= 0) return nullptr;

        return buffer;
    }

    auto send(const char *address, int port, const void *buffer, int length) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(port);
        addr.sin_addr.s_addr = inet_addr(address);
        return sendto(sockfd_, buffer, length, 0, (struct sockaddr *)&addr, sizeof(addr));
    }

  private:
    int sockfd_;
};

} // namespace Atomic

#endif
