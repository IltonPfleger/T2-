#include <pthread.h>

namespace Atomic {

class Thread {
  public:
    Thread() = default;

    Thread(void *(*function)(void *), void *argument) {
        valid_ = true;
        pthread_create(&thread_, 0, function, argument);
    }

    ~Thread() { join(); }

    void join() {
        if (valid_) {
            pthread_join(thread_, nullptr);
        }
    }

  private:
    pthread_t thread_;
    bool valid_ = false;
};

} // namespace Atomic
