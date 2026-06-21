#include <pthread.h>

namespace Atomic {

class Mutex {
  public:
    class Guard {
      public:
        Guard(Guard &&)                 = delete;
        Guard(const Guard &)            = delete;
        Guard &operator=(Guard &&)      = delete;
        Guard &operator=(const Guard &) = delete;

        explicit Guard(Mutex &mutex)
            : mutex_(mutex) {
            mutex_.acquire();
        }

        ~Guard() { mutex_.release(); }

      private:
        Mutex &mutex_;
    };

    Mutex() { pthread_mutex_init(&mutex_, 0); }
    ~Mutex() { pthread_mutex_destroy(&mutex_); }

    void acquire() { pthread_mutex_lock(&mutex_); }
    void release() { pthread_mutex_unlock(&mutex_); }

  private:
    pthread_mutex_t mutex_;
};

} // namespace Atomic
