#ifndef MYSQLD_INIT_INCLUDED
#define MYSQLD_INIT_INCLUDED

#include <mutex>
#include <condition_variable>

// Synchronization for converged binary initialization sequence
// This ensures proper order: MySQL basic init → data substrate init → MySQL rest
namespace mysqld_converged_sync {
    // Mutex and condition variables for initialization synchronization
    extern std::mutex init_mutex;
    extern std::condition_variable mysqld_basic_init_done_cv;
    extern std::condition_variable data_substrate_init_done_cv;
    extern bool mysqld_basic_init_done;
    extern bool data_substrate_init_done;
}

// Function to initialize and run mysqld in library mode
// Returns 0 on success, non-zero on error
extern "C" int mysqld_main(int argc, char **argv);

// Function to shutdown mysqld gracefully
extern "C" void mysqld_shutdown();

#endif // MYSQLD_INIT_INCLUDED
