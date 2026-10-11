#include "logger.h"
#include <time.h>
#include <stdarg.h>

/* Get timestamp in "YYYY-MM-DD HH:MM:SS,ms" format */
static void get_timestamp(char *buffer, size_t len) {
    struct timespec ts;
    struct tm tm_info;

    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tm_info);

    int ms = ts.tv_nsec / 1000000;

    snprintf(buffer, len, "%04d-%02d-%02d %02d:%02d:%02d,%03d",
             tm_info.tm_year + 1900,
             tm_info.tm_mon + 1,
             tm_info.tm_mday,
             tm_info.tm_hour,
             tm_info.tm_min,
             tm_info.tm_sec,
             ms);
}

void log_message(log_level_t level, const char *source, const char *fmt, ...) {
    static const char *level_str[] = {"INFO", "WARN", "ERROR"};

    char timestamp[32];
    get_timestamp(timestamp, sizeof(timestamp));

    // Build formatted message
    va_list args;
    va_start(args, fmt);

    printf("%s [%s] %s: ", timestamp, level_str[level], source);
    vprintf(fmt, args);
    


    va_end(args);

    // Optionally flush immediately for RT logging
    fflush(stdout);
}
