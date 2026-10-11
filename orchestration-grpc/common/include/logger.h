#ifndef LOGGER_H
#define LOGGER_H

#include <stdio.h>

typedef enum {
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR
} log_level_t;

/* Log a formatted message */
void log_message(log_level_t level, const char *source, const char *fmt, ...);

#endif /* LOGGER_H */
