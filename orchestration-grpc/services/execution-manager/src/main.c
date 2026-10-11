#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include <hiredis/hiredis.h>
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <stdint.h>
#include <sched.h>
#include "schedule.h"
#include "grpc_client.h"
#include "realtime_scheduler.h"

#define MAX_TASKS 100
#define MAX_EPOLL_EVENTS 20

// Callback for async task responses
static void task_response_callback(unsigned int task_id,
                                   const char* status,
                                   const char* result_json,
                                   const char* error_message,
                                   void* user_data) {
    const char* task_name = (const char*)user_data;
    
    if (strcmp(status, "STARTED") == 0) {
        printf("✅ [ASYNC] Task '%s' (ID: %u) STARTED - execution manager is FREE!\n", task_name, task_id);
    } else if (strcmp(status, "COMPLETED") == 0) {
        printf("🎉 [ASYNC] Task '%s' (ID: %u) COMPLETED\n", task_name, task_id);
        printf("   [RESULT] %s\n\n", result_json);
    } else if (strcmp(status, "ERROR") == 0) {
        printf("❌ [ASYNC] Task '%s' (ID: %u) ERROR: %s\n\n", task_name, task_id, error_message);
    }
}

// Simple function to extract values from nested JSON format
// Converts {"a": {"type": "int", "value": 10}, "b": {"type": "int", "value": 5}} to {"a": 10, "b": 5}
static void extract_input_values(const char* nested_json, char* simple_json, size_t max_len) {
    strcpy(simple_json, "{");
    const char* search_ptr = nested_json;
    int first = 1;
    
    // Look for pattern: "KEY": { ... "value": NUMBER ...}
    while (1) {
        // Find next quoted string that could be a key
        const char* quote1 = strchr(search_ptr, '\"');
        if (!quote1) break;
        quote1++;
        
        const char* quote2 = strchr(quote1, '\"');
        if (!quote2) break;
        
        // Extract potential key
        char key[64];
        size_t key_len = quote2 - quote1;
        if (key_len >= sizeof(key)) key_len = sizeof(key) - 1;
        memcpy(key, quote1, key_len);
        key[key_len] = '\0';
        
        // Skip keys that are not top-level (like "type", "value")
        if (strcmp(key, "type") == 0 || strcmp(key, "value") == 0) {
            search_ptr = quote2 + 1;
            continue;
        }
        
        // Look for corresponding "value": in the next 100 chars
        const char* value_str = strstr(quote2, "\"value\"");
        if (!value_str || (value_str - quote2) > 100) {
            search_ptr = quote2 + 1;
            continue;
        }
        
        // Find the number after "value":
        const char* num_start = value_str + 7; // skip "value"
        while (*num_start && (*num_start == ' ' || *num_start == ':')) num_start++;
        
        char num[32];
        int ni = 0;
        while (*num_start && ((*num_start >= '0' && *num_start <= '9') || *num_start == '-' || *num_start == '.')) {
            if (ni < (int)sizeof(num) - 1) num[ni++] = *num_start;
            num_start++;
        }
        num[ni] = '\0';
        
        // Add to result
        if (ni > 0) {
            if (!first) strcat(simple_json, ", ");
            strcat(simple_json, "\"");
            strcat(simple_json, key);
            strcat(simple_json, "\": ");
            strcat(simple_json, num);
            first = 0;
        }
        
        search_ptr = num_start;
    }
    
    strcat(simple_json, "}");
}

/**
 * Helper to inspect the internal state of the schedule.
 * It iterates through the sorted GQueues for both Start and End timelines.
 */
void print_schedule_report(schedule_t *sched) {
    if (!sched) return;

    printf("\n--- SCHEDULE REPORT: %s (v%s) ---\n", 
           sched->schedule_name->str, sched->schedule_version->str);
    printf("Total Duration: %ld ms\n", (long)sched->schedule_duration);

    // 1. Inspect Activation Timeline (Start Priority Queue)
    printf("\n[Activation Timeline (Sorted GQueue)]:\n");
    for (GList *l = sched->schedule_start_info->head; l != NULL; l = l->next) {
        timeline_entry_t *entry = (timeline_entry_t *)l->data;
        printf("  Time %ld ms: ", (long)entry->timestamp);
        for (GSList *s = entry->data_list; s != NULL; s = s->next) {
            activation_data_t *act = (activation_data_t *)s->data;
            printf("[%s (ID:%u, Prio:%d)] ", act->task_name->str, act->task_id, act->priority);
        }
        printf("\n");
    }

    // 2. Inspect Expiration Timeline (End Priority Queue)
    printf("\n[Expiration Timeline (Sorted GQueue)]:\n");
    for (GList *l = sched->schedule_end_info->head; l != NULL; l = l->next) {
        timeline_entry_t *entry = (timeline_entry_t *)l->data;
        printf("  Time %ld ms: ", (long)entry->timestamp);
        for (GSList *s = entry->data_list; s != NULL; s = s->next) {
            expiration_data_t *exp = (expiration_data_t *)s->data;
            printf("[%s (ID:%u)] ", exp->task_name->str, exp->task_id);
        }
        printf("\n");
    }

    // 3. Inspect Results Storage (GArray)
    printf("\n[Results Storage (GArray)]:\n");
    for (guint i = 0; i < sched->schedule_results->len; i++) {
        task_result_t *res = &g_array_index(sched->schedule_results, task_result_t, i);
        if (res->output_data != NULL) {
            printf("  Task ID %u: Remaining Runs: %u, JSON: %s\n", 
                   i, res->remaining_runs, res->output_data->str);
        }
    }
    printf("--------------------------------------------\n\n");
}

int main(int argc, char *argv[]) {
    printf("============================================\n");
    printf("   DUAL PRIORITY QUEUE ARCHITECTURE TESTER  \n");
    printf("============================================\n\n");

    // ========================================
    // Set Real-Time Scheduling for Execution Manager
    // ========================================
    struct sched_param param;
    param.sched_priority = 95;  // High priority (range 1-99, higher than task priorities)
    
    if (sched_setscheduler(0, SCHED_FIFO, &param) != 0) {
        perror("Warning: Failed to set RT scheduling (need root or CAP_SYS_NICE)");
        printf("⚠️  Execution Manager will run with SCHED_OTHER\n\n");
    } else {
        printf("✅ Execution Manager running with SCHED_FIFO priority %d\n", 
               param.sched_priority);
        printf("   This ensures precise timing and prevents preemption by tasks\n\n");

        
        // Force SCHED_OTHER as the default for all threads created with NULL attrs.
        // Without this, gRPC internal threads (created via pthread_create(NULL,...))
        // inherit SCHED_FIFO from their creator.  Two SCHED_FIFO threads at the same
        // priority are non-preemptive: if a gRPC thread is running at T=11000ms the
        // main thread cannot be scheduled even though its timerfd fired.
        pthread_attr_t default_attr;
        struct sched_param default_param = {.sched_priority = 45};
        pthread_attr_init(&default_attr);
        pthread_attr_setinheritsched(&default_attr, PTHREAD_EXPLICIT_SCHED);
        pthread_attr_setschedpolicy(&default_attr, SCHED_FIFO);
        pthread_attr_setschedparam(&default_attr, &default_param);
        if (pthread_setattr_default_np(&default_attr) != 0) {
            perror("Warning: pthread_setattr_default_np failed");
        } else {
            printf("✅ Default thread policy set to SCHED_FIFO 45\n");
            printf("   gRPC internal threads: RT scheduling, below task threads (50/65)\n\n");
        }
        pthread_attr_destroy(&default_attr);
        
    }

    // ========================================
    // NUOVO: Connessione Redis
    // ========================================
    // With host networking, use localhost instead of service name
    redisContext *redis = redisConnect("localhost", 6379);
    
    if (redis == NULL || redis->err) {
        if (redis) {
            printf("Redis Error: %s\n", redis->errstr);
            redisFree(redis);
            redis = NULL;
        } else {
            printf("Can't allocate redis context\n");
        }
    }
    
    // ========================================
    // NUOVO: Leggi e Stampa da Redis
    // ========================================
    if (redis && !redis->err) {
        printf("\n[Redis] Reading schedule from Redis...\n\n");
        
        // Leggi informazioni schedule
        redisReply *schedule_reply = redisCommand(redis, "HGETALL schedule");
        
        char *schedule_name = NULL;
        char *schedule_version = NULL;
        int num_tasks = 0;
        int iterations = 1;
        
        if (schedule_reply && schedule_reply->type == REDIS_REPLY_ARRAY) {
            for (size_t i = 0; i < schedule_reply->elements; i += 2) {
                char *key = schedule_reply->element[i]->str;
                char *value = schedule_reply->element[i+1]->str;
                
                if (strcmp(key, "name") == 0) {
                    schedule_name = value;
                } else if (strcmp(key, "version") == 0) {
                    schedule_version = value;
                } else if (strcmp(key, "length") == 0) {
                    num_tasks = atoi(value);
                } else if (strcmp(key, "iterations") == 0) {
                    iterations = atoi(value);
                    if (iterations < 1) iterations = 1;
                }
            }
        }
        
        printf("=== REDIS DATA ===\n");
        printf("Schedule: %s (v%s)\n", 
               schedule_name ? schedule_name : "N/A",
               schedule_version ? schedule_version : "N/A");
        printf("Number of tasks: %d\n", num_tasks);
        printf("Iterations: %d\n\n", iterations);
        
        // Leggi ogni task
        printf("Tasks:\n");
        for (int i = 1; i <= num_tasks; i++) {
            char key[64];
            snprintf(key, sizeof(key), "scheduletask:%d", i);
            
            redisReply *task_reply = redisCommand(redis, "HGETALL %s", key);
            
            if (task_reply && task_reply->type == REDIS_REPLY_ARRAY) {
                char *task_name = NULL;
                char *task_policy = NULL;
                int task_priority = 0;
                int task_start = -1;
                char *task_inputs = NULL;
                
                for (size_t j = 0; j < task_reply->elements; j += 2) {
                    char *field = task_reply->element[j]->str;
                    char *value = task_reply->element[j+1]->str;
                    
                    if (strcmp(field, "name") == 0) {
                        task_name = value;
                    } else if (strcmp(field, "policy") == 0) {
                        task_policy = value;
                    } else if (strcmp(field, "priority") == 0) {
                        task_priority = atoi(value);
                    } else if (strcmp(field, "start") == 0) {
                        task_start = atoi(value);
                    } else if (strcmp(field, "inputs") == 0) {
                        task_inputs = value;
                    }
                }
                
                printf("  %d. Task: %s\n", i, task_name ? task_name : "N/A");
                printf("     Policy: %s, Priority: %d, Start: %d ms\n", 
                       task_policy ? task_policy : "N/A", 
                       task_priority, 
                       task_start);
                printf("     Inputs: %s\n\n", task_inputs ? task_inputs : "N/A");
            }
            
            freeReplyObject(task_reply);
        }
        
        printf("==================\n\n");
        
        // ========================================
        // EXECUTE TASKS with EVENT-DRIVEN SCHEDULER
        // ========================================
        if (num_tasks > 0) {
            // Use the new real-time event-driven scheduler
            // This provides:
            // - Precise timing with timerfd
            // - Timeout enforcement
            // - Event-driven (no sleep)
            // - Full time awareness
            execute_schedule_with_event_loop(redis, num_tasks, iterations);
        }
        
        freeReplyObject(schedule_reply);
        redisFree(redis);
    }
    

    

    printf("Test Suite Finished Successfully.\n");
    return 0;
}