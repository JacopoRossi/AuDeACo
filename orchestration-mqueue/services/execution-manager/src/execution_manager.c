#include "execution_manager.h"

/* ----------------- Helpers ----------------- */
const char* redis_get_hash_value(redisReply *r, const char *key) {
    if (r->type != REDIS_REPLY_ARRAY) return NULL;
    for (size_t i = 0; i < r->elements; i += 2) {
        if (strcmp(r->element[i]->str, key) == 0) {
            return r->element[i+1]->str;
        }
    }
    return NULL;
}

int get_sched_policy(const char *t_policy) {
    // g_strcmp0 è sicuro anche se t_policy è NULL
    if (g_strcmp0(t_policy, "rr") == 0) {
        return SCHED_RR;
    } else if (g_strcmp0(t_policy, "fifo") == 0) {
        return SCHED_FIFO;
    } else if (g_strcmp0(t_policy, "other") == 0) {
        return SCHED_OTHER;
    }

    // Fallback per NULL o stringhe sconosciute (es. "normal", "batch", vuote)
    return SCHED_OTHER;
}

/* ----------------- Executor Manager Constructor/Distructors ----------------- */
execution_manager_t* em_new(const gchar *name){
    g_return_val_if_fail(name != NULL, NULL);

    execution_manager_t *em = g_new0(execution_manager_t, 1);
    em->em_name = g_string_new(name);

    GString *q_name = g_string_new(NULL);
    g_string_printf(q_name, "/%s_q", em->em_name->str);

    struct mq_attr attr = {
        .mq_flags = 0,
        .mq_maxmsg = 10,           
        .mq_msgsize = sizeof(ipc_msg_t), 
        .mq_curmsgs = 0
    };
    mqd_t qd = mq_open(q_name->str, O_RDONLY | O_CREAT | O_NONBLOCK, 0644, &attr);
    if (qd == (mqd_t)-1) {
            g_error("[ERROR] Execution Manager (%s) : mq_open failed.", q_name->str);
    }
    em->em_queue = qd;
    g_string_free(q_name, TRUE);

    redisContext *c = redisConnect("redis", 6379);
    if (c != NULL && c->err ){
        g_error("[ERROR] Execution Manager: %s\n", c->errstr);
    }
    em->redis_client = c;

    return em;
}


void em_free(execution_manager_t *em){
    if (!em) return;

    g_string_free(em->em_name, TRUE);
    if (em->em_queue != (mqd_t)-1) {
        mq_close(em->em_queue);
        em->em_queue = (mqd_t)-1;
    }

    redisFree(em->redis_client);

    g_free(em);
}

/* ----------------- Exection Manager Activities ----------------- */
void em_run_schedule(execution_manager_t *em, schedule_t *sched) {
    g_return_if_fail(em != NULL);
    g_return_if_fail(sched != NULL);

    /* 0. Drain any stale messages from previous runs */
    ipc_msg_t stale_msg;
    g_print("[INFO] Execution Manager: Draining stale messages from the queue...\n");
    while (mq_receive(em->em_queue, (char *)&stale_msg, sizeof(ipc_msg_t), NULL) != -1) {
        g_print("[INFO] Execution Manager: Discarded stale message for Task ID %u\n", stale_msg.task_id);
    }

    // The loop ends when mq_receive returns -1. We expect errno to be EAGAIN because the queue is now empty.
    if (errno != EAGAIN) {
        g_printerr("[WARNING] Execution Manager: Unexpected error while draining queue");
    }

    GMainLoop *loop = g_main_loop_new(NULL, FALSE);
    gint64 time_zero_us = g_get_monotonic_time();

    /* 1. Listen for incoming results from Task Wrappers */
    GIOChannel *channel = g_io_channel_unix_new(em->em_queue);
    g_io_channel_set_encoding(channel, NULL, NULL); // Binary messages
    g_io_channel_set_buffered(channel, FALSE);
 
    result_context_t *result_ctx = g_new0(result_context_t, 1);
    result_ctx->em = em;
    result_ctx->sched = sched;
 
    // Store the ID of the watch to remove it later
    guint result_watch_id = g_io_add_watch(channel, G_IO_IN, (GIOFunc)handle_result_message, result_ctx);
 

    /* 2. Plan the Deadlines */
    for (GList *l = sched->schedule_end_info->head; l != NULL; l = l->next) {
        timeline_entry_t *entry = (timeline_entry_t *)l->data;

        deadline_context_t *ctx = g_new0(deadline_context_t, 1);
        ctx->data = entry->data_list;
        ctx->loop = loop;
        ctx->timestamp = entry->timestamp;
        ctx->is_last = (l->next == NULL); // Check if it's the last node
        ctx->sched = sched;

        gint64 target_mono_us = time_zero_us + (entry->timestamp * 1000);

        GSource *source = g_timeout_source_new(0);
        g_source_set_ready_time(source, target_mono_us);
        g_source_set_callback(source, handle_expiration, ctx, NULL);
        g_source_attach(source, g_main_loop_get_context(loop));
        g_source_unref(source);
    }

    /* 3. Plan the Starts */
    for (GList *l = sched->schedule_start_info->head; l != NULL; l = l->next) {
        timeline_entry_t *entry = (timeline_entry_t *)l->data;

        start_context_t *ctx = g_new0(start_context_t, 1);
        ctx->data = entry->data_list;
        ctx->timestamp = entry->timestamp;

        gint64 target_mono_us = time_zero_us + (entry->timestamp * 1000);

        GSource *source = g_timeout_source_new(0);
        g_source_set_ready_time(source, target_mono_us);
        g_source_set_callback(source, handle_initialization, ctx, NULL);
        g_source_attach(source, g_main_loop_get_context(loop));
        g_source_unref(source);
    }

    g_print("[INFO] Execution Manager: Scheduler started. Waiting for events...\n");
    g_main_loop_run(loop);
    
    /* --- Cleanup of event sources --- */
    // This is crucial to prevent dangling pointers and race conditions in the next main loop iteration
    g_source_remove(result_watch_id);
    g_io_channel_unref(channel);
    g_free(result_ctx);

    g_main_loop_unref(loop);
    g_print("[INFO] Execution Manager: Scheduler terminated successfully.\n");
}

void em_wait_for_schedule(execution_manager_t *em){
    while (1) {
        redisReply *r = redisCommand(em->redis_client, "EXISTS schedule");
        if (r && r->integer == 1) {
            freeReplyObject(r);
            return;
        }
        if (r) freeReplyObject(r);
        sleep(1);
    }
}

schedule_t* em_read_schedule(execution_manager_t *em) {
    // 1. Fetch main schedule metadata from Redis hash "schedule"
    redisReply *r = redisCommand(em->redis_client, "HGETALL schedule");
    if (!r || r->type == REDIS_REPLY_ERROR || r->elements == 0) {
        if (r) freeReplyObject(r);
        return NULL;
    }

    const char *name = redis_get_hash_value(r, "name");
    const char *version = redis_get_hash_value(r, "version");
    const char *s_len = redis_get_hash_value(r, "length");

    if (!name || !version || !s_len) {
        freeReplyObject(r);
        return NULL;
    }

    // Initialize the schedule object using the provided constructor
    schedule_t *sched = schedule_new(name, version);
    int task_count = atoi(s_len);
    freeReplyObject(r);

    /* ---------- Tasks Loading Loop ---------- */
    for (int i = 1; i <= task_count; i++) {
        char key[64];
        snprintf(key, sizeof(key), "scheduletask:%d", i);
        
        // Fetch specific task data using the key generated by Python (scheduletask:i)
        redisReply *tr = redisCommand(em->redis_client, "HGETALL %s", key);
        if (!tr || tr->type != REDIS_REPLY_ARRAY || tr->elements == 0) {
            if (tr) freeReplyObject(tr);
            continue;
        }

        // Extracting fields according to the Python loader mapping
        const char *t_id_str = redis_get_hash_value(tr, "id");
        const char *t_image = redis_get_hash_value(tr, "image"); // Map "image" to name
        const char *t_start = redis_get_hash_value(tr, "start");
        const char *t_end = redis_get_hash_value(tr, "deadline");
        const char *t_cpu = redis_get_hash_value(tr, "cpu_affinity");
        const char *t_policy = redis_get_hash_value(tr, "policy");
        const char *t_prio = redis_get_hash_value(tr, "priority");
        const char *t_input = redis_get_hash_value(tr, "inputs");
        // const char *t_deps = redis_get_hash_value(tr, "depends_on"); // JSON array string

        // Convert Redis string values to appropriate C/GLib types
        guint16 task_id = t_id_str ? (guint16)atoi(t_id_str) : (guint16)i;

        gint policy = get_sched_policy(t_policy);
        gint8 priority = t_prio ? (gint8)atoi(t_prio) : 0;
        gint cpu = t_cpu ? atoi(t_cpu) : -1;


        
        // Use GLib functions for 64-bit integer conversion (timestamps)
        gint64 start_time = t_start ? g_ascii_strtoll(t_start, NULL, 10) : 0;
        gint64 end_time = t_end ? g_ascii_strtoll(t_end, NULL, 10) : 0;

        /**
         * Note on depends_on: 
         * The Python loader sends a JSON list. For a simple implementation,
         * we pass NULL here. If dependencies are needed, you should parse 
         * the t_deps string and populate a GSList* of IDs.
         */
        GSList *depends_on = NULL; 

        // Add the task to the schedule via the official method
        schedule_add_task(
            sched, 
            task_id, 
            t_image, 
            policy, 
            priority, 
            cpu, 
            1,            // Default repetition to 1
            depends_on, 
            start_time, 
            end_time, 
            t_input //g_strdup(t_input)
        );

        // Clean up the reply object for this specific task
        freeReplyObject(tr);
    }

    return sched; // Return the fully populated schedule
}


/* ----------------- Execution Manager Event Handlers ----------------- */
gboolean handle_initialization(gpointer user_data) {
    start_context_t *ctx = (start_context_t *)user_data;
    GSList *tasks = (GSList *)ctx->data;

    if (tasks == NULL) {
        g_print("[ERROR] Execution Manager: in handle_initialization No tasks\n");
        g_free(ctx);
        return G_SOURCE_REMOVE;
    }

    for (GSList *l = tasks; l != NULL; l = l->next) {
        activation_data_t *task = (activation_data_t *)l->data;
        ipc_msg_t msg;
        
        /* Clean the memory for avoid dirty messages */
        memset(&msg, 0, sizeof(ipc_msg_t));

        /* Prepare the message */
        msg.task_id = task->task_id;
        msg.type = MSG_TASK_REQUEST;
        msg.data.task_request.policy = task->policy;
        msg.data.task_request.priority = task->priority;
        msg.data.task_request.cpu_affinity = task->cpu_affinity;
        msg.data.task_request.repetition = task->repetition;

        if (task->input_data && task->input_data->str) {
            g_strlcpy(msg.data.task_request.input_data, 
                      task->input_data->str, 
                      MAX_TASK_JSON_IN);
        }

        /* Check if task->task_queue was open correctly */
        if (mq_send(task->task_queue, (const char *)&msg, sizeof(ipc_msg_t), 0) == -1) {
            g_printerr("[ERROR] Execution Manager: mq_send (REQUEST) failed");
        }
        
        g_print("[INFO] Execution Manager: Sent REQUEST for Task ID %u\n", task->task_id);
    }

    g_free(ctx); 
    return G_SOURCE_REMOVE;
}


gboolean handle_result_message(GIOChannel *source, GIOCondition condition, gpointer user_data) {
    result_context_t *ctx = (result_context_t *)user_data;
    execution_manager_t *em = ctx->em;
    ipc_msg_t msg;
    unsigned int priority;

    (void)source; 

    if (condition & G_IO_IN) {
        
        while (mq_receive(em->em_queue, (char *)&msg, sizeof(ipc_msg_t), &priority) != -1) {
            
            if (msg.type == MSG_TASK_RESULT) {

                g_print("[INFO] Execution Manager: Received result for Task ID %u: %s\n", msg.task_id, msg.data.result);
                
                // Qui puoi aggiungere la logica per marcare il task come completato nello schedule.
                // Esempio: schedule_set_result(ctx->sched, msg.task_id, msg.data.result);
                schedule_set_result(ctx->sched, msg.task_id, msg.data.result);

            } else {
                g_warning("[INFO] Execution Manager: Received unaspected type message %d.", msg.type);
            }
        }

        if (errno != EAGAIN) {
            g_printerr("[ERROR] Execution Manager: mq_receive failed, unaspected error: %s", g_strerror(errno));
        }
    }

    return TRUE; 
}


gboolean handle_expiration(gpointer user_data) {
    deadline_context_t *ctx = (deadline_context_t *)user_data;
    GSList *tasks = (GSList *)ctx->data;

    if (tasks == NULL) {
        g_print("[INFO] Execution Manager: No tasks to expire\n");
    } else {
        for (GSList *l = tasks; l != NULL; l = l->next) {
            expiration_data_t *exp = (expiration_data_t *)l->data;
            
            if (schedule_is_task_completed(ctx->sched, exp->task_id)) {
                g_print("[INFO] Execution Manager: Task %u already completed. No ABORT sent.\n", exp->task_id);
                continue; 
            }
            
            /* ---- Part only for test (disable the abort messages ) ----------- */
            g_print("[INFO] Execution Manager: Task %u NOT completed.\n", exp->task_id);
            continue;
            /* ---------------------------------------------------------------- */

            ipc_msg_t msg;
            memset(&msg, 0, sizeof(ipc_msg_t));

            msg.task_id = exp->task_id; 
            msg.type = MSG_TASK_ABORT;

    
            if (mq_send(exp->task_queue, (const char *)&msg, sizeof(ipc_msg_t), 0) == -1) {
                g_printerr("[ERROR] Execution Manager: mq_send (ABORT) failed");
            }

            g_print("[INFO] Execution Manager: Sent ABORT for Task ID %u\n", exp->task_id);
        }
    }

    if (ctx->is_last) {
        g_print("[INFO] Execution Manager: Final deadline reached. Quitting...\n");
        g_main_loop_quit(ctx->loop);
    }

    g_free(ctx); 
    return G_SOURCE_REMOVE;
}