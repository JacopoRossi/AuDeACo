#include "execution_manager.h"

/* ----------------- Execution Manager Helper Functions ----------------- */
const char* redis_get_hash_value(redisReply *r, const char *key) {
    for (size_t i = 0; i < r->elements; i += 2) {
        if (strcmp(r->element[i]->str, key) == 0) {
            return r->element[i+1]->str;
        }
    }
    return NULL;
}

GList* json_array_to_gstring_list(JsonArray *array) {
    if (!array) return NULL;

    GList *list = NULL;
    guint length = json_array_get_length(array);

    for (guint i = 0; i < length; i++) {
        const char *value = json_array_get_string_element(array, i);
        
        // Create a new GString object for each element
        GString *gs = g_string_new(value);
        
        // Append the GString pointer to the GList
        list = g_list_append(list, gs);
    }

    return list;
}

GSList* parse_input_list(const gchar *input_data) {
    g_return_val_if_fail(input_data != NULL, NULL);

    JsonParser *parser = json_parser_new();
    GError *error = NULL;

    if (!json_parser_load_from_data(parser, input_data, -1, &error)) {
        g_printerr("[ERROR] JSON Parser: JSON Load %s\n", error->message);
        g_error_free(error);
        g_object_unref(parser);
        return NULL;
    }

    JsonNode *root = json_parser_get_root(parser);
    if (!JSON_NODE_HOLDS_ARRAY(root)) {
        g_printerr("[ERROR] JSON Parser: JSON root is not an array.\n");
        g_object_unref(parser);
        return NULL;
    }

    GSList *list = NULL;
    JsonArray *array = json_node_get_array(root);
    guint length = json_array_get_length(array);

    for (guint i = 0; i < length; i++) {
        JsonObject *obj = json_array_get_object_element(array, i);
        input_t *input_elem = g_new0(input_t, 1);
        
        if (convert_json_to_input(obj, input_elem) == -1) {
            g_free(input_elem);
            g_slist_free_full(list, g_free); 
            g_object_unref(parser);
            return NULL;
        }
        
        /* Prepending is O(1). We will reverse it at the end for O(N) total. */
        list = g_slist_prepend(list, input_elem);
    }

    g_object_unref(parser);
    return g_slist_reverse(list);
}

void free_input_list(GSList *list) {
    /* Assuming input_t is a flat structure, otherwise use a custom free wrapper */
    g_slist_free_full(list, g_free);
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
        g_error("[ERROR] Execution Manager (redis): %s\n", c->errstr);
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
/* ----------------- Executor Manager Getters/Setters ----------------- */
void em_set_leader(execution_manager_t *em, gboolean leader_flag){
    g_return_if_fail(em != NULL);

    em->is_leader = leader_flag;
    return; 
}


/* ----------------- Executor Manager Activities ----------------- */

void em_run_schedule(execution_manager_t *em, schedule_t *sched) {
    g_return_if_fail(em != NULL);
    g_return_if_fail(sched != NULL);

    ipc_msg_t msg;
    gint64 time_zero_us;

    if (em->is_leader) {
        /* --- LEADER LOGIC: Wait for all workers --- */
        g_print("[SYNC] Leader: Waiting for %d images to be READY...\n", 
                g_list_length(sched->schedule_images_queues));

        guint ready_count = 0;
        guint target_count = g_list_length(sched->schedule_images_queues);

        while (ready_count < target_count) {
            // Receive from EM's own queue (Workers send to Leader's queue)
            if (mq_receive(em->em_queue, (char*)&msg, sizeof(msg), NULL) != -1) {
                if (msg.type == MSG_TASK_READY) {
                    ready_count++;
                    g_print("[SYNC] Leader: Worker %d/%d is ready.\n", ready_count, target_count);
                }
            }
            g_usleep(1000); // Prevent CPU pegging
        }

        /* All ready! Set T-Zero to (Now + 500ms) to account for network/latency */
        time_zero_us = g_get_monotonic_time() + 500000; 

        /* Broadcast T-Zero to all workers */
        msg.type = MSG_TASK_SYNC;
        msg.data.sync_time_us = time_zero_us;

        for (GList *l = sched->schedule_images_queues; l != NULL; l = l->next) {
            mqd_t target_qd = (mqd_t)GPOINTER_TO_INT(l->data);

            // Using PRIO_SYNC (20) to jump ahead of any pending task results
            if (mq_send(target_qd, (const char*)&msg, sizeof(msg), PRIO_SYNC) == -1) {
                g_printerr("[ERROR] Failed to send SYNC to worker: %s\n", g_strerror(errno));
            }
        }
        g_print("[SYNC] Leader: Barrier released. Sync time sent.\n");

    } else {
        /* --- WORKER LOGIC: Signal ready and wait --- */
        msg.type = MSG_TASK_READY;
        msg.task_id = 0; // Identifies the container
        
        g_print("[SYNC] Worker: Signaling READY to leader...\n");
        /* Signaling READY with high priority */
        mq_send(sched->schedule_leader_queue, (const char*)&msg, sizeof(msg), PRIO_SYNC);

        /* Wait for the Leader to send the start timestamp */
        g_print("[SYNC] Worker: Waiting for SYNC signal from leader...\n");
        
        while (TRUE) {
            if (mq_receive(em->em_queue, (char*)&msg, sizeof(msg), NULL) != -1) {
                if (msg.type == MSG_TASK_SYNC) {
                    time_zero_us = msg.data.sync_time_us;
                    break;
                }
            }
            g_usleep(1000);
        }
        g_print("[SYNC] Worker: Received sync time. Starting timers...\n");
    }
    g_print("[SYNC] Final Synchronization Complete. T-Zero (Monotonic): %ld us\n", (long)time_zero_us);
    //return;
    /* --- THE REST OF YOUR FUNCTION (Unchanged timers logic) --- */
    GMainLoop *loop = g_main_loop_new(NULL, FALSE);

    

    /* 1. Plan the scheudle DEADLINES */
    for (GList *l = sched->schedule_end_info->head; l != NULL; l = l->next) {
        timeline_entry_t *entry = (timeline_entry_t *)l->data;

        /* CORREZIONE: Uso deadline_context_t invece di context_t */
        deadline_context_t *ctx = g_new0(deadline_context_t, 1);
        ctx->data = entry->data_list;
        ctx->loop = loop;
        ctx->timestamp = entry->timestamp;
        ctx->is_last = (l->next == NULL); // Se è l'ultimo nodo della GQueue
        ctx->sched = sched;

        gint64 target_mono_us = time_zero_us + (entry->timestamp * 1000);

        GSource *source = g_timeout_source_new(0);
        g_source_set_ready_time(source, target_mono_us);
        g_source_set_callback(source, handle_expiration, ctx, NULL);
        g_source_attach(source, g_main_loop_get_context(loop));
        g_source_unref(source);
    }

    /* 2. Plan the schedule STARTS */
    for (GList *l = sched->schedule_start_info->head; l != NULL; l = l->next) {
        timeline_entry_t *entry = (timeline_entry_t *)l->data;

        start_context_t *ctx = g_new0(start_context_t, 1);
        ctx->data = entry->data_list;
        ctx->timestamp = entry->timestamp;
        ctx->sched = sched;

        

        gint64 target_mono_us = time_zero_us + (entry->timestamp * 1000);

        GSource *source = g_timeout_source_new(0);
        g_source_set_ready_time(source, target_mono_us);
        g_source_set_callback(source, handle_initialization, ctx, NULL);
        g_source_attach(source, g_main_loop_get_context(loop));
        g_source_unref(source);
    }

    g_print("[INFO] Execution Manager: Scheduler started! Waiting for events...\n");
    g_main_loop_run(loop);
    
    g_main_loop_unref(loop);
    g_print("[INFO] Execution Manager: Scheduler terminated successfully.\n");
}

void em_wait_for_schedule(execution_manager_t *em) {
    while (1) {
        // Updated to check for the new hash name
        redisReply *r = redisCommand(em->redis_client, "EXISTS schedule_data");
        if (r && r->integer == 1) {
            freeReplyObject(r);
            return;
        }
        if (r) freeReplyObject(r);
        g_print("[INFO] Waiting for schedule_data in Redis...\n");
        sleep(1);
    }
}



schedule_t* em_read_schedule(execution_manager_t *em) {
    // 1. Fetch the entire flat hash from Redis
    redisReply *r = redisCommand(em->redis_client, "HGETALL schedule_data");
    if (!r || r->type == REDIS_REPLY_ERROR || r->elements == 0) {
        if (r) freeReplyObject(r);
        return NULL;
    }

    const char *name = redis_get_hash_value(r, "schedule:name");
    const char *version = redis_get_hash_value(r, "schedule:version");
    const char *leader = redis_get_hash_value(r, "schedule:leader");
    const char *duration_str = redis_get_hash_value(r, "schedule:duration");
    const char *images_json = redis_get_hash_value(r, "schedule:images");

    // 2. Parse the list of image names
    JsonParser *parser = json_parser_new();
    if (!json_parser_load_from_data(parser, images_json, -1, NULL)) {
        g_warning("Failed to parse images JSON");
        freeReplyObject(r);
        return NULL; 
    }

    JsonArray *images_array = json_node_get_array(json_parser_get_root(parser));
    GList* images_list = json_array_to_gstring_list(images_array);

    GList *l, *next;

    for (l = images_list; l != NULL; l = next) {
        next = l->next; // Always capture next at the start of the loop
        GString *gs = (GString *)l->data;

        if (g_strcmp0(em->em_name->str, gs->str) == 0) {
            // Unlink the current node from the list
            images_list = g_list_remove_link(images_list, l);
            
            // Free the data (GString)
            g_string_free(gs, TRUE);
            
            // Free the specific list node structure
            g_list_free_1(l);

            break;
        }
    }
    

    if (!name || !version || !leader || !images_json) {
        freeReplyObject(r);
        return NULL;
    }

    // Set Leader Flag
    em_set_leader(em, g_strcmp0(em->em_name->str, leader) == 0);
    //g_print("[DEBUG] Execution Manager: is_leader %s\n", (g_strcmp0(em->em_name->str, leader) == 0) ? "TRUE" : "FALSE");

    // Create the schedule
    schedule_t *sched = schedule_new(name, version, leader, images_list, duration_str ? g_ascii_strtoll(duration_str, NULL, 10) : 0);

    // Task Loading into the schedule
    gchar *img_name = em->em_name->str;
    char len_key[128];
    snprintf(len_key, sizeof(len_key), "schedule:%s:length", img_name);
    const char *img_task_count_str = redis_get_hash_value(r, len_key);
    int img_task_count = img_task_count_str ? atoi(img_task_count_str) : 0;

    for (int j = 0; j < img_task_count; j++) {
        char task_key[128];
        snprintf(task_key, sizeof(task_key), "schedule:%s:%d", img_name, j);
        const char *task_json = redis_get_hash_value(r, task_key);

        if (task_json) {
            JsonParser *t_parser = json_parser_new();
            if (json_parser_load_from_data(t_parser, task_json, -1, NULL)) {
                JsonObject *t_obj = json_node_get_object(json_parser_get_root(t_parser));

                // --- FIELD MAPPING LOGIC ---

                // A. Numeric fields (Direct)
                guint16 t_id = (guint16)json_object_get_int_member(t_obj, "id");
                gint64 start_time = json_object_get_int_member(t_obj, "start");
                gint64 end_time = json_object_get_int_member(t_obj, "deadline");

                // B. Stringified Numeric fields ("1", "10") -> Use atoi()
                const char *cpu_str = json_object_get_string_member(t_obj, "cpu_affinity");
                const char *prio_str = json_object_get_string_member(t_obj, "priority");
                
                gint cpu = cpu_str ? atoi(cpu_str) : 0;
                gint8 priority = prio_str ? (gint8)atoi(prio_str) : 0;

                // C. Policy String ("fifo", "rr") -> Map to C constants
                const char *policy_name = json_object_get_string_member(t_obj, "policy");
                gint policy = SCHED_OTHER; // Default to 0
                if (policy_name != NULL) {
                    if (g_ascii_strcasecmp(policy_name, "fifo") == 0) {
                        policy = SCHED_FIFO;
                    } else if (g_ascii_strcasecmp(policy_name, "rr") == 0) {
                        policy = SCHED_RR;
                    }
                }

                // D. Inputs (Read as string to prevent JSON nested-node errors)
                const char *json_input = json_object_get_string_member(t_obj, "inputs");
                g_print("[DEBUG] Execution Manager (input): %s\n", json_input ? json_input : "[{}]");

                GSList* t_input =  parse_input_list(g_strdup(json_input ? json_input : "[{}]"));

                schedule_add_task(
                    sched,
                    t_id,
                    img_name,
                    NULL, 
                    policy,
                    priority,
                    cpu,
                    1, 
                    NULL, 
                    start_time,
                    end_time,
                    t_input
                );
            }
            g_object_unref(t_parser);
        }
    }

    g_object_unref(parser);
    freeReplyObject(r);
    return sched;
}


/* ----------------- Executor Manager Usefull Functions ----------------- */

void* task_wrapper_func(void* data){
    
    task_wrapper_input_t* tw_input = (task_wrapper_input_t*)data;
    
    /* Read the thread context arguments */
    guint16 task_id = tw_input->task_id;
    gpointer input = tw_input->data;
    GThreadFunc thread_func = tw_input->thread_func;
    schedule_t* sched = tw_input->sched;

    g_print("[INFO] ThreadCall %u: start thread function.\n", task_id);
    /* Run the thread function */
    gpointer res = task_main(input);

    g_print("[INFO] ThreadCall %u: termination thread function. \n", task_id);


    /* Write the result */
    schedule_set_result(sched, task_id, "{}");

    /* Cleanup */
    g_free(res);
    g_free(tw_input);
    return NULL;

}

/* ----------------- Executor Manager Event Handlers ----------------- */

gboolean handle_initialization(gpointer user_data) {
    start_context_t *ctx = (start_context_t *)user_data;
    GSList *tasks = (GSList *)ctx->data;
    schedule_t* sched = ctx->sched;


    if (tasks == NULL) {
        g_print("[ERROR] Execution Manager: No tasks\n");
        g_free(ctx);
        return G_SOURCE_REMOVE;
    }

    for (GSList *l = tasks; l != NULL; l = l->next) {
        
        /* Read the current task information */
        activation_data_t *task = (activation_data_t *)l->data;

        /* Prepare the thread (wrapper) input */
        task_wrapper_input_t* tw_input = g_new0(task_wrapper_input_t, 1);
        tw_input->task_id = task->task_id;
        tw_input->data = task->input_data->data;
        tw_input->thread_func = task->task_exec;
        tw_input->sched = sched;


        
        /* Prepare the thread */
        pthread_attr_t attr;
        pthread_attr_init(&attr);

        /* Set CPU Affinity core */
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(task->cpu_affinity, &set);
        gint affinity_err = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);
        if (affinity_err != 0) {
            g_warning("[WARNING] Execution Manager: Failed to set CPU affinity for Task ID %u. Error: %d (%s)", task->task_id, affinity_err, g_strerror(affinity_err));
        }

        /* Setting scheduler policy and priority */
        struct sched_param param;
        param.sched_priority = task->priority;

        pthread_attr_setstacksize(&attr, PTHREAD_STACK_MIN);
        pthread_attr_setschedpolicy(&attr, task->policy);
        pthread_attr_setschedparam(&attr, &param);
        pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);

        pthread_t thread;
        gint rc = pthread_create(&thread, &attr, task_wrapper_func, tw_input);
        pthread_attr_destroy(&attr); // Clean up attributes
        if (rc) {
            g_printerr("[ERROR] Execution Manager: pthread_create failed with code %d (%s) for Task ID %u\n", rc, g_strerror(rc), task->task_id);
            continue;
        }
        pthread_detach(thread);

        // Iterate through GSList of dependencies
        if (task->depends_on) {
            g_print("[INFO] Depends On (IDs): ");
            for (GSList *dep = task->depends_on; dep != NULL; dep = dep->next) {
                // Assuming the list stores integers cast to pointers, or pointers to guint
                // If they are pointers to uint: *(guint*)dep->data
                // If they are IDs stored directly in the pointer: GPOINTER_TO_UINT(dep->data)
                g_print("%u ", GPOINTER_TO_UINT(dep->data));
            }
            g_print("\n");
        } else {
            g_print("[INFO] Depends On: None\n");
        }
    }

    g_free(ctx); 
    return G_SOURCE_REMOVE;
}



gboolean handle_expiration(gpointer user_data) {
    deadline_context_t *ctx = (deadline_context_t *)user_data;
    GSList *tasks = (GSList *)ctx->data;

    if (tasks == NULL) {
        g_print("[INFO] Execution Manager: No tasks to expire\n");
    } else {
        for (GSList *l = tasks; l != NULL; l = l->next) {
            expiration_data_t *exp = (expiration_data_t *)l->data;
            
            /* Check if the task is jet completed*/
            if (schedule_is_task_completed(ctx->sched, exp->task_id)) {
                g_print("[INFO] Execution Manager: Task %u already completed.\n", exp->task_id);
                continue;
            }
            g_print("[INFO] Execution Manager: Sent ABORT for Task ID %u\n", exp->task_id);
        }
    }

    if (ctx->is_last) {
        g_print("[INFO] Execution Manager (handle_expiration): Final deadline reached. Quitting...\n");
        g_main_loop_quit(ctx->loop);
    }

    g_free(ctx); 
    return G_SOURCE_REMOVE;
}