#include "task_wrapper.h"


/* ----------------- Helper Functions ----------------- */


static void task_session_list_free(gpointer data) {
    GSList *list = (GSList *)data;
    if (list) {
        g_slist_free_full(list, g_free);
    }
}

/* ----------------- Constructor/Distructor ----------------- */

task_wrapper_t* task_wrapper_new(const gchar *task_name, const gchar *task_queue_name, const gchar *em_queue_name) {
    g_return_val_if_fail(task_name != NULL, NULL);
    g_return_val_if_fail(task_queue_name != NULL, NULL);
    g_return_val_if_fail(em_queue_name != NULL, NULL);


    /* Create the task wrapper structure */
    task_wrapper_t *tw = g_new0(task_wrapper_t, 1);
    tw->task_name = g_string_new(task_name);
    tw->task_queue_name = g_string_new(task_queue_name);

    struct mq_attr attr = {
        .mq_flags = 0,
        .mq_maxmsg = 10,           
        .mq_msgsize = sizeof(ipc_msg_t), 
        .mq_curmsgs = 0
    };

    /* Open the incoming task queue (Receiver) */
    tw->task_queue = mq_open(tw->task_queue_name->str, O_RDONLY | O_CREAT | O_NONBLOCK, 0644, &attr);
    if (tw->task_queue == (mqd_t)-1) {
        g_error("[ERROR] %s: mq_open failed for %s", task_name, tw->task_queue_name->str);
    }

    /* Setup TX Queue (Connection to Execution Manager) */
    /* Polling Loop: Wait until the Manager creates its queue */
    g_print("[INFO] %s: Waiting for Execution Manager queue '%s'...\n", task_name, em_queue_name);
    while (TRUE) {
        /* Attempt to open the queue in write-only mode. DO NOT create it. */
        tw->em_queue = mq_open(em_queue_name, O_WRONLY | O_NONBLOCK);

        if (tw->em_queue != (mqd_t)-1) {
            g_print("[INFO] %s: Connected to Execution Manager queue.\n", task_name);
            break; // Success
        }

        if (errno == ENOENT) {
            g_usleep(500000); // Wait 0.5 seconds and retry
        } else {
            // For other errors (e.g., permissions), it's a fatal issue.
            g_error("[ERROR] %s: mq_open failed for EM queue '%s': %s", task_name, em_queue_name, g_strerror(errno));
        }
    }

    /* Initialize the Session Hash Table: Session ID (guint16) -> GSList of pthread_t* */
    tw->task_session_table = g_hash_table_new_full(
        g_direct_hash, 
        g_direct_equal, 
        NULL, 
        task_session_list_free
    );

    return tw;
}

void task_wrapper_free(task_wrapper_t *tw) {
    if (!tw) return;

    g_string_free(tw->task_name, TRUE);
    if (tw->task_queue != (mqd_t)-1) {
        mq_close(tw->task_queue);
        /* Unlink the message queue to remove it from the system */
        mq_unlink(tw->task_queue_name->str);
    }
    g_string_free(tw->task_queue_name, TRUE);
    if (tw->em_queue != (mqd_t)-1) mq_close(tw->em_queue);

    /* This will recursively call task_session_list_free on all remaining sessions */
    g_hash_table_destroy(tw->task_session_table);

    g_free(tw);
}

/* ----------------- Manager Threads/Sessions ----------------- */

void task_wrapper_add_thread(task_wrapper_t *tw, guint16 session_id, pthread_t thread_id) {
    g_return_if_fail(tw != NULL);

    gpointer key = GINT_TO_POINTER(session_id);
    GSList *list = g_hash_table_lookup(tw->task_session_table, key);
    
    pthread_t *p_tid = g_new(pthread_t, 1);
    *p_tid = thread_id;

    if (list != NULL) {
        // Steal the list from the hash table to modify it.
        g_hash_table_steal(tw->task_session_table, key);

        // Insert the list element 
        list = g_slist_prepend(list, p_tid);
    } else {
        list = g_slist_prepend(NULL, p_tid);
    }

    g_hash_table_insert(tw->task_session_table, key, list);
}



void task_wrapper_remove_thread(task_wrapper_t *tw, guint16 session_id, pthread_t thread_id) {
    g_return_if_fail(tw != NULL);

    gpointer key = GINT_TO_POINTER(session_id);
    GSList *list = g_hash_table_lookup(tw->task_session_table, key);

    if (!list) return; 

    GSList *iterator = list;
    while (iterator != NULL) {
        pthread_t *stored_tid = (pthread_t *)iterator->data;
        if (pthread_equal(*stored_tid, thread_id)) {
            
            // Steal the list from the hash table to modify it.
            g_hash_table_steal(tw->task_session_table, key);
            
            // Remove the list element. g_slist_delete_link is O(1) since we have the iterator.
            list = g_slist_delete_link(list, iterator);
            
            // If the list is not empty, put it back in the hash table.
            if (list != NULL) {
                g_hash_table_insert(tw->task_session_table, key, list);
            }
            
            // Now that the data is no longer referenced by the list, it's safe to free it.
            g_free(stored_tid);
            
            return;
        }
        iterator = iterator->next;
    }
}


GSList* task_wrapper_get_threads(task_wrapper_t *tw, guint16 session_id) {
    g_return_val_if_fail(tw != NULL, NULL);
    g_return_val_if_fail(tw->task_session_table != NULL, NULL);

    /* Search the original list in the table */
    GSList *original_list = g_hash_table_lookup(tw->task_session_table, GINT_TO_POINTER(session_id));

    if (original_list == NULL) {
        return NULL;
    }

    return g_slist_copy(original_list);
}

/**
 * @brief Cleanup handler for worker threads.
 * 
 * This function is registered with pthread_cleanup_push and is guaranteed to be
 * called when a thread is cancelled or exits. It frees all resources
 * associated with a task's execution.
 */
static void thread_resource_cleanup_handler(void* arg) {
    task_input_t *input_data = (task_input_t *)arg;
    if (!input_data) return;

    g_print("[INFO] thread %lu: Running cleanup for Session ID %u\n", pthread_self(), input_data->session_id);
    
    // Free resources owned by the thread's context
    g_free(input_data->output); // It's safe to call g_free on NULL
    g_free(input_data->input);
    g_free(input_data);
}

void * task_wrapper_run_thread(void *arg){
    task_input_t *input_data = (task_input_t *)arg;
    task_wrapper_t *tw = input_data->tw;
    guint16 sid = input_data->session_id;
    pthread_t self = pthread_self();

    // Register the cleanup handler. It will be called if the thread is cancelled
    // or when pthread_cleanup_pop(1) is called on normal exit.
    pthread_cleanup_push(thread_resource_cleanup_handler, input_data);
    
    /* Execution of the real task */
    gpointer res = task_main(input_data->input);

    input_data->output = (output_t*) res; 

    /* Send the result to the em_queue */
    if (input_data->output != NULL) {
        ipc_msg_t msg_out;
        memset(&msg_out, 0, sizeof(ipc_msg_t));

        /* Prepare the message */

        msg_out.type = MSG_TASK_RESULT;
        msg_out.task_id = sid;

        /* Convert the output in a JSON string*/
        gchar *json_res = convert_output_to_json(input_data->output);

        if (json_res != NULL) {
            /* Copy the string inside the payload of the message */
            g_strlcpy(msg_out.data.result, json_res, MAX_TASK_JSON_OUT);
            g_free(json_res);

            /* Send the message on the EM queue */
            if (mq_send(tw->em_queue, (const char *)&msg_out, sizeof(ipc_msg_t), 0) == -1) {
                g_warning("[WARNING] thread %lu : mq_send result failed: %s", self, g_strerror(errno));
            } else {
                g_print("[INFO] thread %lu : Result sent to EM for Session ID: %u\n", self, sid);
            }
        }
    }

    // We are exiting normally. Pop the cleanup handler and execute it to free all thread resources.
    pthread_cleanup_pop(1);

    /*
     * After a thread has finished its work (either normally or by cancellation),
     * we must still inform the main thread to remove the thread ID from its tracking table.
     */
    thread_cleanup_data_t *cleanup = g_new0(thread_cleanup_data_t, 1);
    cleanup->tw = tw;
    cleanup->session_id = sid;
    cleanup->thread_id = self;
    g_main_context_invoke(NULL, (GSourceFunc)handle_end_thread, cleanup);

    return NULL;
}
/* ---- JSON Parsing Operations ---- */

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





/* ----------------- Message Event Handlers ----------------- */
gboolean handle_input_message(GIOChannel *source, GIOCondition condition, gpointer user_data) {
    task_wrapper_t *tw = (task_wrapper_t *)user_data;
    ipc_msg_t msg;
    unsigned int priority;

    if (condition & G_IO_IN) {
        /* Loop to drain all available messages in a single callback invocation */
        while (mq_receive(tw->task_queue, (char *)&msg, sizeof(ipc_msg_t), &priority) != -1) {
            /* A message was successfully received, process it */
            g_print("[INFO] Message for Session ID: %u\n", msg.task_id);

            switch (msg.type) {
                case MSG_TASK_REQUEST: {
                    
                    sched_policy_t policy = msg.data.task_request.policy;
                    gint8 priority = msg.data.task_request.priority;
                    gint8 cpu_affinity = msg.data.task_request.cpu_affinity;
                    guint8 repetition = msg.data.task_request.repetition;
                    (void)repetition; // Mark as unused to prevent compiler warnings.
                    GSList *inputs = parse_input_list(msg.data.task_request.input_data);
                    
                    if (!inputs) break;

                    for (GSList *l = inputs; l != NULL; l = l->next) {

                        task_input_t *t_in = g_new0(task_input_t, 1);
                        t_in->session_id = msg.task_id;
                        t_in->tw = tw;
                        t_in->input = (input_t *)l->data;

                        /* ---- Prepare the thread ---- */
                        pthread_attr_t attr;
                        pthread_attr_init(&attr);

                        cpu_set_t set;
                        CPU_ZERO(&set);
                        CPU_SET(cpu_affinity, &set);
                        gint affinity_err = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);
                        if (affinity_err != 0) {
                            g_warning("[WARNING] %s: Failed to set CPU affinity for Task ID %u. Error: %d (%s)", tw->task_name->str, t_in->session_id, affinity_err, g_strerror(affinity_err));
                        }
                        
                        struct sched_param param;
                        param.sched_priority = priority;
                        pthread_attr_setstacksize(&attr, PTHREAD_STACK_MIN);
                        pthread_attr_setschedpolicy(&attr, policy);
                        pthread_attr_setschedparam(&attr, &param);
                        pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);

                        g_print("[DEBUG] TW: priority: %d\n", param.sched_priority);

                        pthread_t tid;
                        gint rc = pthread_create(&tid, &attr, task_wrapper_run_thread, t_in);
                        pthread_attr_destroy(&attr); // Clean up attributes
                        if (rc) {
                            g_free(t_in->input);
                            g_free(t_in);
                            g_printerr("[ERROR] %s: pthread_create failed with code %d (%s) for Task ID %u\n", tw->task_name->str, rc, g_strerror(rc), t_in->session_id);
                            continue;
                        }
                        pthread_detach(tid);

                        task_wrapper_add_thread(tw, t_in->session_id, tid);

                        /* ---------------------------- */

                    }
                    
                    g_slist_free(inputs);
                    break;
                }

                case MSG_TASK_ABORT: {
                    g_print("[INFO] %s: FORCED ABORT for Session ID: %u\n", tw->task_name->str, msg.task_id);
                    
                    GSList *threads = task_wrapper_get_threads(tw, msg.task_id);
                    if (!threads) {
                        g_print("[WARNING] %s: No active threads found for session %u\n", tw->task_name->str, msg.task_id);
                        break;
                    }

                    for (GSList *l = threads; l != NULL; l = l->next) {
                        pthread_t *tid_ptr = (pthread_t *)l->data;
                        g_print("[INFO] %s: Cancelling thread %lu\n", tw->task_name->str, (unsigned long)*tid_ptr);
                        
                        // Request cancellation. The thread is responsible for its own cleanup,
                        // including removal from the tracking table, via its cleanup handlers.
                        pthread_cancel(*tid_ptr);
                    }
                    
                    g_slist_free(threads);
                    break;
                }

                default:
                    break;
            }
        }

        /* The loop terminates when mq_receive returns -1.
         * If errno is EAGAIN, it means the queue is now empty, which is the expected behavior.
         * Any other errno would indicate a real error. */
        if (errno != EAGAIN) {
            g_printerr("[ERROR] %s: mq_receive failed with an unexpected error: %s", tw->task_name->str, g_strerror(errno));
        }
    }
    return TRUE;
}

gboolean handle_end_thread(gpointer data) {
    thread_cleanup_data_t *cleanup = (thread_cleanup_data_t *)data;
    task_wrapper_remove_thread(cleanup->tw, cleanup->session_id, cleanup->thread_id);
    g_free(cleanup);
    return G_SOURCE_REMOVE;
}