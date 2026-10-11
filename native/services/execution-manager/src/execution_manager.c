#include "execution_manager.h"


execution_manager_t* em_new(const gchar *name){
    g_return_val_if_fail(name != NULL, NULL);

    execution_manager_t *em = g_new0(execution_manager_t, 1);
    em->em_name = g_string_new(name);

    return em;
}


void em_free(execution_manager_t *em){
    if (!em) return;

    g_string_free(em->em_name, TRUE);
    g_free(em);
}



void em_run_schedule(execution_manager_t *em, schedule_t *sched) {
    g_return_if_fail(em != NULL);
    g_return_if_fail(sched != NULL);


    GMainLoop *loop = g_main_loop_new(NULL, FALSE);
    gint64 time_zero_us = g_get_monotonic_time();

    

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


void* task_wrapper_func(void* data){
    
    task_wrapper_input_t* tw_input = (task_wrapper_input_t*)data;
    
    /* Read the thread context arguments */
    guint16 task_id = tw_input->task_id;
    gpointer input = tw_input->data;
    GThreadFunc thread_func = tw_input->thread_func;
    schedule_t* sched = tw_input->sched;

    g_print("[INFO] ThreadCall %u: start thread function.\n", task_id);
    /* Run the thread function */
    gpointer res = thread_func(input);

    g_print("[INFO] ThreadCall %u: termination thread function. \n", task_id);


    /* Write the result */
    schedule_set_result(sched, task_id, "{}");

    /* Cleanup */
    g_free(res);
    g_free(tw_input);
    return NULL;

}



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
        tw_input->data = task->input_data;
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