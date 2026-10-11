#include <glib.h>
#include <stdlib.h>
#include <signal.h> // For signals
#include <unistd.h> // For sleep()
#include <sched.h>
#include <sys/mman.h>

#include "schedule.h"
#include "execution_manager.h"
#include "app_task.h"




/* Gloabal flag for check the main loop */
volatile gboolean keep_running = TRUE;


/* Signal Handler for SIGINT (Ctrl+C) */
void int_handler(int dummy) {
    (void)dummy; 
    g_print("\n[INFO] Execution Manager: SIGINT received.\n");
    keep_running = FALSE;
}

int main(int argc, char *argv[]) {
    (void)argc; (void)argv;


    /* Lock memory */
    if(mlockall(MCL_CURRENT|MCL_FUTURE) == -1) {
        g_error("[ERROR] Execution Manager: mlockall failed: %m\n");
        return 1;
    }

    /* Registre the signal handler for a clean clousure */
    signal(SIGINT, int_handler);

    /* ------ Init Execution Manager ------ */
    int exit_code = 0;
    char *env_task = getenv("TASK_NAME");
    char *env_queue = getenv("TASK_QUEUE_NAME");
    
    gchar *em_name = g_strdup_printf("%s", env_task ? env_task : DEFAULT_EXECUTION_MANAGER_NAME);

    execution_manager_t *em = em_new(em_name);
    if (!em) {
        g_error("[ERROR] Execution Manager: em_new failed.");
        return 1;
    }
    
    schedule_t *sched = NULL; // Init to NULL

    g_print("[INFO] Execution Manager: Initialized (Click Ctrl+C for a clean exit)\n");

    /* Control for new schedule */
    em_wait_for_schedule(em);
    sched = em_read_schedule(em);
    schedule_print(sched);


    /* -------------- Main Loop Execution -------------- */
    for(int i=0; i < sched->schedule_iterations && keep_running; i++){
        if (i > 0) {
            schedule_reset(sched);
        }

        /* Run the schedule */
        g_print("\n[INFO] Execution Manager: Start iteration %d/%d, progress percentage %d %% \n",i, sched->schedule_iterations, (i*100)/sched->schedule_iterations);
        em_run_schedule(em, sched);
        

        //if (keep_running) {
            //g_print("\n[INFO] Execution Manager: Schedule Completed. Reboot in 5 seconds... (or push Ctrl+C for exit)...\n\n");
            
            /* Slee for 5 second, but check flag each second */
            //for (int i = 0; i < 5 && keep_running; i++) {
                //sleep(1);
            //}
        //}
    }

    g_print("\n[INFO] Execution Manager: Exit from the main loop. Cleanup ...\n");

    if (em) em_free(em);
    if (sched) schedule_free(sched);
    
    g_print("[INFO] Execution Manager: Cleanup completed.\n");
    return exit_code;
}