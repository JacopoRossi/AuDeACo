#include <iostream>
#include <memory>
#include <string>
#include <pthread.h>
#include <sched.h>
#include <limits.h>
#include <sys/mman.h>
#include <errno.h>
#include <grpcpp/grpcpp.h>
#include "proto/task_service.grpc.pb.h"

// Include C headers
extern "C" {
    #include "../include/app_task.h"
    #include "../include/logger.h"
}

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using taskservice::TaskExecutor;
using taskservice::TaskRequest;
using taskservice::TaskResponse;

// ============================================
// THREAD WRAPPER
// (Generic wrapper in grpc_server.cpp, like task_wrapper.c in MQ version)
// ============================================
extern "C" {
    void* task_main_wrapper(void* arg) {
        /* Set CPU affinity from inside the thread (self-affinity) */
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(7, &cpuset);  // Pin to CPU 7
        int affinity_ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (affinity_ret != 0) {
            printf("[THREAD WRAPPER] Warning: Failed to set CPU affinity: %s\n", strerror(affinity_ret));
        }

        /* Execute the actual task (task-specific code) */
        task_main(arg);

        return NULL;
    }
}

// Implementation of TaskExecutor service
class TaskExecutorServiceImpl final : public TaskExecutor::Service {
    Status ExecuteTask(ServerContext* context, 
                      const TaskRequest* request,
                      TaskResponse* response) override {
        
        printf("[gRPC Server] Received task: %s (ID: %u)\n", 
               request->task_name().c_str(), request->task_id());
        
        // Allocate task context
        task_context_t* ctx = (task_context_t*)malloc(sizeof(task_context_t));
        if (!ctx) {
            response->set_status("ERROR");
            response->set_error_message("Memory allocation failed");
            return Status::OK;
        }
        
        pthread_mutex_init(&ctx->lock, NULL);
        ctx->status = IDLE;
        
        // Convert JSON input to input_t structure
        std::string inputs_json = request->inputs_json();
        if (convert_input((char*)inputs_json.c_str(), &ctx->input) != 0) {
            response->set_status("ERROR");
            response->set_error_message("Failed to parse inputs");
            pthread_mutex_destroy(&ctx->lock);
            free(ctx);
            return Status::OK;
        }
        
        printf("[gRPC Server] Input parsed successfully\n");
        
        // Prepare thread attributes for real-time execution
        pthread_attr_t attr;
        struct sched_param param;
        pthread_t task_thread;
        
        pthread_attr_init(&attr);
        
        // Set stack size (avoid page faults)
        pthread_attr_setstacksize(&attr, PTHREAD_STACK_MIN + 0x4000);
        
        // Set CPU affinity (CPU 2)
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(7, &cpuset);
        int ret = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        if (ret != 0) {
            printf("[gRPC Server] Warning: CPU affinity failed\n");
        }
        
        // Set scheduling policy based on request
        int policy = SCHED_OTHER; // Default
        std::string policy_str = request->policy();
        if (policy_str == "fifo") {
            policy = SCHED_FIFO;
        } else if (policy_str == "rr") {
            policy = SCHED_RR;
        }
        
        if (pthread_attr_setschedpolicy(&attr, policy) != 0) {
            printf("[gRPC Server] Warning: Failed to set RT policy (need root privileges)\n");
        }
        
        // Set priority
        param.sched_priority = request->priority();
        pthread_attr_setschedparam(&attr, &param);
        
        // Use explicit scheduler inheritance
        pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
        
        // Create real-time thread (using generic wrapper that pins CPU affinity)
        if (pthread_create(&task_thread, &attr, task_main_wrapper, ctx) != 0) {
            response->set_status("ERROR");
            response->set_error_message("Failed to create task thread");
            pthread_attr_destroy(&attr);
            pthread_mutex_destroy(&ctx->lock);
            free(ctx);
            return Status::OK;
        }
        
        pthread_attr_destroy(&attr);
        
        // Wait for task completion
        pthread_join(task_thread, NULL);
        
        printf("[gRPC Server] Task completed\n");
        
        // Convert output to JSON
        char result_json[MAX_TASK_JSON_OUT];
        if (convert_output(&ctx->output, result_json) < 0) {
            response->set_status("ERROR");
            response->set_error_message("Failed to serialize output");
            pthread_mutex_destroy(&ctx->lock);
            free(ctx);
            return Status::OK;
        }
        
        // Set response
        response->set_task_id(request->task_id());
        response->set_status("COMPLETED");
        response->set_result_json(result_json);
        
        printf("[gRPC Server] Result: %s\n", result_json);
        
        // Cleanup
        pthread_mutex_destroy(&ctx->lock);
        free(ctx);
        
        return Status::OK;
    }
    
    // Asynchronous task execution with streaming responses
    Status ExecuteTaskAsync(ServerContext* context,
                           const TaskRequest* request,
                           ServerWriter<TaskResponse>* writer) override {

        printf("[gRPC Server Async] Request received for task: %s (ID: %u)\n",
               request->task_name().c_str(), request->task_id());
        
        // Allocate task context
        task_context_t* ctx = (task_context_t*)malloc(sizeof(task_context_t));
        if (!ctx) {
            TaskResponse error_response;
            error_response.set_task_id(request->task_id());
            error_response.set_status("ERROR");
            error_response.set_error_message("Memory allocation failed");
            writer->Write(error_response);
            return Status::OK;
        }
        
        pthread_mutex_init(&ctx->lock, NULL);
        ctx->status = IDLE;
        
        // Convert JSON input to input_t structure
        std::string inputs_json = request->inputs_json();
        if (convert_input((char*)inputs_json.c_str(), &ctx->input) != 0) {
            TaskResponse error_response;
            error_response.set_task_id(request->task_id());
            error_response.set_status("ERROR");
            error_response.set_error_message("Failed to parse inputs");
            writer->Write(error_response);
            pthread_mutex_destroy(&ctx->lock);
            free(ctx);
            return Status::OK;
        }
        
        printf("[gRPC Server Async] Input parsed successfully\n");
        
        // Prepare thread attributes for real-time execution
        pthread_attr_t attr;
        struct sched_param param;
        pthread_t task_thread;
        bool use_rt = true;
        
        pthread_attr_init(&attr);
        
        // 1. Set stack size (matching message queue order)
        int stack_ret = pthread_attr_setstacksize(&attr, PTHREAD_STACK_MIN + 0x4000);
        printf("[gRPC Server Async] Set stack size: %s\n", stack_ret == 0 ? "OK" : "FAILED");
        
        // Note: CPU affinity will be set AFTER pthread_create
        // Setting it in attributes with pthread_attr_setaffinity_np causes errno=22 with SCHED_FIFO
        
        // 3. Set scheduling policy
        memset(&param, 0, sizeof(param));
        
        int policy = SCHED_OTHER;
        std::string policy_str = request->policy();
        int priority_val = request->priority();
        
        printf("[gRPC Server Async] Requested policy='%s', priority=%d\n", policy_str.c_str(), priority_val);
        
        if (policy_str == "fifo") {
            policy = SCHED_FIFO;
        } else if (policy_str == "rr") {
            policy = SCHED_RR;
        }
        
        if (policy != SCHED_OTHER) {
            printf("[gRPC Server Async] Setting SCHED_FIFO with priority %d\n", priority_val);
            
            int policy_ret = pthread_attr_setschedpolicy(&attr, policy);
            printf("[gRPC Server Async] Set policy SCHED_FIFO: %s (ret=%d)\n",
                   policy_ret == 0 ? "OK" : "FAILED", policy_ret);
            
            if (policy_ret != 0) {
                printf("[gRPC Server Async] Warning: Failed to set RT policy, using SCHED_OTHER\n");
                policy = SCHED_OTHER;
                use_rt = false;
            } else {
                // 4. Set priority
                param.sched_priority = priority_val;
                int param_ret = pthread_attr_setschedparam(&attr, &param);
                printf("[gRPC Server Async] Set priority %d: %s (ret=%d)\n",
                       priority_val, param_ret == 0 ? "OK" : "FAILED", param_ret);
                
                if (param_ret != 0) {
                    use_rt = false;
                } else {
                    // 5. Use PTHREAD_EXPLICIT_SCHED (LAST - matching message queue)
                    int sched_ret = pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
                    printf("[gRPC Server Async] Set inherit sched: %s (ret=%d)\n", 
                           sched_ret == 0 ? "OK" : "FAILED", sched_ret);
                    
                    if (sched_ret == 0) {
                        printf("[gRPC Server Async] RT attributes configured successfully\n");
                    } else {
                        use_rt = false;
                    }
                }
            }
        } else {
            // SCHED_OTHER means no real-time
            use_rt = false;
        }
        
        // Create thread with RT attributes (or fallback to default if RT failed)
        int ret = pthread_create(&task_thread, &attr, task_main_wrapper, ctx);
        pthread_attr_destroy(&attr);
        
        if (ret != 0) {
            // RT failed, try again with no attributes
            printf("[gRPC Server Async] Thread creation with RT failed (errno=%d), trying without RT...\n", ret);
            use_rt = false;  // ← FIX: aggiorna flag!
            ret = pthread_create(&task_thread, NULL, task_main_wrapper, ctx);
        }
        
        if (ret != 0) {
            TaskResponse error_response;
            error_response.set_task_id(request->task_id());
            error_response.set_status("ERROR");
            error_response.set_error_message("Failed to create task thread");
            writer->Write(error_response);
            pthread_mutex_destroy(&ctx->lock);
            free(ctx);
            return Status::OK;
        }
        
        printf("[gRPC Server Async] Thread created successfully (RT=%s), waiting for task completion...\n", use_rt ? "yes" : "no");
        
        // Note: CPU affinity is set by the thread itself in task_main_wrapper

        // Wait for task completion (blocking join)
        pthread_join(task_thread, NULL);

        // Check if cancelled during execution
        bool task_cancelled = context->IsCancelled();
        
        // If task was cancelled, cleanup and return
        if (task_cancelled) {
            pthread_mutex_destroy(&ctx->lock);
            free(ctx);
            return Status::OK;
        }
        
        // Convert output to JSON
        char result_json[MAX_TASK_JSON_OUT];
        if (convert_output(&ctx->output, result_json) < 0) {
            TaskResponse error_response;
            error_response.set_task_id(request->task_id());
            error_response.set_status("ERROR");
            error_response.set_error_message("Failed to serialize output");
            writer->Write(error_response);
            pthread_mutex_destroy(&ctx->lock);
            free(ctx);
            return Status::OK;
        }
        
        // ✅ SEND RESULT - Task completed
        TaskResponse result_response;
        result_response.set_task_id(request->task_id());
        result_response.set_status("COMPLETED");
        result_response.set_result_json(result_json);
        writer->Write(result_response);
        
        printf("[gRPC Server Async] Result sent: %s\n", result_json);
        
        // Cleanup
        pthread_mutex_destroy(&ctx->lock);
        free(ctx);
        
        return Status::OK;
    }
};

void RunServer(const std::string& server_address) {
    TaskExecutorServiceImpl service;
    
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    
    // ============================================
    // PERFORMANCE OPTIMIZATIONS
    // ============================================
    
    // Increase thread pool for handling concurrent requests
    // Default is usually 2, we increase to handle multiple tasks simultaneously
    builder.SetSyncServerOption(ServerBuilder::SyncServerOption::NUM_CQS, 4);
    builder.SetSyncServerOption(ServerBuilder::SyncServerOption::MIN_POLLERS, 2);
    builder.SetSyncServerOption(ServerBuilder::SyncServerOption::MAX_POLLERS, 8);
    
    // Increase max concurrent streams per connection
    builder.AddChannelArgument(GRPC_ARG_MAX_CONCURRENT_STREAMS, 100);
    
    // Enable keepalive to maintain connections
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);  // 10 seconds
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);  // 5 seconds
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    
    // Increase message size limits (if needed for large payloads)
    builder.SetMaxReceiveMessageSize(4 * 1024 * 1024);  // 4MB
    builder.SetMaxSendMessageSize(4 * 1024 * 1024);     // 4MB
    
    // Optimize for low latency
    builder.AddChannelArgument(GRPC_ARG_HTTP2_BDP_PROBE, 0);  // Disable bandwidth probing
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000);
    
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "[gRPC Server] Listening on " << server_address << std::endl;
    std::cout << "[gRPC Server] Performance optimizations enabled:" << std::endl;
    std::cout << "  - Thread pool: 2-8 pollers, 4 completion queues" << std::endl;
    std::cout << "  - Max concurrent streams: 100" << std::endl;
    std::cout << "  - Keepalive enabled (10s interval)" << std::endl;
    std::cout << "  - Low latency optimizations active" << std::endl;
    
    server->Wait();
}

int main(int argc, char** argv) {
    // Memory locking for real-time determinism
    // NOTE: usiamo solo MCL_CURRENT (non MCL_FUTURE) per non forzare la residenza
    // di tutte le pagine future allocate da gRPC/protobuf. Riduce drasticamente
    // l'RSS osservato in `docker stats` mantenendo lockate le pagine già mappate
    // all'avvio (codice, .so, dati statici).
    if (mlockall(MCL_CURRENT) == -1) {
        std::cerr << "[gRPC Server] Warning: mlockall failed (need root privileges)" << std::endl;
    }

    // Set Real-Time Scheduling: SCHED_FIFO with priority 85
    struct sched_param rt_param;
    rt_param.sched_priority = 85;
    if (sched_setscheduler(0, SCHED_FIFO, &rt_param) != 0) {
        std::cerr << "[gRPC Server] Warning: Failed to set RT scheduling (need root or CAP_SYS_NICE)" << std::endl;
        std::cout << "[gRPC Server] ⚠️  Running with SCHED_OTHER" << std::endl;
    } else {
        std::cout << "[gRPC Server] ✅ Running with SCHED_FIFO priority 85" << std::endl;
    }

    // Get task name and port from environment
    const char* task_name = getenv("TASK_NAME");
    const char* grpc_port = getenv("GRPC_PORT");
    
    std::string server_address = "0.0.0.0:";
    server_address += grpc_port ? grpc_port : "50051";  // Default to 50051 if not set
    
    // Pin gRPC server and its internal threads to CPU 6.
    // Task execution threads (task_main_wrapper) are pinned to CPU 1 separately.
    cpu_set_t server_cpuset;
    CPU_ZERO(&server_cpuset);
    CPU_SET(6, &server_cpuset);
    if (sched_setaffinity(0, sizeof(cpu_set_t), &server_cpuset) != 0) {
        std::cerr << "[gRPC Server] Warning: Failed to set CPU affinity to CPU 0" << std::endl;
    } else {
        std::cout << "[gRPC Server] ✅ gRPC server pinned to CPU 0" << std::endl;
    }

    if (task_name) {
        std::cout << "[gRPC Server] Starting server for task: " << task_name << std::endl;
    }
    std::cout << "[gRPC Server] Listening on " << server_address << std::endl;
    
    RunServer(server_address);
    
    return 0;
}
