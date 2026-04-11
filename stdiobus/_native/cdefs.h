/* C definitions for libstdio_bus embedding API */
/* This file must match include/stdio_bus_embed.h */

/* Return codes (from stdio_bus.h) */
#define STDIO_BUS_OK 0
#define STDIO_BUS_ERR -1
#define STDIO_BUS_EAGAIN -2
#define STDIO_BUS_EOF -3
#define STDIO_BUS_EFULL -4
#define STDIO_BUS_ENOTFOUND -5
#define STDIO_BUS_EINVAL -6

/* Error codes (from stdio_bus_embed.h) */
#define STDIO_BUS_ERR_CONFIG     -10
#define STDIO_BUS_ERR_WORKER     -11
#define STDIO_BUS_ERR_ROUTING    -12
#define STDIO_BUS_ERR_BUFFER     -13
#define STDIO_BUS_ERR_INVALID    -14
#define STDIO_BUS_ERR_STATE      -15

/* Bus states */
typedef enum {
    STDIO_BUS_STATE_CREATED = 0,
    STDIO_BUS_STATE_STARTING = 1,
    STDIO_BUS_STATE_RUNNING = 2,
    STDIO_BUS_STATE_STOPPING = 3,
    STDIO_BUS_STATE_STOPPED = 4
} stdio_bus_state_t;

/* Listen modes */
typedef enum {
    STDIO_BUS_LISTEN_NONE = 0,
    STDIO_BUS_LISTEN_TCP = 1,
    STDIO_BUS_LISTEN_UNIX = 2
} stdio_bus_listen_mode_t;

/* Forward declaration - opaque handle */
typedef struct stdio_bus stdio_bus_t;

/* Statistics */
typedef struct {
    uint64_t messages_in;
    uint64_t messages_out;
    uint64_t bytes_in;
    uint64_t bytes_out;
    uint64_t worker_restarts;
    uint64_t routing_errors;
    uint64_t client_connects;
    uint64_t client_disconnects;
} stdio_bus_stats_t;

/* Callback types - must match stdio_bus_embed.h exactly */
typedef void (*stdio_bus_message_cb)(stdio_bus_t *bus, const char *msg, 
                                     size_t len, void *user_data);
typedef void (*stdio_bus_error_cb)(stdio_bus_t *bus, int code,
                                   const char *message, void *user_data);
typedef void (*stdio_bus_log_cb)(stdio_bus_t *bus, int level,
                                 const char *message, void *user_data);
typedef void (*stdio_bus_worker_cb)(stdio_bus_t *bus, int worker_id,
                                    const char *event, void *user_data);
typedef void (*stdio_bus_client_connect_cb)(stdio_bus_t *bus, int client_id,
                                            const char *peer_info, void *user_data);
typedef void (*stdio_bus_client_disconnect_cb)(stdio_bus_t *bus, int client_id,
                                               const char *reason, void *user_data);

/* Listener configuration */
typedef struct {
    stdio_bus_listen_mode_t mode;
    const char *tcp_host;
    uint16_t tcp_port;
    const char *unix_path;
} stdio_bus_listener_config_t;

/* Options */
typedef struct {
    /* Configuration source */
    const char *config_path;
    const char *config_json;
    
    /* Listener configuration */
    stdio_bus_listener_config_t listener;
    
    /* Callbacks */
    stdio_bus_message_cb on_message;
    stdio_bus_error_cb on_error;
    stdio_bus_log_cb on_log;
    stdio_bus_worker_cb on_worker;
    stdio_bus_client_connect_cb on_client_connect;
    stdio_bus_client_disconnect_cb on_client_disconnect;
    
    /* User context */
    void *user_data;
    
    /* Options */
    int log_level;
} stdio_bus_options_t;

/* API functions */
stdio_bus_t *stdio_bus_create(const stdio_bus_options_t *options);
void stdio_bus_destroy(stdio_bus_t *bus);
int stdio_bus_start(stdio_bus_t *bus);
int stdio_bus_stop(stdio_bus_t *bus, int timeout_sec);
int stdio_bus_step(stdio_bus_t *bus, int timeout_ms);
int stdio_bus_ingest(stdio_bus_t *bus, const char *msg, size_t len);
stdio_bus_state_t stdio_bus_get_state(const stdio_bus_t *bus);
void stdio_bus_get_stats(const stdio_bus_t *bus, stdio_bus_stats_t *stats);
int stdio_bus_worker_count(const stdio_bus_t *bus);
int stdio_bus_session_count(const stdio_bus_t *bus);
int stdio_bus_pending_count(const stdio_bus_t *bus);
int stdio_bus_client_count(const stdio_bus_t *bus);
int stdio_bus_get_poll_fd(const stdio_bus_t *bus);
