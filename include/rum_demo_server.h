#include "cs165_api.h"

#define DEMO_SERVER_PORT 8088
#define COMMAND_NUM 12

struct cmdsocket {
	// The file descriptor for this client's socket
	int fd;

	// Whether this socket has been shut down
	int shutdown;

	// The client's socket address
	struct sockaddr_in6 addr;

	// The server's event loop
	struct event_base *evloop;

	// The client's buffered I/O event
	struct bufferevent *buf_event;

	// The client's output buffer (commands should write to this buffer,
	// which is flushed at the end of each command processing loop)
	struct evbuffer *buffer;

	// Doubly-linked list (so removal is fast) for cleaning up at shutdown
	struct cmdsocket *prev, *next;
};

struct command {
	char *name;
	char *desc;
	void (*func)(struct cmdsocket *cmdsocket, struct command *command, const char *params);
};

static void workload_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void dataSet_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void part_algo_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void phys_part_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void exec_work_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void userConstrain_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void userRUM_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void userSatisfy_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void conn_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void quit_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void kill_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void open_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void echo_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void help_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void info_func(struct cmdsocket *cmdsocket, struct command *command, const char *params);
static void shutdown_cmdsocket(struct cmdsocket *cmdsocket);

static char *strndup_p(const char *str, size_t len);
static void send_prompt(struct cmdsocket *cmdsocket);
static void setup_connection(int sockfd, struct sockaddr_in6 *remote_addr, struct event_base *evloop);
static void cmd_connect(int listenfd, short evtype, void *arg);
static void add_cmdsocket(struct cmdsocket *cmdsocket);
static struct cmdsocket *create_cmdsocket(int sockfd, struct sockaddr_in6 *remote_addr, struct event_base *evloop);
static void free_cmdsocket(struct cmdsocket *cmdsocket);
static void shutdown_cmdsocket(struct cmdsocket *cmdsocket);
static void flush_cmdsocket(struct cmdsocket *cmdsocket);
static void process_command(size_t len, char *cmdline, struct cmdsocket *cmdsocket);
static void cmd_read(struct bufferevent *buf_event, void *arg);
static void cmd_error(struct bufferevent *buf_event, short error, void *arg);
static int set_nonblock(int fd);
static void setup_connection(int sockfd, struct sockaddr_in6 *remote_addr, struct event_base *evloop);
static void cmd_connect(int listenfd, short evtype, void *arg);
static void sighandler(int signal);

char* execute_db_operator(db_operator* dbO);
void exec_dsl(struct cmdsocket *cmdsocket, char *dsl);
void setup_database(unsigned int dataset_num);