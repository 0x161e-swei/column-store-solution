#define PORT 8088

// Behaves similarly to printf(...), but adds file, line, and function
// information.  I omit do ... while(0) because I always use curly braces in my
// if statements.
#define INFO_OUT(...) {\
	printf("%s:%d: %s():\t", __FILE__, __LINE__, __FUNCTION__);\
	printf(__VA_ARGS__);\
}

// Behaves similarly to fprintf(stderr, ...), but adds file, line, and function
// information.
#define ERROR_OUT(...) {\
	fprintf(stderr, "\e[0;1m%s:%d: %s():\t", __FILE__, __LINE__, __FUNCTION__);\
	fprintf(stderr, __VA_ARGS__);\
	fprintf(stderr, "\e[0m");\
}

// Behaves similarly to perror(...), but supports printf formatting and prints
// file, line, and function information.
#define ERRNO_OUT(...) {\
	fprintf(stderr, "\e[0;1m%s:%d: %s():\t", __FILE__, __LINE__, __FUNCTION__);\
	fprintf(stderr, __VA_ARGS__);\
	fprintf(stderr, ": %d (%s)\e[0m\n", errno, strerror(errno));\
}

// Prints a message and returns 1 if o is NULL, returns 0 otherwise
#define CHECK_NULL(o) ( (o) == NULL ? ( fprintf(stderr, "\e[0;1m%s is null.\e[0m\n", #o), 1 ) : 0 )

// Size of array (Caution: references its parameter multiple times)
#define ARRAY_SIZE(array) (sizeof((array)) / sizeof((array)[0]))

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


