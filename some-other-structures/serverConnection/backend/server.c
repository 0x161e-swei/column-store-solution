#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <event.h>

#include "server.h"

#define COMMAND_NUM 7
char *current_file = NULL;


static struct command commands[] = {
	{ "connect", "Just connect", conn_func									},
	{ "echo", "Prints the command line.", echo_func							},
	{ "open_file", "Opens a file.", open_func								},
	{ "help", "Prints a list of commands and their descriptions.", help_func},
	{ "info", "Prints connection information.", info_func					},
	{ "quit", "Disconnects from the server.", quit_func						},
	{ "kill", "Shuts down the server.", kill_func							},
};

// List of open connections to be cleaned up at server shutdown
static struct cmdsocket cmd_listhead = { .next = NULL };
static struct cmdsocket * const socketlist = &cmd_listhead;

// str must have at least len bytes to copy
static char *strndup_p(const char *str, size_t len)
{
	char *newstr;

	newstr = malloc(len + 1);
	if(newstr == NULL) {
		ERRNO_OUT("Error allocating buffer for string duplication");
		return NULL;
	}

	memcpy(newstr, str, len);
	newstr[len] = 0;

	return newstr;
}

static void send_prompt(struct cmdsocket *cmdsocket)
{
	if(evbuffer_add_printf(cmdsocket->buffer, "> ") < 0) {
		ERROR_OUT("Error sending prompt to client.\n");
	}
}

void open_file(const char* file) {	
	if(current_file == NULL) {
		current_file = malloc(sizeof(char)*1024);		
	}
	if(strcmp(file, current_file) != 0) {
	    
	    strcpy(current_file, file);
		fprintf(stderr, ">>> Opening file %s.\n", file);

	}
	else {
		// Already open.
	}
}

static void conn_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	INFO_OUT("%s %s\n", command->name, params);
	evbuffer_add_printf(cmdsocket->buffer, "Hello, connect setup\n");
}

static void echo_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	INFO_OUT("%s %s\n", command->name, params);
	evbuffer_add_printf(cmdsocket->buffer, "%s\n", params);
}

static void open_func(struct cmdsocket *cmdsocket, struct command *command, const char *params) {
	INFO_OUT("%s %s\n", command->name, params);
	evbuffer_add_printf(cmdsocket->buffer, "1Data.json\n");

	// char *filetest = malloc(sizeof(char)*10240);
	// strcpy(filetest, "data/");
	// strcat(filetest, params);
	// strcat(filetest, ".bin");

 //    open_file(params);
    // evbuffer_add_printf(cmdsocket->buffer, "{\"event\":\"open_file\", \"status\": \"");
	
    // evbuffer_add_printf(cmdsocket->buffer,  "\"}\n");	
}

static void help_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	int i;

	INFO_OUT("%s %s\n", command->name, params);

	for(i = 0; i < COMMAND_NUM; i++) {
		evbuffer_add_printf(cmdsocket->buffer, "%s:\t%s\n", commands[i].name, commands[i].desc);
	}
}

static void info_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	char addr[INET6_ADDRSTRLEN];
	const char *addr_start;

	INFO_OUT("%s %s\n", command->name, params);

	addr_start = inet_ntop(cmdsocket->addr.sin6_family, &cmdsocket->addr.sin6_addr, addr, sizeof(addr));
	if(!strncmp(addr, "::ffff:", 7) && strchr(addr, '.') != NULL) {
		addr_start += 7;
	}

	evbuffer_add_printf(
			cmdsocket->buffer,
			"Client address: %s\nClient port: %hu\n",
			addr_start,
			cmdsocket->addr.sin6_port
			);
}

static void quit_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	INFO_OUT("%s %s\n", command->name, params);
	shutdown_cmdsocket(cmdsocket);
}

static void kill_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	INFO_OUT("%s %s\n", command->name, params);

	INFO_OUT("Shutting down server.\n");
	if(event_base_loopexit(cmdsocket->evloop, NULL)) {
		ERROR_OUT("Error shutting down server\n");
	}
	
	shutdown_cmdsocket(cmdsocket);
}

static void setup_connection(int sockfd, struct sockaddr_in6 *remote_addr, struct event_base *evloop)
{
	struct cmdsocket *cmdsocket;

	if(set_nonblock(sockfd)) {
		ERROR_OUT("Error setting non-blocking I/O on an incoming connection.\n");
	}

	// Copy connection info into a command handler info structure
	cmdsocket = create_cmdsocket(sockfd, remote_addr, evloop);
	if(cmdsocket == NULL) {
		close(sockfd);
		return;
	}

	// Initialize a buffered I/O event
	cmdsocket->buf_event = bufferevent_new(sockfd, cmd_read, NULL, cmd_error, cmdsocket);
	if(CHECK_NULL(cmdsocket->buf_event)) {
		ERROR_OUT("Error initializing buffered I/O event for fd %d.\n", sockfd);
		free_cmdsocket(cmdsocket);
		return;
	}
	bufferevent_base_set(evloop, cmdsocket->buf_event);
	bufferevent_settimeout(cmdsocket->buf_event, 0, 0);
	if(bufferevent_enable(cmdsocket->buf_event, EV_READ)) {
		ERROR_OUT("Error enabling buffered I/O event for fd %d.\n", sockfd);
		free_cmdsocket(cmdsocket);
		return;
	}

	// Create the outgoing data buffer
	cmdsocket->buffer = evbuffer_new();
	if(CHECK_NULL(cmdsocket->buffer)) {
		ERROR_OUT("Error creating output buffer for fd %d.\n", sockfd);
		free_cmdsocket(cmdsocket);
		return;
	}

	send_prompt(cmdsocket);
	flush_cmdsocket(cmdsocket);
}

static void add_cmdsocket(struct cmdsocket *cmdsocket)
{
	cmdsocket->prev = socketlist;
	cmdsocket->next = socketlist->next;
	if(socketlist->next != NULL) {
		socketlist->next->prev = cmdsocket;
	}
	socketlist->next = cmdsocket;
}

static struct cmdsocket *create_cmdsocket(int sockfd, struct sockaddr_in6 *remote_addr, struct event_base *evloop)
{
	struct cmdsocket *cmdsocket;

	cmdsocket = calloc(1, sizeof(struct cmdsocket));
	if(cmdsocket == NULL) {
		ERRNO_OUT("Error allocating command handler info");
		close(sockfd);
		return NULL;
	}
	cmdsocket->fd = sockfd;
	cmdsocket->addr = *remote_addr;
	cmdsocket->evloop = evloop;

	add_cmdsocket(cmdsocket);

	return cmdsocket;
}

static void free_cmdsocket(struct cmdsocket *cmdsocket)
{
	if(CHECK_NULL(cmdsocket)) {
		abort();
	}

	// Remove socket info from list of sockets
	if(cmdsocket->prev->next == cmdsocket) {
		cmdsocket->prev->next = cmdsocket->next;
	} else {
		ERROR_OUT("BUG: Socket list is inconsistent: cmdsocket->prev->next != cmdsocket!\n");
	}
	if(cmdsocket->next != NULL) {
		if(cmdsocket->next->prev == cmdsocket) {
			cmdsocket->next->prev = cmdsocket->prev;
		} else {
			ERROR_OUT("BUG: Socket list is inconsistent: cmdsocket->next->prev != cmdsocket!\n");
		}
	}

	// Close socket and free resources
	if(cmdsocket->buf_event != NULL) {
		bufferevent_free(cmdsocket->buf_event);
	}
	if(cmdsocket->buffer != NULL) {
		evbuffer_free(cmdsocket->buffer);
	}
	if(cmdsocket->fd >= 0) {
		shutdown_cmdsocket(cmdsocket);
		if(close(cmdsocket->fd)) {
			ERRNO_OUT("Error closing connection on fd %d", cmdsocket->fd);
		}
	}
	free(cmdsocket);
}

static void shutdown_cmdsocket(struct cmdsocket *cmdsocket)
{
	if(!cmdsocket->shutdown && shutdown(cmdsocket->fd, SHUT_RDWR)) {
		ERRNO_OUT("Error shutting down client connection on fd %d", cmdsocket->fd);
	}
	cmdsocket->shutdown = 1;
}

static void flush_cmdsocket(struct cmdsocket *cmdsocket)
{
	if(bufferevent_write_buffer(cmdsocket->buf_event, cmdsocket->buffer)) {
		ERROR_OUT("Error sending data to client on fd %d\n", cmdsocket->fd);
	}
}

static void process_command(size_t len, char *cmdline, struct cmdsocket *cmdsocket)
{
	size_t cmdlen;
	char *cmd;
	int i;

	// Skip leading whitespace, then find command name
	cmdline += strspn(cmdline, " \t");
	cmdlen = strcspn(cmdline, " \t");
	if(cmdlen == 0) {
		// The line was empty -- no command was given
		send_prompt(cmdsocket);
		return;
	} else if(len == cmdlen) {
		// There are no parameters
		cmd = cmdline;
		cmdline = "";
	} else {
		// There may be parameters
		cmd = strndup_p(cmdline, cmdlen);
		cmdline += cmdlen + 1; // Skip first space after command name
	}

	INFO_OUT("Command received: %s\n", cmd);

	// Execute the command, if it is valid
	for(i = 0; i < COMMAND_NUM; i++) {
		if(!strcmp(cmd, commands[i].name)) {
			INFO_OUT("Running command %s\n", commands[i].name);
			commands[i].func(cmdsocket, &commands[i], cmdline);
			break;
		}
	}
	if(i == COMMAND_NUM) {
		ERROR_OUT("Unknown command: %s\n", cmd);
		evbuffer_add_printf(cmdsocket->buffer, "Unknown command: %s\n", cmd);
	}
		
	// send_prompt(cmdsocket);

	if(cmd != cmdline && len != cmdlen) {
		free(cmd);
	}
}

static void cmd_read(struct bufferevent *buf_event, void *arg)
{
	struct cmdsocket *cmdsocket = (struct cmdsocket *)arg;
	char *cmdline;
	size_t len;
	int i;

	// Process up to 10 commands at a time
	for(i = 0; i < 10 && !cmdsocket->shutdown; i++) {
		cmdline = evbuffer_readline(buf_event->input);
		if(cmdline == NULL) {
			// No data, or data has arrived, but no end-of-line was found
			break;
		}
		len = strlen(cmdline);
	
		INFO_OUT("Read a line of length %zd from client on fd %d: %s\n", len, cmdsocket->fd, cmdline);
		process_command(len, cmdline, cmdsocket);
		free(cmdline);
	}

	// Send the results to the client
	flush_cmdsocket(cmdsocket);
}

static void cmd_error(struct bufferevent *buf_event, short error, void *arg)
{
	struct cmdsocket *cmdsocket = (struct cmdsocket *)arg;

	if(error & EVBUFFER_EOF) {
		INFO_OUT("Remote host disconnected from fd %d.\n", cmdsocket->fd);
		cmdsocket->shutdown = 1;
	} else if(error & EVBUFFER_TIMEOUT) {
		INFO_OUT("Remote host on fd %d timed out.\n", cmdsocket->fd);
	} else {
		ERROR_OUT("A socket error (0x%hx) occurred on fd %d.\n", error, cmdsocket->fd);
	}

	free_cmdsocket(cmdsocket);
}

static int set_nonblock(int fd)
{
	int flags;

	flags = fcntl(fd, F_GETFL);
	if(flags == -1) {
		ERRNO_OUT("Error getting flags on fd %d", fd);
		return -1;
	}
	flags |= O_NONBLOCK;
	if(fcntl(fd, F_SETFL, flags)) {
		ERRNO_OUT("Error setting non-blocking I/O on fd %d", fd);
		return -1;
	}

	return 0;
}

static void cmd_connect(int listenfd, short evtype, void *arg)
{
	struct sockaddr_in6 remote_addr;
	socklen_t addrlen = sizeof(remote_addr);
	int sockfd;
	int i;

	if(!(evtype & EV_READ)) {
		ERROR_OUT("Unknown event type in connect callback: 0x%hx\n", evtype);
		return;
	}
	
	// Accept and configure incoming connections (up to 10 connections in one go)
	for(i = 0; i < 10; i++) {
		sockfd = accept(listenfd, (struct sockaddr *)&remote_addr, &addrlen);
		if(sockfd < 0) {
			if(errno != EWOULDBLOCK && errno != EAGAIN) {
				ERRNO_OUT("Error accepting an incoming connection");
			}
			break;
		}

		INFO_OUT("Client connected on fd %d\n", sockfd);

		setup_connection(sockfd, &remote_addr, (struct event_base *)arg);
	}
}

// Used only by signal handler
static struct event_base *server_loop;

static void sighandler(int signal)
{
	INFO_OUT("Received signal %d. Shutting down.\n", signal);

	if(event_base_loopexit(server_loop, NULL)) {
		ERROR_OUT("Error shutting down server\n");
	}
}

int main(int argc, char const *argv[])
{
	struct event_base *evloop;
	struct event connect_event;

	unsigned short listenport = PORT;
	struct sockaddr_in6 local_addr;
	int listenfd;

	// Set signal handlers
	sigset_t sigset;
	sigemptyset(&sigset);
	struct sigaction siginfo = {
		.sa_handler = sighandler,
		.sa_mask = sigset,
		.sa_flags = SA_RESTART,
	};
	sigaction(SIGINT, &siginfo, NULL);
	sigaction(SIGTERM, &siginfo, NULL);
	
	// Initialize libevent
	INFO_OUT("libevent version: %s\n", event_get_version());
	evloop = event_base_new();
	if(CHECK_NULL(evloop)) {
		ERROR_OUT("Error initializing event loop.\n");
		return -1;
	}
	server_loop = evloop;
	INFO_OUT("libevent is using %s for events.\n", event_base_get_method(evloop));

	// Initialize socket address
	memset(&local_addr, 0, sizeof(local_addr));
	local_addr.sin6_family = AF_INET6;
	local_addr.sin6_port = htons(listenport);
	local_addr.sin6_addr = in6addr_any;

	// Begin listening for connections
	listenfd = socket(AF_INET6, SOCK_STREAM, 0);
	if(listenfd == -1) {
		ERRNO_OUT("Error creating listening socket");
		return -1;
	}
	int tmp_reuse = 1;
	if(setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &tmp_reuse, sizeof(tmp_reuse))) {
		ERRNO_OUT("Error enabling socket address reuse on listening socket");
		return -1;
	}
	if(bind(listenfd, (struct sockaddr *)&local_addr, sizeof(local_addr))) {
		ERRNO_OUT("Error binding listening socket");
		return -1;
	}
	if(listen(listenfd, 8)) {
		ERRNO_OUT("Error listening to listening socket");
		return -1;
	}

	// Set socket for non-blocking I/O
	if(set_nonblock(listenfd)) {
		ERROR_OUT("Error setting listening socket to non-blocking I/O.\n");
		return -1;
	}

	// Add an event to wait for connections
	event_set(&connect_event, listenfd, EV_READ | EV_PERSIST, cmd_connect, evloop);
	event_base_set(evloop, &connect_event);
	if(event_add(&connect_event, NULL)) {
		ERROR_OUT("Error scheduling connection event on the event loop.\n");
	}


	// Start the event loop
	if(event_base_dispatch(evloop)) {
		ERROR_OUT("Error running event loop.\n");
	}

	INFO_OUT("Server is shutting down.\n");

	// Clean up and close open connections
	while(socketlist->next != NULL) {
		free_cmdsocket(socketlist->next);
	}

	// Clean up libevent
	if(event_del(&connect_event)) {
		ERROR_OUT("Error removing connection event from the event loop.\n");
	}
	event_base_free(evloop);
	if(close(listenfd)) {
		ERRNO_OUT("Error closing listening socket");
	}

	return 0;
}
