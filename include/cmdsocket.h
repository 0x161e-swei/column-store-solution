#ifndef _CMDSOCKET_H_
#define _CMDSOCKET_H_

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <event.h>


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

void flush_cmdsocket(struct cmdsocket *cmdsocket);
#endif /* _CMDSOCKET_H_ */