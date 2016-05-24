#include "cmdsocket.h"
#include "utils.h"

void flush_cmdsocket(struct cmdsocket *cmdsocket)
{
	if(bufferevent_write_buffer(cmdsocket->buf_event, cmdsocket->buffer)) {
		log_err("Error sending data to client on fd %d\n", cmdsocket->fd);
	}
}
