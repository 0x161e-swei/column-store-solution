/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The server should allow for multiple concurrent connections from clients.
 * Each client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "cs165_api.h"
#include "message.h"
#include "parser.h"
#include "db.h"
#include "utils.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

// Here, we allow for a global of DSL COMMANDS to be shared in the program
dsl** dsl_commands;

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/

db_operator* parse_command(message* recv_message, message* send_message) { 
	send_message->status = OK_WAIT_FOR_RESPONSE;
	db_operator *dbo = malloc(sizeof(db_operator));
	// Here you parse the message and fill in the proper db_operator fields for
	// now we just log the payload
	cs165_log(stdout, recv_message->payload);

	// Here, we give you a default parser, you are welcome to replace it with anything you want
	status parse_status = parse_command_string(recv_message->payload,
		dsl_commands, dbo);

	switch (parse_status.code) {
		case UNKNOWN_CMD: {
			free(dbo);
			dbo = NULL;
			log_info("unknown command!\n");
			send_message->status = UNKNOWN_COMMAND;
			break;
		}
		case ERROR: {
			free(dbo);
			dbo = NULL;
			log_info("server error! See logs\n");
			send_message->status = INTERNAL_ERROR;
			break;
		}
		case QUIT: case SHUTDOWN:{
			free(dbo);
			dbo = NULL;
			log_info("quit executed!\n");
			send_message->status = (parse_status.code == QUIT)? CLIENT_QUIT:
									SERVER_SHUTDOWN;
			break;
		}
		case CMD_DONE: {
			free(dbo);
			dbo = NULL;
			log_info("command done!\n");
			send_message->status = OK_DONE;
			break;
		}
		case OK:
			log_info("query to be executed!\n");
			send_message->status = OK_WAIT_FOR_RESPONSE;
		default: {
			break;
		}
	}
	return dbo;
}

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
char* execute_db_operator(db_operator* dbO) {
	switch (dbO->type) {
		case SHOWDB : {
			char *ret = show_db();
			if (NULL == ret) {
				free(dbO);
				ret = malloc(sizeof(char) * (strlen("no info found!") + 1));
				sprintf(ret, "%s", "no info found!");
				return ret;
			}
			else {
				free(dbO);
				return ret;
			}
			break;
		}
		case SELECT_COL: case SELECT_PRE: case FETCH: {
			Result *res = NULL;
			status s = query_execute(dbO, &res);
			if (OK != s.code) {

			}
			else {
				// Serialize the result if it is a FETCH
			}
			break;
		}
		case TUPLE: {
			char* ret = tuple(dbO);
			if (NULL != ret) {
				free((dbO->domain).res);
				free(dbO);
				return ret;
			}
			else {
				free((dbO->domain).res);
				free(dbO);
				ret = malloc(sizeof(char) * (strlen("no tuple found!") + 1));
				sprintf(ret, "%s", "no tuple found!");
				return ret;
			}
			break;
		}
		default : break;
	}
	free(dbO);
	char *ret;
	ret = malloc(sizeof(char) *(strlen("Command Done") + 1));
	sprintf(ret, "Command Done");
	return ret;
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
bool handle_client(int client_socket) {
	int done = 0;
	int length = 0;
	bool ret = false;

	log_info("Connected to socket: %d.\n", client_socket);

	// Create two messages, one from which to read and one from which to receive
	message send_message;
	message recv_message;

	// Continually receive messages from client and execute queries.
	// 1. Parse the command
	// 2. Handle request if appropriate
	// 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
	// 4. Send response of request.
	do {
		length = recv(client_socket, &recv_message, sizeof(message), 0);
		if (length < 0) {
			log_err("Client connection closed!\n");
			exit(1);
		} else if (length == 0) {
			log_err("length = 0 close!\n");
			done = 1;
		}

		if (!done) {
			char recv_buffer[recv_message.length];
			length = recv(client_socket, recv_buffer, recv_message.length,0);
			recv_message.payload = recv_buffer;
			recv_message.payload[recv_message.length] = '\0';

			// 1. Parse command
			db_operator* dbO = parse_command(&recv_message, &send_message);

			// 2. Handle request
			char* res = NULL;
			if (NULL != dbO && OK_WAIT_FOR_RESPONSE == send_message.status){
				res = execute_db_operator(dbO);
				send_message.length = strlen(res);
				log_info("cmd result:\n%s\n", res);
			}
			else {
				if (SERVER_SHUTDOWN == send_message.status) {
					done = 1;
					ret = true;
				}
				else if (CLIENT_QUIT == send_message.status) {
					done = 1;
				}
				send_message.length = 0;
			}

			// 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
			if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
				log_err("Failed to send message.");
				exit(1);
			}

			// 4. Send response of request
			if (OK_WAIT_FOR_RESPONSE  == send_message.status) {
				if (send(client_socket, res, send_message.length, 0) == -1) {
					log_err("Failed to send message.");
					exit(1);
				}
				// TODO: HOW TO FREE THIS STR
				// free(res);
			}
		}
	} while (!done);

	log_info("Connection closed at socket %d!\n", client_socket);
	// close(client_socket);
	return ret;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
	int server_socket;
	size_t len;
	struct sockaddr_un local;

	log_info("Attempting to setup server...\n");

	if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
		log_err("L%d: Failed to create socket.\n", __LINE__);
		return -1;
	}

	local.sun_family = AF_UNIX;
	strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
	unlink(local.sun_path);

	/*
	int on = 1;
	if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
	{
		log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
		return -1;
	}
	*/

	len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
	if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
		log_err("L%d: Socket failed to bind.\n", __LINE__);
		return -1;
	}

	if (listen(server_socket, 5) == -1) {
		log_err("L%d: Failed to listen on socket.\n", __LINE__);
		return -1;
	}

	return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
	int server_socket = setup_server();
	if (server_socket < 0) {
		exit(1);
	}

	// Populate the global dsl commands
	dsl_commands = dsl_commands_init();

	Db *default_db;
	OpenFlags flags = LOAD;
	status s = open_db("data/dbinfo", &default_db, flags);

	if (ERROR == s.code) {
		log_info("No database found on server or database info corrupted\n");
	}
	else {
		log_info("Database found on server\n");
	}


	log_info("Waiting for a connection %d ...\n", server_socket);

	struct sockaddr_un remote;
	socklen_t t = sizeof(remote);
	int client_socket = 0;

	bool shutdown_server = false;
	while (!shutdown_server) {
		if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
			log_err("L%d: Failed to accept a new connection.\n", __LINE__);
			exit(1);
		}

		shutdown_server = handle_client(client_socket);
		if (false == shutdown_server) {
			close(client_socket);
		}

		if (NULL != default_db)
			sync_db(default_db);
	}

	return 0;
}
