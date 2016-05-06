#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <event.h>

#include "rum_demo_server.h"
#include "cs165_api.h"
#include "message.h"
#include "parser.h"
#include "db.h"
#include "table.h"
#include "utils.h"
#include "dsl.h"
#include "index.h"


dsl** dsl_commands;


char *current_file		= NULL;
int current_workload	= -1;
int current_dataset		= -1;

// TODO: add more dataset and workload mapping
const char* datasetMap[] = {	"data/dataset0/",
								"data/dataset1/",
								"data/dataset2/",
							};
const char* workloadMap[] = {	"data/workload0/workload",
								"data/workload1/workload",
								"data/workload2/workload",
							};

static struct command commands[] = {
	// setup connection between nodejs server and demo_server
	{ "connect", "Just connect", 							conn_func		},

	// dataset would be a prepared binary file
	// only called if user has decided the dataset (size and distribution)
	{ "dataSet", "Apply the dataSet", 						dataSet_func	},

	// workload would ba a prepare ascii file
	// onlyl called if user has decided the workload (size and distribution)
	{ "workload", "Select the workload", 					workload_func	},
	
	// apply the specified algorithm
	{ "part_algo","apply partition-decision algorithm", 	part_algo_func	},

	// apply the partition decision
	{ "phys_part","exec physical partition", 				phys_part_func	},

	// exec the specified workload on the specified dataset
	{ "exec_work","Execute workload", 						exec_work_func	},


	{"part_info", "Get partition info", 					part_info_func	},

	{ "userConstrain", "User Constrain", 					userConstrain_func},
	{ "userRUM", "User define RUM", userRUM_func                            },
	{ "userSatisfy", "User is ok with current RUM", userSatisfy_func        },
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
		case SHOWTBL: {
			show_tbl(dbO->tables[0]);
			free(dbO->tables);
			break;
		}
		case SELECT_COL: case SELECT_PRE: case FETCH: case DELETE: 
		case INSERT: case UPDATE: case DELETE_POS: {
			Result *res = NULL;
			// TODO: need to clean up dbO inside query_execute

			query_execute(dbO, &res);
			// if (OK != s.code) {

			// }
			// else {
			// 	// Serialize the result if it is a FETCH
			// }
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

void exec_dsl(struct cmdsocket *cmdsocket, const char *demo_dsl)
{
	debug("in side execs %s\n", demo_dsl);
	status parse_status;
	db_operator *dbo = malloc(sizeof(db_operator));
	#ifdef DEMO
	parse_status = parse_command_string(cmdsocket, demo_dsl, dsl_commands, dbo);
	#else
	parse_status = parse_command_string(demo_dsl, dsl_commands, dbo);
	#endif
	char *res = NULL;
	switch (parse_status.code) {
		case UNKNOWN_CMD: {
			free(dbo);
			dbo = NULL;
			log_info("unknown command!\n");
			res = malloc(sizeof(char) * 30);
			sprintf(res, "%s", "unknown command!");
			break;
		}
		case ERROR: {
			free(dbo);
			dbo = NULL;
			log_info("server error! See logs\n");
			res = malloc(sizeof(char) * 30);
			sprintf(res, "%s", "server error!");
			break;
		}
		case QUIT: case SHUTDOWN:{
			free(dbo);
			dbo = NULL;
			log_info("quit executed!\n");
			res = malloc(sizeof(char) * 30);
			sprintf(res, "%s", "quit executed!");
			break;
		}
		case CMD_DONE: {
			free(dbo);
			dbo = NULL;
			log_info("command done!\n");
			res = malloc(sizeof(char) * 30);
			sprintf(res, "%s", "command done!");
			break;
		}
		case OK: {
			log_info("query to be executed!\n");
			res = execute_db_operator(dbo);
		}
		case PARTALGO_DONE: {
			log_info("partition algorithm called!\n");
			evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"message\",");
			evbuffer_add_printf(cmdsocket->buffer, "\"msg\": \"partition_algo done, %d partitions in total!\"", part_inst->p_count);
			evbuffer_add_printf(cmdsocket->buffer, "}\n");
			break;
		}
		default: {
			break;
		}
	}

	if (parse_status.code != PARTALGO_DONE) {
		evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"dsl_result\",");
		evbuffer_add_printf(cmdsocket->buffer, "\"res\": \"%s\"", res);
		evbuffer_add_printf(cmdsocket->buffer, "}\n");
		free(res);
	}
	flush_cmdsocket(cmdsocket);
}

void setup_database(unsigned int dataset_num) {
	data_path = datasetMap[dataset_num];
	Db *default_db;
	OpenFlags flags = LOAD;
	// +7 for 'dbinfo\0'
	char *dbinfo = malloc(strlen(data_path) + 7);
	strncpy(dbinfo, data_path, strlen(data_path) + 1);
	strncat(dbinfo, "dbinfo", 6);
	log_info("loading db from %s\n", dbinfo);
	open_db(dbinfo, &default_db, flags);
	free(dbinfo);
}

// str must have at least len bytes to copy
static char *strndup_p(const char *str, size_t len)
{
	char *newstr;
	newstr = malloc(len + 1);
	if(newstr == NULL) {
		log_err("Error allocating buffer for string duplication");
		return NULL;
	}
	memcpy(newstr, str, len);
	newstr[len] = 0;
	return newstr;
}

static void send_prompt(struct cmdsocket *cmdsocket)
{
	if(evbuffer_add_printf(cmdsocket->buffer, "> ") < 0) {
		log_err("Error sending prompt to client.\n");
	}
}

static void open_file(const char* file) 
{	
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


/**
 * The workload is stored in another directory. 
 * The nodejs server gives a number to specify which workload to use
 * What database server actually does is using the number mapping to a file/directory and
 * then load and apply the workload to the already choosed dataset
 */
static void workload_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	if (current_workload != (*params - '0')) {
		current_workload = *params - '0';
	}
	evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"message\",");
	evbuffer_add_printf(cmdsocket->buffer, "\"msg\":\"Workload specified!\""); // evaluated RUM
	evbuffer_add_printf(cmdsocket->buffer, "}\n");
	flush_cmdsocket(cmdsocket);
}

/**
 * The nodejs server gives a number to specify the dataset to use
 * the server actually use the number to find a path where the dataseta is stored
 */
static void dataSet_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	// evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"dataSet\",");
	// evbuffer_add_printf(cmdsocket->buffer, "\"fileName\": \"../Data/DataSet1.json\"");
	// evbuffer_add_printf(cmdsocket->buffer, "}\n");
	// flush_cmdsocket(cmdsocket);

	if (-1 == current_dataset) {
		log_info("no previous database loaded!\n");
		current_dataset = *params - '0';
		setup_database(current_dataset);
	}
	else if ((*params - '0') != current_dataset) {
		log_info("previous database already loaded!\nDropping the previous DB...\n");
		// drop the current database and setup a new one
		sync_db(database);
		current_dataset = *params - '0';
		setup_database(current_dataset);
	}
	evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"message\",");
	evbuffer_add_printf(cmdsocket->buffer, "\"msg\":\"Dataset Loaded!\""); // evaluated RUM
	evbuffer_add_printf(cmdsocket->buffer, "}\n");
	flush_cmdsocket(cmdsocket);
}

static void part_algo_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	int partition_algo = *params - '0';
	char *demo_dsl = malloc(sizeof(char) * 150);
	memset(demo_dsl, 0, sizeof(char) * 150);
	if (partition_algo == 4) {
		// no partition
	}
	else if (partition_algo == 5) {
		// sort the data
	}
	else if (current_dataset >= 0 && current_workload >=0) {
		// TODO: the database should maintain the instruction of current decision for future execution
		sprintf(demo_dsl, "partition_decision(foo.tb1.a,\"%d\",%d)", current_workload, partition_algo);
		exec_dsl(cmdsocket, demo_dsl);
	}
	else {
		evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"message\",");
		evbuffer_add_printf(cmdsocket->buffer, "\"msg\":\"NO database loaded or no workload specified!\""); // error msg
		evbuffer_add_printf(cmdsocket->buffer, "}\n");
		flush_cmdsocket(cmdsocket);
	}
	free(demo_dsl);
}

/**
 * According to the partition instruction produced by the latest function call of part_algo_func,
 * To physically partition the data
 */
static void phys_part_func(struct cmdsocket *cmdsocket , struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	if (NULL != part_inst) {
		char *demo_dsl = malloc(sizeof(char) * 150);
	        memset(demo_dsl, 0, sizeof(char) * 150); 
		sprintf(demo_dsl, "partition(foo.tb1.a)");
		exec_dsl(cmdsocket, demo_dsl);
		if (part_inst->pivots) {
			free(part_inst->pivots);
		}
		#ifdef GHOST_VALUE
		if (part_inst->ghost_count) {
			free(part_inst->ghost_count);
		}
		#endif
		free(part_inst);
		free(demo_dsl);
	}
	else {
		evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"message\",");
		evbuffer_add_printf(cmdsocket->buffer, "\"msg\":\"NO partition instruction found!\""); // error msg
		evbuffer_add_printf(cmdsocket->buffer, "}\n");
		flush_cmdsocket(cmdsocket);

	}
}

static void part_info_func(struct cmdsocket *cmdsocket , struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	Table *tmp_tbl = NULL;
	grab_table("foo.tb1", &tmp_tbl);
	if (NULL != tmp_tbl && NULL != tmp_tbl->primary_indexed_col) {
		evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"partInfo\",");
		
		evbuffer_add_printf(cmdsocket->buffer, "\"sizes\": [");
		unsigned int i = 0;
		for (; i < tmp_tbl->primary_indexed_col->partitionCount - 1; i++) {
			evbuffer_add_printf(cmdsocket->buffer, "%d,", tmp_tbl->primary_indexed_col->part_size[i]);
		}
		evbuffer_add_printf(cmdsocket->buffer, "%d", tmp_tbl->primary_indexed_col->part_size[i]);

		evbuffer_add_printf(cmdsocket->buffer, "],\"pivots\": [");
		i = 0;
		for (; i < tmp_tbl->primary_indexed_col->partitionCount - 1; i++) {
			evbuffer_add_printf(cmdsocket->buffer, "%d,", tmp_tbl->primary_indexed_col->pivots[i]);
		}
		evbuffer_add_printf(cmdsocket->buffer, "%d", tmp_tbl->primary_indexed_col->pivots[i]);
		evbuffer_add_printf(cmdsocket->buffer, "]}\n");
		flush_cmdsocket(cmdsocket);
	}
}

static void exec_work_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	// we could probably do it in another way, which is the server(innterface) 
	// itself loads the workload text file and than fire the workload through exec_dsl
	char *demo_dsl = malloc(sizeof(char) * 150);
        memset(demo_dsl, 0, sizeof(char) * 150);
	if (current_dataset >= 0 && current_workload >=0) {
		sprintf(demo_dsl, "exec_work(%s)", workloadMap[current_workload]);
		exec_dsl(cmdsocket, demo_dsl);
	}
	else {
		evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"message\",");
		evbuffer_add_printf(cmdsocket->buffer, "\"msg\":\"NO database loaded or no workload specified!\""); // error msg
		evbuffer_add_printf(cmdsocket->buffer, "}\n");
		flush_cmdsocket(cmdsocket);
	}
	free(demo_dsl);
}

static void userConstrain_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	// Todo: the server should analyse [alg, ghost] and evaluate RUM
	evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"evaluateRUM\",");
	evbuffer_add_printf(cmdsocket->buffer, "\"value\":[10,20,30]"); // evaluated RUM
	evbuffer_add_printf(cmdsocket->buffer, "}\n");
	flush_cmdsocket(cmdsocket);
}

static void userRUM_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	// Todo: server reevaluate the RUM according user defined RUM
	evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"evaluateRUM\",");
	evbuffer_add_printf(cmdsocket->buffer, "\"value\":[23,80,90]"); // new RUM
	evbuffer_add_printf(cmdsocket->buffer, "}\n");
	flush_cmdsocket(cmdsocket);

}

static void userSatisfy_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	// Todo: send current partition info and start physical partition!

	FILE *fp;
	if ((fp = fopen("../../DataGeneration/workLoadData5.json", "rb")) == NULL)
	{
		exit(0);
	}
	fseek(fp, 0, SEEK_END);
	size_t fileLen = ftell(fp);
	char *tmp = (char *) malloc(sizeof(char) * fileLen);
	fseek(fp, 0, SEEK_SET);
	size_t total_read = fread(tmp, sizeof(char), fileLen, fp);
	while (total_read < fileLen) {
		size_t r = fread(tmp + sizeof(char) * total_read, sizeof(char), fileLen - total_read, fp);
		total_read += r;
	}
	fclose(fp);

	evbuffer_add_printf(cmdsocket->buffer, "{\"event\": \"ok\",");
	evbuffer_add_printf(cmdsocket->buffer, "\"size\":");
	evbuffer_add_printf(cmdsocket->buffer, "[8, 73, 71, 362, 1058, 2605, 9966, 8076, 22510, 6873, 11562, 13980, 11215, 2351, 19250, 8499, 10942, 3642, 15413, 9105, 9941, 7963, 7357, 6052, 3957, 1758, 1822, 208, 2276, 877, 226, 3],");
	evbuffer_add_printf(cmdsocket->buffer, "\"workLoadCost\":");
	evbuffer_add_printf(cmdsocket->buffer, "%s", tmp);
	evbuffer_add_printf(cmdsocket->buffer, "}\n");
	flush_cmdsocket(cmdsocket);
}



static void conn_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	evbuffer_add_printf(cmdsocket->buffer, "Hello, connect setup\n");
}

static void echo_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);
	evbuffer_add_printf(cmdsocket->buffer, "%s\n", params);
}

static void open_func(struct cmdsocket *cmdsocket, struct command *command, const char *params) {
	log_info("%s %s\n", command->name, params);
	// Todo: open the file
	evbuffer_add_printf(cmdsocket->buffer, "1Data.json\n");

	// char *filetest = malloc(sizeof(char)*10240);
	// strcpy(filetest, "data/");
	// strcat(filetest, params);
	// strcat(filetest, ".bin");

	// open_file(params);
	// evbuffer_add_printf(cmdsocket->buffer, "{\"event\":\"open_file\", \"status\": \"");
	
	// evbuffer_add_printf(cmdsocket->buffer,  "\"}\n");	
}

static void help_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	int i;

	log_info("%s %s\n", command->name, params);

	for(i = 0; i < COMMAND_NUM; i++) {
		evbuffer_add_printf(cmdsocket->buffer, "%s:\t%s\n", commands[i].name, commands[i].desc);
	}
}

static void info_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	char addr[INET6_ADDRSTRLEN];
	const char *addr_start;

	log_info("%s %s\n", command->name, params);

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
	log_info("%s %s\n", command->name, params);
	shutdown_cmdsocket(cmdsocket);
}

static void kill_func(struct cmdsocket *cmdsocket, struct command *command, const char *params)
{
	log_info("%s %s\n", command->name, params);

	log_info("Shutting down server.\n");
	if(event_base_loopexit(cmdsocket->evloop, NULL)) {
		log_err("Error shutting down server\n");
	}
	
	shutdown_cmdsocket(cmdsocket);
}

static void setup_connection(int sockfd, struct sockaddr_in6 *remote_addr, struct event_base *evloop)
{
	struct cmdsocket *cmdsocket;

	if(set_nonblock(sockfd)) {
		log_err("Error setting non-blocking I/O on an incoming connection.\n");
	}

	// Copy connection info into a command handler info structure
	cmdsocket = create_cmdsocket(sockfd, remote_addr, evloop);
	if(cmdsocket == NULL) {
		close(sockfd);
		return;
	}

	// Initialize a buffered I/O event
	cmdsocket->buf_event = bufferevent_new(sockfd, cmd_read, NULL, cmd_error, cmdsocket);
	if(NULL == cmdsocket->buf_event) {
		log_err("Error initializing buffered I/O event for fd %d.\n", sockfd);
		free_cmdsocket(cmdsocket);
		return;
	}
	bufferevent_base_set(evloop, cmdsocket->buf_event);
	bufferevent_settimeout(cmdsocket->buf_event, 0, 0);
	if(bufferevent_enable(cmdsocket->buf_event, EV_READ)) {
		log_err("Error enabling buffered I/O event for fd %d.\n", sockfd);
		free_cmdsocket(cmdsocket);
		return;
	}

	// Create the outgoing data buffer
	cmdsocket->buffer = evbuffer_new();
	if(NULL == cmdsocket->buffer) {
		log_err("Error creating output buffer for fd %d.\n", sockfd);
		free_cmdsocket(cmdsocket);
		return;
	}

//	send_prompt(cmdsocket);
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
		log_err("Error allocating command handler info");
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
	if(NULL == cmdsocket) {
		abort();
	}

	// Remove socket info from list of sockets
	if(cmdsocket->prev->next == cmdsocket) {
		cmdsocket->prev->next = cmdsocket->next;
	} else {
		log_err("BUG: Socket list is inconsistent: cmdsocket->prev->next != cmdsocket!\n");
	}
	if(cmdsocket->next != NULL) {
		if(cmdsocket->next->prev == cmdsocket) {
			cmdsocket->next->prev = cmdsocket->prev;
		} else {
			log_err("BUG: Socket list is inconsistent: cmdsocket->next->prev != cmdsocket!\n");
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
			log_err("Error closing connection on fd %d", cmdsocket->fd);
		}
	}
	free(cmdsocket);
}

static void shutdown_cmdsocket(struct cmdsocket *cmdsocket)
{
	if(!cmdsocket->shutdown && shutdown(cmdsocket->fd, SHUT_RDWR)) {
		log_err("Error shutting down client connection on fd %d", cmdsocket->fd);
	}
	cmdsocket->shutdown = 1;
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
		// send_prompt(cmdsocket);
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

	log_info("Command received: %s\n", cmd);

	// Execute the command, if it is valid
	for(i = 0; i < COMMAND_NUM; i++) {
		if(!strcmp(cmd, commands[i].name)) {
			log_info("Running command %s\n", commands[i].name);
			//cmdline    send_command("open_file " + ....);
			commands[i].func(cmdsocket, &commands[i], cmdline);
			break;
		}
	}
	if(i == COMMAND_NUM) {
		log_err("Unknown command: %s\n", cmd);
		evbuffer_add_printf(cmdsocket->buffer, "Unknown command: %s\n", cmd);
	}
		
	//send_prompt(cmdsocket);

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
	
		log_info("Read a line of length %zd from client on fd %d: %s\n", len, cmdsocket->fd, cmdline);
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
		log_info("Remote host disconnected from fd %d.\n", cmdsocket->fd);
		cmdsocket->shutdown = 1;
	} else if(error & EVBUFFER_TIMEOUT) {
		log_info("Remote host on fd %d timed out.\n", cmdsocket->fd);
	} else {
		log_err("A socket error (0x%hx) occurred on fd %d.\n", error, cmdsocket->fd);
	}

	free_cmdsocket(cmdsocket);
}

static int set_nonblock(int fd)
{
	int flags;

	flags = fcntl(fd, F_GETFL);
	if(flags == -1) {
		log_err("Error getting flags on fd %d", fd);
		return -1;
	}
	flags |= O_NONBLOCK;
	if(fcntl(fd, F_SETFL, flags)) {
		log_err("Error setting non-blocking I/O on fd %d", fd);
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
		log_err("Unknown event type in connect callback: 0x%hx\n", evtype);
		return;
	}
	
	// Accept and configure incoming connections (up to 10 connections in one go)
	for(i = 0; i < 10; i++) {
		sockfd = accept(listenfd, (struct sockaddr *)&remote_addr, &addrlen);
		if(sockfd < 0) {
			if(errno != EWOULDBLOCK && errno != EAGAIN) {
				log_err("Error accepting an incoming connection");
			}
			break;
		}

		log_info("Client connected on fd %d\n", sockfd);

		setup_connection(sockfd, &remote_addr, (struct event_base *)arg);
	}
}

// Used only by signal handler
static struct event_base *server_loop;

static void sighandler(int signal)
{
	log_info("Received signal %d. Shutting down.\n", signal);

	if(event_base_loopexit(server_loop, NULL)) {
		log_err("Error shutting down server\n");
	}
}

int main()
{
	struct event_base *evloop;
	struct event connect_event;

	unsigned short listenport = DEMO_SERVER_PORT;
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
	log_info("libevent version: %s\n", event_get_version());
	evloop = event_base_new();
	if(NULL == evloop) {
		log_err("Error initializing event loop.\n");
		return -1;
	}
	server_loop = evloop;
	log_info("libevent is using %s for events.\n", event_base_get_method(evloop));

	// Initialize socket address
	memset(&local_addr, 0, sizeof(local_addr));
	local_addr.sin6_family = AF_INET6;
	local_addr.sin6_port = htons(listenport);
	local_addr.sin6_addr = in6addr_any;

	// Populate the global dsl commands
	dsl_commands = dsl_commands_init();

	// Begin listening for connections
	listenfd = socket(AF_INET6, SOCK_STREAM, 0);
	if(listenfd == -1) {
		log_err("Error creating listening socket");
		return -1;
	}
	int tmp_reuse = 1;
	if(setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &tmp_reuse, sizeof(tmp_reuse))) {
		log_err("Error enabling socket address reuse on listening socket");
		return -1;
	}
	if(bind(listenfd, (struct sockaddr *)&local_addr, sizeof(local_addr))) {
		log_err("Error binding listening socket");
		return -1;
	}
	if(listen(listenfd, 8)) {
		log_err("Error listening to listening socket");
		return -1;
	}

	// Set socket for non-blocking I/O
	if(set_nonblock(listenfd)) {
		log_err("Error setting listening socket to non-blocking I/O.\n");
		return -1;
	}

	// Add an event to wait for connections
	event_set(&connect_event, listenfd, EV_READ | EV_PERSIST, cmd_connect, evloop);
	event_base_set(evloop, &connect_event);
	if(event_add(&connect_event, NULL)) {
		log_err("Error scheduling connection event on the event loop.\n");
	}


	// Start the event loop
	if(event_base_dispatch(evloop)) {
		log_err("Error running event loop.\n");
	}

	log_info("Server is shutting down.\n");

	// Clean up and close open connections
	while(socketlist->next != NULL) {
		free_cmdsocket(socketlist->next);
	}

	// Clean up libevent
	if(event_del(&connect_event)) {
		log_err("Error removing connection event from the event loop.\n");
	}
	event_base_free(evloop);
	if(close(listenfd)) {
		log_err("Error closing listening socket");
	}

	return 0;
}
