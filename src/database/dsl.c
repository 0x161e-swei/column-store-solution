#include "dsl.h"

// Create Commands
// Matches: create(db, <db_name>);
const char* create_db_command = "^create\\(db\\,\\\"[a-zA-Z0-9_]+\\\"\\)";

// Matches: create(tbl, <table_name>, <db_name>, <col_count>);OperatorType
const char* create_table_command = "^create\\(tbl\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,[0-9]+\\)";

// Matches: create(col, <col_name>, <tbl_var>, sorted);
const char* create_col_command_sorted = "^create\\(col\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,sorted)";

// Matches: create(col, <col_name>, <tbl_var>, unsorted);
const char* create_col_command_unsorted = "^create\\(col\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,unsorted)";

// Matches: quit
const char* quit_command = "^quit";

// Matches: shutdown
const char* shutdown_command = "^shutdown";

// Matches: load(<filename>)
const char* load_command = "^load\\(\\\"[a-zA-Z0-9_/\\.]+\\\"\\)";

// Matches: show_db
const char* show_command = "^show_db";

// Matches: <vec_pos>=select(<col_var>,<low>,<high>)
const char* select_from_col_command = "^[a-zA-Z0-9_]+\\=s\\([a-zA-Z0-9_\\.]+\\,-?[0-9]+\\,-?[0-9]+\\)";

// Matches: <vec_pos>=select(<col_var>,<posn_vec>,<low>,<high>)
const char* select_from_pre_command = "^[a-zA-Z0-9_]+\\=s\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_]+\\,-?[0-9]+\\,-?[0-9]+\\)";

// Matches: <vec_val>=fetch(<col_var>,<vec_pos>)
const char* fetch_command = "^[a-zA-Z0-9_]+\\=fetch\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_]+\\)";

// Matches: tuple(<col_var>)
const char* tuple_command = "^tuple\\([a-zA-Z0-9_\\.]+\\)";

// Matches: d(<col_var>,<vec_pos>)
const char* delete_pos = "^d\\([a-zA-Z0-9_\\.]+\\,\\\"[a-zA-Z0-9_]+\\\"\\)";

// Matches: d(<col_var>,<val>)
const char* delete_command = "^d\\([a-zA-Z0-9_\\.]+\\,-?[0-9]+\\)";

// Matches: i(<col_var>, number, ...)
const char* insert_command = "^i\\([a-zA-Z0-9_\\.]+(\\,-?[0-9]+)+\\)";

// Matches: u(<col_var>, <old>, <new>)
const char* update_command = "^u\\([a-zA-Z0-9_\\.]+\\,-?[0-9]+\\,-?[0-9]+\\)";

// temporary dsls for test
// Matches: partition("colName")
const char* partition_test = "^partition\\([a-zA-Z0-9_\\.]+\\)";

// Matches: show_tbl("colName")
const char* show_table_test = "^show_tbl\\([a-zA-Z0-9_\\.]+\\)";

// TODO(USER): You will need to update the commands here for every single command you add.

dsl** dsl_commands_init(void)
{
	dsl** commands = calloc(NUM_DSL_COMMANDS, sizeof(dsl*));

	for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
		commands[i] = malloc(sizeof(dsl));
	}

	// Assign the create commands
	commands[0]->c = create_db_command;
	commands[0]->g = CREATE_DB_CMD;

	commands[1]->c = create_table_command;
	commands[1]->g = CREATE_TABLE_CMD;

	commands[2]->c = create_col_command_sorted;
	commands[2]->g = CREATE_COLUMN_CMD;

	commands[3]->c = create_col_command_unsorted;
	commands[3]->g = CREATE_COLUMN_CMD;

	commands[4]->c = quit_command;
	commands[4]->g = QUIT_CMD;	

	commands[5]->c = load_command;
	commands[5]->g = LOAD_FILE_CMD;

	commands[6]->c = show_command;
	commands[6]->g = SHOW_DB_CMD;

	commands[7]->c = select_from_col_command;
	commands[7]->g = SELECT_COL_CMD;

	commands[8]->c = select_from_pre_command;
	commands[8]->g = SELECT_PRE_CMD;

	commands[9]->c = fetch_command;
	commands[9]->g = FETCH_CMD;

	commands[10]->c = tuple_command;
	commands[10]->g = TUPLE_CMD;

	commands[11]->c = shutdown_command;
	commands[11]->g = SHUTDOWN_CMD;

	commands[12]->c = delete_pos;
	commands[12]->g = DELETE_POS_CMD;

	commands[13]->c = insert_command;
	commands[13]->g = INSERT_CMD;

	commands[14]->c = update_command;
	commands[14]->g = UPDATE_CMD;
	
	commands[15]->c = delete_command;
	commands[15]->g = DELETE_CMD;

	commands[16]->c = partition_test;
	commands[16]->g = PARTITION_TEST;

	commands[17]->c = show_table_test;
	commands[17]->g = SHOWTBL_TEST;
	return commands;
}
