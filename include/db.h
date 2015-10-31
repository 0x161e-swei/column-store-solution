#ifndef DB_H__
#define DB_H__

#include "cs165_api.h"

status grab_db(const char* db_name, Db** db);

extern Db *database;

#endif // DB_H__
