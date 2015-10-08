#include "db.h"
#include <stdio.h>

// TODO(USER): Here we provide an incomplete implementation of the create_db.
// There will be changes that you will need to include here.
status create_db(const char* db_name, db** db) {
    if (*db == NULL) {
        *db = malloc(sizeof(db));
    }

    (*db)->name = db_name;
    (*db)->table_count = 0;
    (*db)->tables = NULL;


    FILE *dbinfo;
    if (NULL != (dbinfo = fopen("dbinfo", "r+"))) {
    	
    }
    else {
    	if (NULL != (dbinfo = fopen("dbinfo", "w+"))) {

    	}
    }



    status s;
    s.code = OK;
    return s;
}

