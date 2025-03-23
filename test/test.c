#include <mysql/mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void finish_with_error(MYSQL *con) {
    fprintf(stderr, "%s\n", mysql_error(con));
    mysql_close(con);
    exit(1);
}

int main() {
    MYSQL *con = mysql_init(NULL);
    if (con == NULL) {
        fprintf(stderr, "mysql_init() failed\n");
        exit(1);
    }
    
    // Connect to the MySQL database
    if (mysql_real_connect(con, "localhost", "root", "Rishofencing123", "kct", 0, NULL, 0) == NULL) {
        finish_with_error(con);
    }
  
    MYSQL_STMT *stmt = mysql_stmt_init(con);
    if (!stmt) {
        finish_with_error(con);
    }
    
    const char *query = "SELECT * FROM student";
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        finish_with_error(con);
    }
    
    if (mysql_stmt_execute(stmt)) {
        finish_with_error(con);
    }
    
    MYSQL_RES *res = mysql_stmt_result_metadata(stmt);
    if (!res) {
        finish_with_error(con);
    }
    
    int num_fields = mysql_num_fields(res);
    MYSQL_FIELD *fields = mysql_fetch_fields(res);
    
    // Print column names
    for (int i = 0; i < num_fields; i++) {
        printf("%s ", fields[i].name);
    }
    printf("\n");
    
    MYSQL_BIND *bind = (MYSQL_BIND *)malloc(sizeof(MYSQL_BIND) * num_fields);
    memset(bind, 0, sizeof(MYSQL_BIND) * num_fields);
    
    char **row_data = (char **)malloc(sizeof(char *) * num_fields);
    unsigned long *lengths = (unsigned long *)malloc(sizeof(unsigned long) * num_fields);
    bool *is_null = (bool *)malloc(sizeof(bool) * num_fields);
    
    for (int i = 0; i < num_fields; i++) {
        row_data[i] = (char *)malloc(1024);
        bind[i].buffer_type = MYSQL_TYPE_STRING;
        bind[i].buffer = row_data[i];
        bind[i].buffer_length = 1024;
        bind[i].length = &lengths[i];
        bind[i].is_null = &is_null[i];
    }
    
    if (mysql_stmt_bind_result(stmt, bind)) {
        finish_with_error(con);
    }
    
    while (!mysql_stmt_fetch(stmt)) {
        for (int i = 0; i < num_fields; i++) {
            if (is_null[i]) {
                printf("NULL ");
            } else {
                row_data[i][lengths[i]] = '\0';
                printf("%s ", row_data[i]);
            }
        }
        printf("\n");
    }
    
    for (int i = 0; i < num_fields; i++) {
        free(row_data[i]);
    }
    free(row_data);
    free(lengths);
    free(is_null);
    free(bind);
    
    mysql_free_result(res);
    mysql_stmt_close(stmt);
    mysql_close(con);
    
    return 0;
}
