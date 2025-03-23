/*
** LuaSQL, MySQL driver
** Authors:  Eduardo Quintao
** See Copyright Notice in license.html
*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#ifdef WIN32
#include <winsock2.h>
#define NO_CLIENT_LONG_LONG
#endif

#include "mysql.h"
#include "errmsg.h"

#include "lua.h"
#include "lauxlib.h"

#include "luasql.h"

#define LUASQL_ENVIRONMENT_MYSQL "MySQL environment"
#define LUASQL_CONNECTION_MYSQL "MySQL connection"
#define LUASQL_CURSOR_MYSQL "MySQL cursor"
#define LUASQL_STATEMENT "MySQL statement"
#define LUASQL_STATEMENT_CURSOR "MySQL statement cursor"

/* For compat with old version 4.0 */
#if (MYSQL_VERSION_ID < 40100) 
#define MYSQL_TYPE_VAR_STRING   FIELD_TYPE_VAR_STRING 
#define MYSQL_TYPE_STRING       FIELD_TYPE_STRING 
#define MYSQL_TYPE_DECIMAL      FIELD_TYPE_DECIMAL 
#define MYSQL_TYPE_SHORT        FIELD_TYPE_SHORT 
#define MYSQL_TYPE_LONG         FIELD_TYPE_LONG 
#define MYSQL_TYPE_FLOAT        FIELD_TYPE_FLOAT 
#define MYSQL_TYPE_DOUBLE       FIELD_TYPE_DOUBLE 
#define MYSQL_TYPE_LONGLONG     FIELD_TYPE_LONGLONG 
#define MYSQL_TYPE_INT24        FIELD_TYPE_INT24 
#define MYSQL_TYPE_YEAR         FIELD_TYPE_YEAR 
#define MYSQL_TYPE_TINY         FIELD_TYPE_TINY 
#define MYSQL_TYPE_TINY_BLOB    FIELD_TYPE_TINY_BLOB 
#define MYSQL_TYPE_MEDIUM_BLOB  FIELD_TYPE_MEDIUM_BLOB 
#define MYSQL_TYPE_LONG_BLOB    FIELD_TYPE_LONG_BLOB 
#define MYSQL_TYPE_BLOB         FIELD_TYPE_BLOB 
#define MYSQL_TYPE_DATE         FIELD_TYPE_DATE 
#define MYSQL_TYPE_NEWDATE      FIELD_TYPE_NEWDATE 
#define MYSQL_TYPE_DATETIME     FIELD_TYPE_DATETIME 
#define MYSQL_TYPE_TIME         FIELD_TYPE_TIME 
#define MYSQL_TYPE_TIMESTAMP    FIELD_TYPE_TIMESTAMP 
#define MYSQL_TYPE_ENUM         FIELD_TYPE_ENUM 
#define MYSQL_TYPE_SET          FIELD_TYPE_SET
#define MYSQL_TYPE_NULL         FIELD_TYPE_NULL

#define mysql_commit(_) ((void)_)
#define mysql_rollback(_) ((void)_)
#define mysql_autocommit(_,__) ((void)_)

#endif

typedef struct {
	short      closed;
} env_data;

typedef struct {
	short      closed;
	int        env;                /* reference to environment */
	MYSQL     *my_conn;
} conn_data;

typedef struct {
	short      closed;
	int        conn;               /* reference to connection */
	int        numcols;            /* number of columns */
	int        colnames, coltypes; /* reference to column information tables */
	MYSQL_RES *my_res;
	MYSQL 	  *my_conn;
} cur_data;

typedef struct {
	short      closed;
	MYSQL_STMT *stmt;
	int        num_fields;          
	MYSQL_BIND *bind ; 
	MYSQL_RES *my_res;
	MYSQL_FIELD *fields;
	char **row_data;
	unsigned long *lengths ;
	bool *is_null;
	int stmt_ref;  // Reference to the connection in Lua registry
} stmt_cur_data;


typedef struct {
    short closed;
    MYSQL_STMT *stmt;
    MYSQL_BIND *params;
    unsigned int num_params;
    int conn;  // Reference to the connection in Lua registry

    // Added persistent storage for parameter values
    struct {
        int integer; 
        double number;
        char boolean;
        char *str;
        unsigned long size;
    } *params_data;

} stmt_data;

/*
** Check for valid environment.
*/
static env_data *getenvironment (lua_State *L) {
	env_data *env = (env_data *)luaL_checkudata (L, 1, LUASQL_ENVIRONMENT_MYSQL);
	luaL_argcheck (L, env != NULL, 1, "environment expected");
	luaL_argcheck (L, !env->closed, 1, "environment is closed");
	return env;
}


/*
** Check for valid connection.
*/
static conn_data *getconnection (lua_State *L) {
	conn_data *conn = (conn_data *)luaL_checkudata (L, 1, LUASQL_CONNECTION_MYSQL);
	luaL_argcheck (L, conn != NULL, 1, "connection expected");
	luaL_argcheck (L, !conn->closed, 1, "connection is closed");
	return conn;
}


/*
** Check for valid cursor.
*/
static cur_data *getcursor (lua_State *L) {
	cur_data *cur = (cur_data *)luaL_checkudata (L, 1, LUASQL_CURSOR_MYSQL);
	luaL_argcheck (L, cur != NULL, 1, "cursor expected");
	luaL_argcheck (L, !cur->closed, 1, "cursor is closed");
	return cur;
}


/*
** Push the value of #i field of #tuple row.
*/
static void pushvalue (lua_State *L, void *row, long int len) {
	if (row == NULL)
		lua_pushnil (L);
	else
		lua_pushlstring (L, row, len);
}


/*
** Get the internal database type of the given column.
*/
static char *getcolumntype (enum enum_field_types type) {

	switch (type) {
		case MYSQL_TYPE_VAR_STRING: case MYSQL_TYPE_STRING:
			return "string";
		case MYSQL_TYPE_DECIMAL: case MYSQL_TYPE_SHORT: case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_FLOAT: case MYSQL_TYPE_DOUBLE: case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_INT24: case MYSQL_TYPE_YEAR: case MYSQL_TYPE_TINY: 
			return "number";
		case MYSQL_TYPE_TINY_BLOB: case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB: case MYSQL_TYPE_BLOB:
			return "binary";
		case MYSQL_TYPE_DATE: case MYSQL_TYPE_NEWDATE:
			return "date";
		case MYSQL_TYPE_DATETIME:
			return "datetime";
		case MYSQL_TYPE_TIME:
			return "time";
		case MYSQL_TYPE_TIMESTAMP:
			return "timestamp";
		case MYSQL_TYPE_ENUM: case MYSQL_TYPE_SET:
			return "set";
		case MYSQL_TYPE_NULL:
			return "null";
		default:
			return "undefined";
	}
}


/*
** Creates the lists of fields names and fields types.
*/
static void create_colinfo (lua_State *L, cur_data *cur) {
	MYSQL_FIELD *fields;
	char typename[50];
	int i;
	fields = mysql_fetch_fields(cur->my_res);
	lua_newtable (L); /* names */
	lua_newtable (L); /* types */
	for (i = 1; i <= cur->numcols; i++) {
		lua_pushstring (L, fields[i-1].name);
		lua_rawseti (L, -3, i);
		sprintf (typename, "%.20s(%ld)", getcolumntype (fields[i-1].type), fields[i-1].length);
		lua_pushstring(L, typename);
		lua_rawseti (L, -2, i);
	}
	/* Stores the references in the cursor structure */
	cur->coltypes = luaL_ref (L, LUA_REGISTRYINDEX);
	cur->colnames = luaL_ref (L, LUA_REGISTRYINDEX);
}


/*
** Closes the cursos and nullify all structure fields.
*/
static void cur_nullify (lua_State *L, cur_data *cur) {
	/* Nullify structure fields. */
	cur->closed = 1;
	mysql_free_result(cur->my_res);
	luaL_unref (L, LUA_REGISTRYINDEX, cur->conn);
	luaL_unref (L, LUA_REGISTRYINDEX, cur->colnames);
	luaL_unref (L, LUA_REGISTRYINDEX, cur->coltypes);
}

void stmt_cur_nullify(stmt_cur_data *cur) {
    if (!cur) return;
	if (cur->closed) return;
	cur->closed = 1;
    for (int i = 0; i < cur->num_fields; i++) {
        free(cur->row_data[i]);
    }
    if (cur->my_res) {
        mysql_free_result(cur->my_res);
    }
}

	
/*
** Get another row of the given cursor.
*/
static int cur_fetch (lua_State *L) {
	cur_data *cur = getcursor (L);
	MYSQL_RES *res = cur->my_res;
	unsigned long *lengths;
	MYSQL_ROW row = mysql_fetch_row(res);
	if (row == NULL) {
		cur_nullify (L, cur);
		lua_pushnil(L);  /* no more results */
		return 1;
	}
	lengths = mysql_fetch_lengths(res);

	if (lua_istable (L, 2)) {
		const char *opts = luaL_optstring (L, 3, "n");
		if (strchr (opts, 'n') != NULL) {
			/* Copy values to numerical indices */
			int i;
			for (i = 0; i < cur->numcols; i++) {
				pushvalue (L, row[i], lengths[i]);
				lua_rawseti (L, 2, i+1);
			}
		}
		if (strchr (opts, 'a') != NULL) {
			int i;
			/* Check if colnames exists */
			if (cur->colnames == LUA_NOREF)
		        create_colinfo(L, cur);
			lua_rawgeti (L, LUA_REGISTRYINDEX, cur->colnames);/* Push colnames*/
	
			/* Copy values to alphanumerical indices */
			for (i = 0; i < cur->numcols; i++) {
				lua_rawgeti(L, -1, i+1); /* push the field name */

				/* Actually push the value */
				pushvalue (L, row[i], lengths[i]);
				lua_rawset (L, 2);
			}
			/* lua_pop(L, 1);  Pops colnames table. Not needed */
		}
		lua_pushvalue(L, 2);
		return 1; /* return table */
	}
	else {
		int i;
		luaL_checkstack (L, cur->numcols, LUASQL_PREFIX"too many columns");
		for (i = 0; i < cur->numcols; i++)
			pushvalue (L, row[i], lengths[i]);
		return cur->numcols; /* return #numcols values */
	}
}

static int stmt_cur_fields (lua_State *L) {
	stmt_cur_data *cur = (stmt_cur_data *)luaL_checkudata (L, 1, LUASQL_STATEMENT_CURSOR);
	lua_newtable(L);  
	for (int i = 0; i < cur->num_fields; i++) {
		lua_pushinteger(L, i+1);
		lua_pushstring(L, cur->fields[i].name);
		lua_settable(L, -3);
	}
	return 1;
}

static int stmt_cur_fetch (lua_State *L) {
	stmt_cur_data *cur = (stmt_cur_data *)luaL_checkudata (L, 1, LUASQL_STATEMENT_CURSOR);
	if (mysql_stmt_fetch(cur->stmt)) {
		stmt_cur_nullify(cur);
		lua_pushnil(L);  /* no more results */
		return 1;
	}
	const char *opts = luaL_optstring (L, 2, "n");
	lua_newtable(L);  
	for (int i = 0; i < cur->num_fields; i++) {
		if (strchr (opts, 'n') != NULL){
			lua_pushinteger(L, i+1);
		} else{
			lua_pushstring(L, cur->fields[i].name);
		}
		if (cur->is_null[i]) {
			lua_pushnil(L);
		} else {
			cur->row_data[i][cur->lengths[i]] = '\0';
			lua_pushstring(L, cur->row_data[i]);
		}
		lua_settable(L, -3);
	}
	return 1;
}

/*
** Get the next result from multiple statements
*/
static int cur_next_result (lua_State *L) {
	cur_data *cur = getcursor (L);
	MYSQL* con = cur->my_conn;
	int status;
	if(mysql_more_results(con)){
		status = mysql_next_result(con);
		if(status == 0){
			mysql_free_result(cur->my_res);
			cur->my_res = mysql_store_result(con);
			if(cur->my_res != NULL){
				lua_pushboolean(L, 1);
				return 1;
			}else{
				lua_pushboolean(L, 0);
				lua_pushinteger(L, mysql_errno(con));
				lua_pushstring(L, mysql_error(con));
				return 3;
			}
		}else{
			lua_pushboolean(L, 0);
			lua_pushinteger(L, status);
			switch(status){
				case CR_COMMANDS_OUT_OF_SYNC:
					lua_pushliteral(L, "CR_COMMANDS_OUT_OF_SYNC");
				break;
				case CR_SERVER_GONE_ERROR:
					lua_pushliteral(L, "CR_SERVER_GONE_ERROR");
				break;
				case CR_SERVER_LOST:
					lua_pushliteral(L, "CR_SERVER_LOST");
				break;
				case CR_UNKNOWN_ERROR:
					lua_pushliteral(L, "CR_UNKNOWN_ERROR");
				break;
				default:
					lua_pushliteral(L, "Unknown");
			}
			return 3;
		}
	}else{
		lua_pushboolean(L, 0);
		lua_pushinteger(L, -1);
		return 2;
	}
}

/*
** Check if next result is available
*/
static int cur_has_next_result (lua_State *L) {
	cur_data *cur = getcursor (L);
	lua_pushboolean(L, mysql_more_results(cur->my_conn));
	return 1;
}


/*
** Cursor object collector function
*/
static int cur_gc (lua_State *L) {
	cur_data *cur = (cur_data *)luaL_checkudata (L, 1, LUASQL_CURSOR_MYSQL);
	if (cur != NULL && !(cur->closed))
		cur_nullify (L, cur);
	return 0;
}


/*
** Close the cursor on top of the stack.
** Return 1
*/
static int cur_close (lua_State *L) {
	cur_data *cur = (cur_data *)luaL_checkudata (L, 1, LUASQL_CURSOR_MYSQL);
	luaL_argcheck (L, cur != NULL, 1, LUASQL_PREFIX"cursor expected");
	if (cur->closed) {
		lua_pushboolean (L, 0);
		lua_pushstring(L, "cursor is already closed");
		return 2;
	}
	cur_nullify (L, cur);
	lua_pushboolean (L, 1);
	return 1;
}

static int stmt_cur_gc (lua_State *L) {
	stmt_cur_data *cur = (stmt_cur_data *)luaL_checkudata (L, 1, LUASQL_STATEMENT_CURSOR);
	if (cur != NULL && !(cur->closed))
		stmt_cur_nullify(cur);
	return 0;
}


/*
** Close the cursor on top of the stack.
** Return 1
*/
static int stmt_cur_close (lua_State *L) {
	stmt_cur_data *cur = (stmt_cur_data *)luaL_checkudata (L, 1, LUASQL_STATEMENT_CURSOR);
	luaL_argcheck (L, cur != NULL, 1, LUASQL_PREFIX"cursor expected");
	if (cur->closed) {
		lua_pushboolean (L, 0);
		lua_pushstring(L, "cursor is already closed");
		return 2;
	}
	stmt_cur_nullify (cur);
	lua_pushboolean (L, 1);
	return 1;
}


/*
** Pushes a column information table on top of the stack.
** If the table isn't built yet, call the creator function and stores
** a reference to it on the cursor structure.
*/
static void _pushtable (lua_State *L, cur_data *cur, size_t off) {
	int *ref = (int *)cur + off / sizeof(int);

	/* If colnames or coltypes do not exist, create both. */
	if (*ref == LUA_NOREF)
		create_colinfo(L, cur);
	
	/* Pushes the right table (colnames or coltypes) */
	lua_rawgeti (L, LUA_REGISTRYINDEX, *ref);
}
#define pushtable(L,c,m) (_pushtable(L,c,offsetof(cur_data,m)))


/*
** Return the list of field names.
*/
static int cur_getcolnames (lua_State *L) {
	pushtable (L, getcursor(L), colnames);
	return 1;
}


/*
** Return the list of field types.
*/
static int cur_getcoltypes (lua_State *L) {
	pushtable (L, getcursor(L), coltypes);
	return 1;
}


/*
** Push the number of rows.
*/
static int cur_numrows (lua_State *L) {
	lua_pushinteger (L, (lua_Number)mysql_num_rows (getcursor(L)->my_res));
	return 1;
}


/*
** Seeks to an arbitrary row in a query result set.
*/
static int cur_seek (lua_State *L) {
	cur_data *cur = getcursor (L);
	lua_Integer rownum = luaL_checkinteger (L, 2);
	mysql_data_seek (cur->my_res, rownum);
	return 0;
}


/*
** Create a new Cursor object and push it on top of the stack.
*/
static int create_cursor (lua_State *L, MYSQL *my_conn, int conn, MYSQL_RES *result, int cols) {
	cur_data *cur = (cur_data *)LUASQL_NEWUD(L, sizeof(cur_data));
	luasql_setmeta (L, LUASQL_CURSOR_MYSQL);

	/* fill in structure */
	cur->closed = 0;
	cur->conn = LUA_NOREF;
	cur->numcols = cols;
	cur->colnames = LUA_NOREF;
	cur->coltypes = LUA_NOREF;
	cur->my_res = result;
	cur->my_conn = my_conn;
	lua_pushvalue (L, conn);
	cur->conn = luaL_ref (L, LUA_REGISTRYINDEX);

	return 1;
}

static int create_stmt_cursor (lua_State *L, MYSQL_STMT *stmt, MYSQL_RES *result, int num_fields, MYSQL_FIELD *fields) {
	stmt_cur_data *cur = (stmt_cur_data *)LUASQL_NEWUD(L, sizeof(stmt_cur_data));
	luasql_setmeta (L, LUASQL_STATEMENT_CURSOR);

	 // Get result metadata
	 cur->my_res = result;
	 cur->stmt = stmt;
	 cur->fields = fields;
 
	 cur->num_fields = num_fields;
 
	 // Allocate memory for MYSQL_BIND array
	 cur->bind = (MYSQL_BIND *)malloc(sizeof(MYSQL_BIND) * num_fields);
	 cur->row_data = (char **)malloc(sizeof(char *) * num_fields);
	 cur->lengths = (unsigned long *)malloc(sizeof(unsigned long) * num_fields);
	 cur->is_null = (bool *)malloc(sizeof(bool) * num_fields);
 
	 memset(cur->bind, 0, sizeof(MYSQL_BIND) * num_fields);
 
	 // Allocate memory for each row data and initialize MYSQL_BIND
	 for (int i = 0; i < num_fields; i++) {
		 cur->row_data[i] = (char *)malloc(1024);
		 cur->bind[i].buffer_type = MYSQL_TYPE_STRING;
		 cur->bind[i].buffer = cur->row_data[i];
		 cur->bind[i].buffer_length = 1024;
		 cur->bind[i].length = &cur->lengths[i];
		 cur->bind[i].is_null = &cur->is_null[i];
	 }

	 if (mysql_stmt_bind_result(stmt, cur->bind)) {
		stmt_cur_nullify(cur);
        luaL_error(L, "Couldn't bind stmt with result");
		return 0;
    }
 
	 cur->closed = 0; // Mark struct as active
	lua_pushvalue (L, 1);
	cur->stmt_ref = luaL_ref (L, LUA_REGISTRYINDEX);

	return 1;
}


static int conn_gc (lua_State *L) {
	conn_data *conn=(conn_data *)luaL_checkudata(L, 1, LUASQL_CONNECTION_MYSQL);
	if (conn != NULL && !(conn->closed)) {
		/* Nullify structure fields. */
		conn->closed = 1;
		luaL_unref (L, LUA_REGISTRYINDEX, conn->env);
		mysql_close (conn->my_conn);
	}
	return 0;
}


/*
** Close a Connection object.
*/
static int conn_close (lua_State *L) {
	conn_data *conn=(conn_data *)luaL_checkudata(L, 1, LUASQL_CONNECTION_MYSQL);
	luaL_argcheck (L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
	if (conn->closed) {
		lua_pushboolean(L, 0);
		lua_pushstring(L, "Connection is already closed");
		return 2;
	}
	
	conn->closed = 1;

	lua_pushboolean (L, 1);
	return 1;
}

/*
** Ping connection.
*/
static int conn_ping (lua_State *L) {
	conn_data *conn=(conn_data *)luaL_checkudata(L, 1, LUASQL_CONNECTION_MYSQL);
	luaL_argcheck (L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
	if (conn->closed) {
		lua_pushboolean (L, 0);
		return 1;
	}
	if (mysql_ping (conn->my_conn) == 0) {
		lua_pushboolean (L, 1);
		return 1;
	} else if (mysql_errno (conn->my_conn) == CR_SERVER_GONE_ERROR) {
		lua_pushboolean (L, 0);
		return 1;
	}
	luaL_error(L, mysql_error(conn->my_conn));
	return 0;
}


static int escape_string (lua_State *L) {
  size_t size, new_size;
  conn_data *conn = getconnection (L);
  const char *from = luaL_checklstring(L, 2, &size);
  char *to;
  to = (char*)malloc(sizeof(char) * (2 * size + 1));
  if(to) {
    new_size = mysql_real_escape_string(conn->my_conn, to, from, size);
    lua_pushlstring(L, to, new_size);
    free(to);
    return 1;
  }
  luaL_error(L, "could not allocate escaped string");
  return 0;
}

/*
** Execute an SQL statement.
** Return a Cursor object if the statement is a query, otherwise
** return the number of tuples affected by the statement.
*/
static int conn_execute (lua_State *L) {
	conn_data *conn = getconnection (L);
	size_t st_len;
	const char *statement = luaL_checklstring (L, 2, &st_len);
	if (mysql_real_query(conn->my_conn, statement, st_len)) 
		/* error executing query */
		return luasql_failmsg(L, "error executing query. MySQL: ", mysql_error(conn->my_conn));
	else
	{
		MYSQL_RES *res = mysql_store_result(conn->my_conn);
		unsigned int num_cols = mysql_field_count(conn->my_conn);

		if (res) { /* tuples returned */
			return create_cursor (L, conn->my_conn, 1, res, num_cols);
		}
		else { /* mysql_use_result() returned nothing; should it have? */
			if(num_cols == 0) { /* no tuples returned */
            	/* query does not return data (it was not a SELECT) */
				lua_pushinteger(L, mysql_affected_rows(conn->my_conn));
				return 1;
        	}
			else /* mysql_use_result() should have returned data */
				return luasql_failmsg(L, "error retrieving result. MySQL: ", mysql_error(conn->my_conn));
		}
	}
}


static int conn_prepare(lua_State *L) {
    conn_data *conn = getconnection(L);
    const char *sql = luaL_checkstring(L, 2);
    
    stmt_data *stmt = (stmt_data *)LUASQL_NEWUD(L, sizeof(stmt_data));
    luasql_setmeta(L, LUASQL_STATEMENT);

    stmt->stmt = mysql_stmt_init(conn->my_conn);
    if (!stmt->stmt) {
        return luasql_failmsg(L, "error preparing statement. MySQL: ", mysql_error(conn->my_conn));
    }

    if (mysql_stmt_prepare(stmt->stmt, sql, strlen(sql)) != 0) {
        return luasql_failmsg(L, "error preparing statement. MySQL: ", mysql_stmt_error(stmt->stmt));
    }

    stmt->num_params = mysql_stmt_param_count(stmt->stmt);
    stmt->params = (MYSQL_BIND *)calloc(stmt->num_params, sizeof(MYSQL_BIND));
	stmt->params_data = (typeof(stmt->params_data))calloc(stmt->num_params, sizeof(*stmt->params_data));
    stmt->closed = 0;
	lua_pushvalue(L, 1);
    stmt->conn = luaL_ref(L, LUA_REGISTRYINDEX);
    return 1; // Return statement object
}

static int stmt_bind(lua_State *L) {
    stmt_data *stmt = (stmt_data *)luaL_checkudata(L, 1, LUASQL_STATEMENT);
    int index = luaL_checkinteger(L, 2) - 1;  // Convert Lua 1-based index to C 0-based index
    
    if (index < 0 || index >= stmt->num_params) {
        return luaL_error(L, "Invalid parameter index");
    }

    MYSQL_BIND *param = &stmt->params[index];
    int type = lua_type(L, 3);

    switch (type) {
        case LUA_TNUMBER:
            if (lua_isinteger(L, 3)) {
                stmt->params_data[index].integer = lua_tointeger(L, 3);
                param->buffer_type = MYSQL_TYPE_LONG;
                param->buffer = &stmt->params_data[index].integer;
                param->buffer_length = sizeof(int);
            } else {
                stmt->params_data[index].number = lua_tonumber(L, 3);
                param->buffer_type = MYSQL_TYPE_DOUBLE;
                param->buffer = &stmt->params_data[index].number;
                param->buffer_length = sizeof(double);
            }
            break;
        
        case LUA_TSTRING:
            stmt->params_data[index].str = strdup(lua_tostring(L, 3));
            param->buffer_type = MYSQL_TYPE_STRING;
            param->buffer = (void *)stmt->params_data[index].str;
            param->buffer_length = strlen(stmt->params_data[index].str);
            break;
        
        case LUA_TBOOLEAN:
            stmt->params_data[index].boolean = lua_toboolean(L, 3);
            param->buffer_type = MYSQL_TYPE_TINY;
            param->buffer = &stmt->params_data[index].boolean;
            param->buffer_length = sizeof(char);
            break;
        
        case LUA_TNIL:
            param->buffer_type = MYSQL_TYPE_NULL;
            break;
        
        default:
            return luasql_faildirect(L, "error executing query. Invalid parameter type");
    }

    if (mysql_stmt_bind_param(stmt->stmt, stmt->params)) {
        printf("[ERROR] mysql_stmt_bind_param() failed: %s\n", mysql_stmt_error(stmt->stmt));
        return luasql_failmsg(L, "error executing query (stmt_bind_param). MySQL: ", mysql_stmt_error(stmt->stmt));
    }

    lua_pushboolean(L, 1);
    return 1;
}



static int stmt_execute(lua_State *L) {
	stmt_data *stmt = (stmt_data *)luaL_checkudata(L, 1, LUASQL_STATEMENT);
	MYSQL_RES * res;
	MYSQL_FIELD *fields;
	unsigned int num_cols;
	if (mysql_stmt_execute(stmt->stmt)) {

		printf("[ERROR] mysql_stmt_execute() failed: %s\n", mysql_stmt_error(stmt->stmt));
		int n = luasql_failmsg(L, "error executing query (stmt_execute). MySQL: ", mysql_stmt_error(stmt->stmt));
		return n;
	}
	if (mysql_stmt_store_result(stmt->stmt)) {
		int n = luasql_failmsg(L, "error executing query (stmt_store_result). MySQL: ", mysql_stmt_error(stmt->stmt));
		return n;
	}
	res = mysql_stmt_result_metadata(stmt->stmt);
	num_cols = mysql_stmt_field_count(stmt->stmt);
	fields = mysql_fetch_fields(res);
	if (res) {
		return create_stmt_cursor(L, stmt->stmt, res, num_cols, fields);
	}

	if(num_cols == 0) { /* no tuples returned */
	/* query does not return data (it was not a SELECT) */
		lua_pushinteger(L, mysql_stmt_affected_rows(stmt->stmt));
		return 1;
	} else { /* mysql_use_result() should have returned data */
		return luasql_failmsg(L, "error retrieving result. MySQL: ", mysql_stmt_error(stmt->stmt));
	}
}

static int stmt_finalize(lua_State *L) {
    stmt_data *stmt = (stmt_data *)luaL_checkudata(L, 1, LUASQL_STATEMENT);

    if (!stmt->closed) {
        mysql_stmt_close(stmt->stmt);
        stmt->closed = 1;

        for (unsigned int i = 0; i < stmt->num_params; i++) {
            if (stmt->params_data[i].str) {
                free(stmt->params_data[i].str);
                stmt->params_data[i].str = NULL;
            }
        }

        free(stmt->params_data);
        stmt->params_data = NULL;

        free(stmt->params);
        stmt->params = NULL;

        luaL_unref(L, LUA_REGISTRYINDEX, stmt->conn);
    }

    lua_pushboolean(L, 1);
    return 1;
}


/*
** Commit the current transaction.
*/
static int conn_commit (lua_State *L) {
	conn_data *conn = getconnection (L);
	lua_pushboolean(L, !mysql_commit(conn->my_conn));
	return 1;
}



/*
** Rollback the current transaction.
*/
static int conn_rollback (lua_State *L) {
	conn_data *conn = getconnection (L);
	lua_pushboolean(L, !mysql_rollback(conn->my_conn));
	return 1;
}


/*
** Set "auto commit" property of the connection. Modes ON/OFF
*/
static int conn_setautocommit (lua_State *L) {
	conn_data *conn = getconnection (L);
	if (lua_toboolean (L, 2)) {
		mysql_autocommit(conn->my_conn, 1); /* Set it ON */
	}
	else {
		mysql_autocommit(conn->my_conn, 0);
	}
	lua_pushboolean(L, 1);
	return 1;
}


/*
** Get Last auto-increment id generated
*/
static int conn_getlastautoid (lua_State *L) {
  conn_data *conn = getconnection(L);
  lua_pushinteger(L, mysql_insert_id(conn->my_conn));
  return 1;
}

/*
** Create a new Connection object and push it on top of the stack.
*/
static int create_connection (lua_State *L, int env, MYSQL *const my_conn) {
	conn_data *conn = (conn_data *)LUASQL_NEWUD(L, sizeof(conn_data));
	luasql_setmeta (L, LUASQL_CONNECTION_MYSQL);
	/* fill in structure */
	conn->closed = 0;
	conn->env = LUA_NOREF;
	conn->my_conn = my_conn;
	lua_pushvalue (L, env);
	conn->env = luaL_ref (L, LUA_REGISTRYINDEX);
	return 1;
}


/*
** Connects to a data source.
**     param: one string for each connection parameter, said
**     datasource, username, password, host and port.
*/
static int env_connect (lua_State *L) {
	const char *sourcename = luaL_checkstring(L, 2);
	const char *username = luaL_optstring(L, 3, NULL);
	const char *password = luaL_optstring(L, 4, NULL);
	const char *host = luaL_optstring(L, 5, NULL);
	const int port = luaL_optinteger(L, 6, 0);
	const char *unix_socket = luaL_optstring(L, 7, NULL);
	const long client_flag = (long)luaL_optinteger(L, 8, 0);
	MYSQL *conn;
	getenvironment(L); /* validade environment */

	/* Try to init the connection object. */
	conn = mysql_init(NULL);
	if (conn == NULL)
		return luasql_faildirect(L, "error connecting: Out of memory.");

	if (!mysql_real_connect(conn, host, username, password, 
		sourcename, port, unix_socket, client_flag))
	{
		char error_msg[100];
		strncpy (error_msg,  mysql_error(conn), 99);
		mysql_close (conn); /* Close conn if connect failed */
		return luasql_failmsg (L, "error connecting to database. MySQL: ", error_msg);
	}
	return create_connection(L, 1, conn);
}


/*
**
*/
static int env_gc (lua_State *L) {
	env_data *env= (env_data *)luaL_checkudata (L, 1, LUASQL_ENVIRONMENT_MYSQL);	if (env != NULL && !(env->closed))
		env->closed = 1;
	return 0;
}


/*
** Close environment object.
*/
static int env_close (lua_State *L) {
	env_data *env= (env_data *)luaL_checkudata (L, 1, LUASQL_ENVIRONMENT_MYSQL);
	luaL_argcheck (L, env != NULL, 1, LUASQL_PREFIX"environment expected");
	if (env->closed) {
		lua_pushboolean (L, 0);
		lua_pushstring(L, "env is already closed");
		return 2;
	}
	mysql_library_end();
	env->closed = 1;
	lua_pushboolean (L, 1);
	return 1;
}


/*
** Create metatables for each class of object.
*/
static void create_metatables (lua_State *L) {
    struct luaL_Reg environment_methods[] = {
        {"__gc", env_gc},
		{"__close", env_gc},
        {"close", env_close},
        {"connect", env_connect},
		{NULL, NULL},
	};
    struct luaL_Reg connection_methods[] = {
        {"__gc", conn_gc},
		{"__close", conn_gc},
        {"close", conn_close},
        {"ping", conn_ping},
        {"escape", escape_string},
        {"execute", conn_execute},
        {"commit", conn_commit},
        {"rollback", conn_rollback},
        {"setautocommit", conn_setautocommit},
		{"getlastautoid", conn_getlastautoid},
		{"prepare", conn_prepare},
		{NULL, NULL},
    };
    struct luaL_Reg cursor_methods[] = {
        {"__gc", cur_gc},
		{"__close", cur_gc},
        {"close", cur_close},
        {"getcolnames", cur_getcolnames},
        {"getcoltypes", cur_getcoltypes},
        {"fetch", cur_fetch},
        {"numrows", cur_numrows},
        {"seek", cur_seek},
		{"nextresult", cur_next_result},
		{"hasnextresult", cur_has_next_result},
		{NULL, NULL},
    };
	struct luaL_Reg statement_methods[] = {
		{"__gc", stmt_finalize},
		{"__close", stmt_finalize},
        {"bind", stmt_bind},
        {"execute", stmt_execute},
        {"finalize", stmt_finalize},
        {NULL, NULL}
    };
	struct luaL_Reg statement_cursor_methods[] = {
		{"__gc", stmt_cur_gc},
		{"__close", stmt_cur_gc},
		{"close", stmt_cur_close},
		{"fields", stmt_cur_fields},
		{"fetch", stmt_cur_fetch},
        {NULL, NULL}
    };

	luasql_createmeta (L, LUASQL_ENVIRONMENT_MYSQL, environment_methods);
	luasql_createmeta (L, LUASQL_CONNECTION_MYSQL, connection_methods);
	luasql_createmeta (L, LUASQL_CURSOR_MYSQL, cursor_methods);
	luasql_createmeta(L, LUASQL_STATEMENT, statement_methods);
	luasql_createmeta(L, LUASQL_STATEMENT_CURSOR, statement_cursor_methods);
	lua_pop (L, 5);
}


/*
** Creates an Environment and returns it.
*/
static int create_environment (lua_State *L) {
	env_data *env = (env_data *)LUASQL_NEWUD(L, sizeof(env_data));
	luasql_setmeta (L, LUASQL_ENVIRONMENT_MYSQL);

	/* fill in structure */
	env->closed = 0;
	return 1;
}


/*
** Creates the metatables for the objects and registers the
** driver open method.
*/
LUASQL_API int luaopen_luasql_mysql (lua_State *L) { 
	struct luaL_Reg driver[] = {
		{"mysql", create_environment},
		{NULL, NULL},
	};
	create_metatables (L);
	lua_newtable(L);
	luaL_setfuncs(L, driver, 0);
	luasql_set_info (L);
    lua_pushliteral (L, "_CLIENTVERSION");
#ifdef MARIADB_CLIENT_VERSION_STR
lua_pushliteral (L, MARIADB_CLIENT_VERSION_STR);
#else
lua_pushliteral (L, MYSQL_SERVER_VERSION);
#endif
    lua_settable (L, -3);
	return 1;
}

LUAMOD_API int luaopen_mysql(lua_State *L) {
    return luaopen_luasql_mysql(L); 
}

