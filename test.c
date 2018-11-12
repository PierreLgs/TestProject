/**
 * @Author: Pierre Lgs
 * @Date:   31-10-2018
 * @Email:  pierre.langlois@synlab.fr
 * @Filename: test.c
 * @Last modified by:   PierreLgs
 * @Last modified time: 12-11-2018
*/

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <dpi.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

int     GetPrivateProfileString(char *pszSection/*SECTION*/, char *pszEntry/*Varaible*/,
				char *pszDefault/*NULL*/, char *pszReturnBuf/*buffer*/,
				int iReturnBufSize/*strlen(psz)*/, char *Filename/*./*.ini*/,
				int iEntryNb);

#ifndef INI
#define INI "./file.ini"
#endif

char *type[] = { "VARCHAR",
"NVARCHAR","CHAR","NCHAR","ROWID","RAW","NATIVE_FLOAT","NATIVE_DOUBLE",
"NATIVE_INT","NATIVE_UINT","NUMBER","DATE","TIMESTAMP","TIMESTAMP_TZ",
"TIMESTAMP_LTZ","INTERVAL_DS","INTERVAL_YM","CLOB","NCLOB","BLOB","BFILE",
"STMT","BOOLEAN","OBJECT","LONG_VARCHAR","LONG_RAW"};

#ifndef TYPE_OFFSET
#define TYPE_OFFSET 2000
#endif

typedef struct ini_param
{
	char *user;
	char *mdp;
	char *nbr_fork;
	char *sql_query;
	char *nbr_query;
} ini_t;

void SELECT_print(dpiStmt *stmt, uint32_t numQueryColumns)
{
	uint32_t bufferRowIndex;
	dpiNativeTypeNum nativeTypeNum;
	dpiData *data;
	int found = 0;

	while (1) {
		dpiStmt_fetch(stmt, &found, &bufferRowIndex);
		if (!found)
			break;
		for (uint32_t i = 1; i < numQueryColumns; i++) {
			dpiStmt_getQueryValue(stmt, i, &nativeTypeNum, &data);
			printf("%.*s\t", data->value.asBytes.length, data->value.asBytes.ptr);
		}
		printf("\n");
	}
	printf("\n");
	fflush(stdout);
}

void METADATA_print(dpiStmt *stmt, uint32_t numQueryColumns)
{
	dpiQueryInfo queryInfo;

	printf("Display column metadata\n");
	for (int i = 0; i < numQueryColumns; i++) {
		if (dpiStmt_getQueryInfo(stmt, i + 1, &queryInfo) < 0)
			return;
		printf("('%.*s', %s, %d, %d, %d, %d, %d)\n", queryInfo.nameLength,
			queryInfo.name, type[queryInfo.typeInfo.oracleTypeNum-TYPE_OFFSET],
			queryInfo.typeInfo.sizeInChars, queryInfo.typeInfo.clientSizeInBytes,
			queryInfo.typeInfo.precision, queryInfo.typeInfo.scale,
			queryInfo.nullOk);
		fflush(stdout);
	}
}

ini_t *load_ini_param(void)
{
	ini_t *IniParam = calloc(1, sizeof(ini_t));

	IniParam->nbr_query = calloc(TYPE_OFFSET, sizeof(char));
	IniParam->user = calloc(TYPE_OFFSET, sizeof(char));
	IniParam->mdp = calloc(TYPE_OFFSET, sizeof(char));
	IniParam->nbr_fork = calloc(TYPE_OFFSET, sizeof(char));
	IniParam->sql_query = calloc(TYPE_OFFSET, sizeof(char));

	GetPrivateProfileString("GENERAL", "USER", "NULL", IniParam->user, 100, INI, -1);
	GetPrivateProfileString("GENERAL", "MDP", "NULL", IniParam->mdp, 100, INI, -1);
	GetPrivateProfileString("GENERAL", "NBR_QUERY", "NULL", IniParam->nbr_query, 100, INI, -1);
	GetPrivateProfileString("GENERAL", "NBR_FORK", "NULL", IniParam->nbr_fork, 100, INI, -1);
	GetPrivateProfileString("GENERAL", "SQL_QUERY", "NULL", IniParam->sql_query, 100, INI, -1);
	return (IniParam);
}

int main(int argc, char const *argv[])
{
	dpiContext *context = NULL;
	dpiPool *pool;
	dpiConn *conn = NULL;
	dpiStmt *stmt = NULL;
	dpiErrorInfo errorInfo;
	uint32_t numQueryColumns = 0;
	pid_t main_pid = getpid();

	ini_t *ini_param = load_ini_param();

	if (dpiContext_create(DPI_MAJOR_VERSION, DPI_MINOR_VERSION, &context, &errorInfo) != 0)
		return (1);

	if (dpiPool_create(context, ini_param->user, strlen(ini_param->user), ini_param->mdp, strlen(ini_param->mdp), NULL, 0, NULL, NULL, &pool) != 0)
		return (1);

	for (int i = 0; i < atoi(ini_param->nbr_fork); i++) {
		if (getpid() == main_pid)
			fork();
	}

	if (dpiPool_acquireConnection(pool, NULL, 0, NULL, 0, NULL, &conn) != 0)
		return (1);

	for (int i = 0; i < atoi(ini_param->nbr_query); i++) {
		if (dpiConn_prepareStmt(conn, false, ini_param->sql_query, strlen(ini_param->sql_query), NULL, 0, &stmt) != 0)
			return (1);
		if (dpiStmt_execute(stmt, 0, &numQueryColumns) != 0)
			return (1);
	}


//	SELECT_print(stmt, numQueryColumns);
//	METADATA_print(stmt, numQueryColumns);

	dpiStmt_release(stmt);
	dpiPool_release(pool);
	dpiConn_close(conn, DPI_MODE_CONN_CLOSE_DEFAULT, NULL, 0);
	return 0;
}
