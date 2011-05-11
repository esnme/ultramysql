#ifdef __NOT_USED__

extern "C"
{
#include "amysql.h"
}

#include "socketdefs.h"
#include <stdio.h>
#include <time.h>
#include <string>
#include <list>
#include <vector>
#include <assert.h>
using namespace std;

struct API_Socket
{
	int sockfd;
};

void *API_createSocket(int family, int type, int proto)
{
	API_Socket *sock = new API_Socket;
	sock->sockfd = socket (AF_INET, SOCK_STREAM, 0);

	if (sock->sockfd == -1)
	{
		return NULL;;
	}

	SocketSetNonBlock(sock->sockfd, true);

	return sock;
}

int API_getSocketFD(void *sock)
{
	return ((API_Socket *)sock)->sockfd;
}

void API_closeSocket(void *sock)
{
	SocketClose( ((API_Socket *)sock)->sockfd);
}

void API_deleteSocket(void *sock)
{
	delete (API_Socket *) sock;
}

int API_wouldBlock(void *sock, int ops, int timeout)
{
	fd_set set;
	fd_set *readSet = NULL;
	fd_set *writeSet = NULL;
	fd_set *exceptSet = NULL;
	struct timeval tv;
		
	SOCKET sockfd =  ((API_Socket *)sock)->sockfd;
	FD_ZERO(&set);

	switch (ops)
	{
		case AMC_READ:
			FD_SET(sockfd, &set);
			readSet = &set;
			break;

		case AMC_WRITE:
			FD_SET(sockfd, &set);
			writeSet = &set;
			break;
	}

	tv.tv_sec = timeout;
	tv.tv_usec = 0;

	if (select (sockfd + 1, readSet, writeSet, exceptSet, &tv) == 1)
	{
		return 1;
	}

	return 0;
}


class Field
{
public:
	Field (int _type, const string &_name)
	{
		m_type = _type;
		m_name = _name;
	}

private:
	int m_type;
	string m_name;
};

class Column
{
public:
	Column (const string &_value)
	{
		m_value = _value;
	}

private:
	string m_value;
};

class Row
{

public:
	Row (int numFields)
	{
	}

	void addColumn (Column &col)
	{
		m_columns.push_back(col);
	}

private:
	list<Column> m_columns;
};

class Result
{

public:
	Result (int numFields)
	{
		m_numFields = numFields;
		//m_fields.reserve(numFields);
	}

	void addField (Field &field)
	{
		m_fields.push_back(field);
	}

	Row &currRow()
	{
		return m_rows.back();
	}
	
	void beginRow()
	{
		Row row(m_numFields);
		m_rows.push_back(row);
	}

	void endRow()
	{
	}

	void setError (const string &_error)
	{
		m_errorMessage = _error;
	}

	void setOK(int affected)
	{
		m_affectedRows = affected;
	}

	
private:
	list<Field> m_fields;
	list<Row> m_rows;
	int m_numFields;
	string m_errorMessage;
	int m_affectedRows;
};

void *API_createResult(int columns)
{
	return NULL;
	return (void *) new Result(columns);
}

void API_resultSetField(void *result, int column, int type, int charset, void *_name, size_t _cbName)
{
	return;
	string name ((char *) _name, _cbName);
	Field field(type, name);

	((Result *) result)->addField(field);
}

void API_resultRowBegin(void *result)
{
	return;
	((Result *) result)->beginRow();
}

void API_resultRowValue(void *result, int column, void *value, size_t cbValue)
{
	return;
	Column col(string((char*)value, cbValue));
	((Result *) result)->currRow().addColumn(col);

}

void API_resultRowEnd(void *result)
{
	return;
	((Result *) result)->endRow();
}

void API_destroyResult(void *result)
{
	return;
	delete (Result *) result;
}

void API_resultSetError(void *result, char *msg, size_t len)
{
	return;
	string errorMessage(msg, len);

	//fprintf (stderr, "%s: ERROR '%s'\n", __FUNCTION__, errorMessage.c_str());
	((Result *) result)->setError(errorMessage);

}

void API_resultSetOK(void *result, UINT64 affected, UINT64 insertId, int serverStatus, const char *_message, size_t len)
{
	return;
	string message(_message, len);
	((Result *) result)->setOK(affected);
}


DWORD *ThreadProc (void *arg)
{
	AMConnectionCAPI capi = {
		API_createSocket,
		API_getSocketFD,
		API_deleteSocket,
		API_closeSocket,
		API_wouldBlock,
		API_createResult,
		API_resultSetField,
		API_resultRowBegin,
		API_resultRowValue,
		API_resultRowEnd,
		API_destroyResult,
		API_resultSetError,
		API_resultSetOK,
	};

	AMConnection conn = AMConnection_Create(&capi);
	if (!AMConnection_Connect(conn, "localhost", 3306, "admin", "admin", "kaka", 0, NULL))
	{
		return NULL;
	}
	
	time_t tsStart = time (0);
	unsigned long count = 0;
	
	while (true)
	{
		//fprintf (stderr, "BEGIN QUERY----------------------------------------------\n");
		//Result *result = (Result *) conn.query("SELECT * from gameserver3 LIMIT 30");
		Result *result = (Result *) AMConnection_Query(conn, "SELECT 1");

		//assert (result);

		count ++;
		time_t tsNow = time (0);

		if (result)
		delete result;

		if (tsNow - tsStart > 30)
		{
			break;
		}
	}
	
	AMConnection_Destroy(conn);

	fprintf (stderr, "%s: Did %u queries in 30 seconds\n", __FUNCTION__, count);
	return NULL;
}




int main (int argc, char **argv)
{

	WSADATA wsaData;
	WSAStartup(MAKEWORD(2, 0), &wsaData);

	for (int index = 0; index < 1; index ++)
	{
		CreateThread (NULL, 0, (LPTHREAD_START_ROUTINE) ThreadProc, NULL, 0, NULL);
	}
	getchar();
	return 0;
}

#endif