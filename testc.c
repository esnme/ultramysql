#include "amysql.h"
#include "socketdefs.h"
#include <time.h>

typedef struct __API_Socket
{
	int sockfd;
} API_Socket;

void *API_createSocket(int family, int type, int proto)
{
	API_Socket *sock = (API_Socket *) malloc(sizeof (API_Socket));
	sock->sockfd = socket (AF_INET, SOCK_STREAM, 0);

	if (sock->sockfd == -1)
	{
		return NULL;;
	}

	SocketSetNonBlock(sock->sockfd, 1);

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
	free (sock);
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

void *API_createResult(int columns)
{
	return NULL;
}

void API_resultSetField(void *result, int column, int type, int charset, void *_name, size_t _cbName)
{
	return;
}

void API_resultRowBegin(void *result)
{
	return;
}

void API_resultRowValue(void *result, int column, void *value, size_t cbValue)
{
	return;
}

void API_resultRowEnd(void *result)
{
	return;
}

void API_destroyResult(void *result)
{
	return;
}

void API_resultSetError(void *result, char *msg, size_t len)
{
	return;
}

void API_resultSetOK(void *result, UINT64 affected, UINT64 insertId, int serverStatus, const char *_message, size_t len)
{
	return;
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

	time_t tsStart;
	unsigned long count = 0;

	AMConnection conn = AMConnection_Create(&capi);
	if (!AMConnection_Connect(conn, "localhost", 3306, "admin", "admin", "kaka", 0, NULL))
	{
		return NULL;
	}
	
	tsStart = time (0);

	
	while (1)
	{
		time_t tsNow;
		//fprintf (stderr, "BEGIN QUERY----------------------------------------------\n");
		//Result *result = (Result *) conn.query("SELECT * from gameserver3 LIMIT 30");
		void *result = AMConnection_Query(conn, "SELECT 1");

		//assert (result);

		count ++;
		tsNow = time (0);

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
	int index;
	WSADATA wsaData;
	WSAStartup(MAKEWORD(2, 0), &wsaData);

	for (index = 0; index < 1; index ++)
	{
		CreateThread (NULL, 0, (LPTHREAD_START_ROUTINE) ThreadProc, NULL, 0, NULL);
	}
	getchar();
	return 0;
}
