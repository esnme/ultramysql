#ifndef __AMYSQL_H__
#define __AMYSQL_H__

#include "amdefs.h"

#define EXPORTFUNCTION extern "C" __declspec(dllexport)

enum AMConnection_Ops
{
	AMC_READ,
	AMC_WRITE,
};

typedef struct 
{
	UINT8 type;
	UINT16 flags;
	UINT16 charset;
} AMTypeInfo;

typedef struct __AMConnectionCAPI
{
	void *(*createSocket)(int family, int type, int proto);
	int (*getSocketFD)(void *instance);
	void (*deleteSocket)(void *instance);
	void (*closeSocket)(void *instance);
	int (*wouldBlock)(void *instance, int fd, int ops, int timeout);

	void *(*createResult)(int columns);
	void (*resultSetField)(void *result, int ifield, AMTypeInfo *ti, void *name, size_t cbName);
	void (*resultRowBegin)(void *result);
	int (*resultRowValue)(void *result, int icolumn, AMTypeInfo *ti, void *value, size_t cbValue);
	void (*resultRowEnd)(void *result);
	void (*destroyResult)(void *result);
	void *(*resultOK)(UINT64 affected, UINT64 insertId, int serverStatus, const char *message, size_t len);
} AMConnectionCAPI;


typedef void * AMConnection;

//#ifdef _WIN32
//#define EXPORT_ATTR __declspec(dllexport)
//#define EXPORT_ATTR __attribute__ ((dllexport))
//#define EXPORT_ATTR extern "C" __declspec(dllexport)
//#else
#define EXPORT_ATTR
//#endif

AMConnection AMConnection_Create(AMConnectionCAPI *_capi);
void AMConnection_Destroy(AMConnection _conn);
void *AMConnection_Query(AMConnection conn, const char *_query, size_t _cbQuery);
int  AMConnection_Connect (AMConnection conn, const char *_host, int _port, const char *_username, const char *_password, const char *_database, int *_autoCommit, int _charset);
int AMConnection_GetLastError (AMConnection conn, const char **_ppOutMessage, int *_outErrno);
int AMConnection_GetTxBufferSize (AMConnection conn);
int AMConnection_GetRxBufferSize (AMConnection conn);
int AMConnection_IsConnected (AMConnection conn);
int AMConnection_Close (AMConnection conn);

#endif
