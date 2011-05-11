extern "C"
{
	#include "amysql.h"
}

#include "Connection.h"

EXPORT_ATTR AMConnection AMConnection_Create(AMConnectionCAPI *_capi)
{
	return (AMConnection) new Connection(_capi);
}

EXPORT_ATTR void AMConnection_Destroy(AMConnection conn)
{
	delete (Connection *)conn;
}

EXPORT_ATTR void * AMConnection_Query(AMConnection conn, const char *_query, size_t _cbQuery)
{
	return ((Connection *)conn)->query(_query, _cbQuery);
}

EXPORT_ATTR int AMConnection_Connect (AMConnection conn, const char *_host, int _port, const char *_username, const char *_password, const char *_database, int *_autoCommit, int _charset)
{
	return ((Connection *)conn)->connect(_host, _port, _username, _password, _database, _autoCommit, (MYSQL_CHARSETS) _charset) ? 1 : 0;
}

EXPORT_ATTR int AMConnection_GetLastError (AMConnection conn, const char **_ppOutMessage, int *_outErrno)
{
	return ((Connection *)conn)->getLastError(_ppOutMessage, _outErrno) ? 1 : 0;
}

EXPORT_ATTR int AMConnection_GetTxBufferSize (AMConnection conn)
{
	return ((Connection *)conn)->getTxBufferSize();
}

EXPORT_ATTR int AMConnection_GetRxBufferSize (AMConnection conn)
{
	return ((Connection *)conn)->getRxBufferSize();
}

EXPORT_ATTR int AMConnection_IsConnected (AMConnection conn)
{
	return ((Connection *)conn)->isConnected() ? 1 : 0;
}

EXPORT_ATTR int AMConnection_Close (AMConnection conn)
{
	return ((Connection *)conn)->close() ? 1 : 0;
}
