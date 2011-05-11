#ifndef __AMCONNECTION_H__
#define __AMCONNECTION_H___

#include <string>
#include "amysql.h"
#include "socketdefs.h"
#include "PacketReader.h"
#include "PacketWriter.h"



class Connection
{
	enum State
	{
		NONE,
		CONNECT,
		HANDSHAKE_RECV,
		HANDSHAKE_SEND,
		HANDSHAKE_REPLY,
		QUERY_WAIT,
		QUERY_SEND,
		QUERY_RECV,
		DISCONNECT,
		FAILED,
	};



private:
	State m_state;

	std::string m_host;
	int m_port;
	std::string m_username;
	std::string m_password;
	std::string m_database;
	bool m_autoCommit;
	MYSQL_CHARSETS m_charset;
	SOCKET m_sockfd;
	void *m_sockInst;
	PacketReader m_reader;
	PacketWriter m_writer;
	UINT32 m_clientCaps;
	std::string m_query;

	std::string m_errorMessage;
	int m_errno;

	AMConnectionCAPI m_capi;

public:


public:
	Connection(AMConnectionCAPI *_capi);
	~Connection();
	bool connect(const char *_host, int _port, const char *_username, const char *_password, const char *_database, int *_autoCommit, MYSQL_CHARSETS _charset);
	//void handleSocketEvent (SocketEvents _evt);
	void *query(const char *_query, size_t _cbQuery);
	bool getLastError (const char **_ppOutMessage, int *_outErrno);

	int getRxBufferSize();
	int getTxBufferSize();
	bool isConnected(void);
	bool close(void);

protected:
	void changeState(State _newState, const char *message);
	bool connectSocket();
	bool readSocket();
	bool writeSocket();
	bool processHandshake();
	void scramble(const char *_scramble1, const char *_scramble2, UINT8 _outToken[20]);
	bool recvPacket();
	bool sendPacket();

	void handleErrorPacket();
	void handleEOFPacket();
	void *handleResultPacket(int fieldCount);
	void *handleOKPacket();
	void setError (const char *_message, int _errno);

protected:
};

#endif