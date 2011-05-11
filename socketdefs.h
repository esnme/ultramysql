#ifndef __SOCKETDEFS_H__
#define __SOCKETDEFS_H__

#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <Ws2tcpip.h>
typedef int socklen_t;
#define SocketClose(_fd) closesocket((_fd))
#define SocketWouldBlock(_fd) (WSAGetLastError () == WSAEWOULDBLOCK)
#define SocketEINPROGRESS(_fd) (WSAGetLastError () == WSAEINPROGRESS)

#define SocketGetLastError()  ((int) WSAGetLastError ())

#define SocketSetNonBlock(_fd, _state) \
{ \
	unsigned long flags = (_state) ? 1 : 0; \
	ioctlsocket((_fd), FIONBIO, &flags); \
} \

#else

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <unistd.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>
typedef int SOCKET;
#define SocketClose(_fd) close((_fd))
#define SocketWouldBlock(_fd) (errno == EAGAIN)
#define SocketEINPROGRESS(_fd) (errno == EINPROGRESS)
#define SocketSetNonBlock(_fd, _state) \
{ \
	if ((_state) \
		fcntl ((_fd), F_SETFL, O_NONBLOCK); \
	else \
		fcntl ((_fd), F_SETFL, 0); \
} \

#define SocketGetLastError() (errno)

#endif

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif



#endif
