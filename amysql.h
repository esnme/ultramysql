/*
Copyright (c) 2011, Jonas Tarnstrom and ESN Social Software AB
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
3. All advertising materials mentioning features or use of this software
   must display the following acknowledgement:
   This product includes software developed by ESN Social Software AB (www.esn.me).
4. Neither the name of the ESN Social Software AB nor the
   names of its contributors may be used to endorse or promote products
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY ESN SOCIAL SOFTWARE AB ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL ESN SOCIAL SOFTWARE AB BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Portions of code from gevent-MySQL
Copyright (C) 2010, Markus Thurlin
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.

    * Neither the name of Hyves (Startphone Ltd.) nor the names of its
      contributors may be used to endorse or promote products derived from this
      software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
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
	int (*connectSocket)(void *sock, const char *host, int port);

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
