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
#include <umysql.h>
#include <Python.h>
#include <stdio.h>
#include <socketdefs.h>

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

#ifndef snwprintf
#define snwprintf _snwprintf
#endif

#ifndef snprintf
#define snprintf _snprintf
#endif

#ifndef alloca
#define alloca _alloca
#endif

//#define PRINTMARK() fprintf(stderr, "%s: MARK(%d)\n", __FILE__, __LINE__)		
#define PRINTMARK() 		

void *API_getSocket()
{
  /* Create a normal socket */
  PyObject *sockobj;
  //FIXME: PyModule will leak
  static PyObject *sockmodule = NULL;
  static PyObject *sockclass = NULL;
  static int once = 1;

  if (once)
  {
    /*FIXME: References for module or class are never released */
    sockmodule = PyImport_ImportModule ("socket");

    if (sockmodule == NULL)
    {
      PRINTMARK();
      return NULL;
    }
    sockclass = PyObject_GetAttrString(sockmodule, "socket");

    if (sockclass == NULL)
    {
      PRINTMARK();
      return NULL;
    }

    //FIXME: PyType will leak
    if (!PyType_Check(sockclass))
    {
      PRINTMARK();
      return NULL;
    }

    if (!PyCallable_Check(sockclass))
    {
      PRINTMARK();
      return NULL;
    }

    once = 0;
  }

  PRINTMARK();
  sockobj = PyObject_Call (sockclass, PyTuple_New(0), NULL);
  PRINTMARK();

  if (sockobj == NULL)
  {
    PRINTMARK();
    return NULL;
  }

  PRINTMARK();
  return sockobj;
}

int API_setTimeout(void *sock, int timeoutSec)
{
  PyObject *intobj;
  PyObject *retobj;
  PyObject *methodObj;

  PRINTMARK();
  intobj = PyFloat_FromDouble( (double) timeoutSec);

  methodObj = PyString_FromString("settimeout");
  PRINTMARK();
  retobj = PyObject_CallMethodObjArgs ((PyObject *) sock, methodObj, intobj, NULL);
  Py_DECREF(intobj);
  Py_DECREF(methodObj);
  PRINTMARK();

  if (retobj == NULL)
  {
    PyErr_Clear();
    return 0;
  }

  Py_DECREF(retobj);
  return 1;

}   

void API_closeSocket(void *sock)
{
  PyObject *res = PyObject_CallMethod( (PyObject *) sock, "close", NULL);

  if (res == NULL)
  {
    PRINTMARK();
    return;
  }

  Py_DECREF(res);
}

void API_deleteSocket(void *sock)
{
  Py_DECREF( (PyObject *) sock);
}

int API_connectSocket(void *sock, const char *host, int port)
{
  PyObject *res;
  PyObject *addrTuple;
  PyObject *connectStr;

  PRINTMARK();

  addrTuple = PyTuple_New(2);
  PyTuple_SET_ITEM(addrTuple, 0, PyString_FromString(host));
  PyTuple_SET_ITEM(addrTuple, 1, PyInt_FromLong(port));

  connectStr = PyString_FromString("connect");
  res = PyObject_CallMethodObjArgs( (PyObject *) sock, connectStr, addrTuple, NULL);

  Py_DECREF(connectStr);
  Py_DECREF(addrTuple);

  if (res == NULL)
  {
    PRINTMARK();
    return 0;
  }

  PRINTMARK();

  Py_DECREF(res);
  return 1;
}

int API_recvSocket(void *sock, char *buffer, int cbBuffer)
{
  PyObject *res;
  PyObject *bufSize;
  PyObject *funcStr;
  int ret;

  funcStr = PyString_FromString("recv");
  bufSize = PyInt_FromLong(cbBuffer);
  res = PyObject_CallMethodObjArgs ((PyObject *) sock, funcStr, bufSize, NULL);
  Py_DECREF(funcStr);
  Py_DECREF(bufSize);

  if (res == NULL)
  {
    return -1;
  }

  ret = (int) PyString_GET_SIZE(res);
  memcpy (buffer, PyString_AS_STRING(res), ret);
  Py_DECREF(res);
  return ret;
}

int API_sendSocket(void *sock, const char *buffer, int cbBuffer)
{
  PyObject *res;
  PyObject *pybuffer;
  PyObject *funcStr;
  int ret;

  funcStr = PyString_FromString("send");
  pybuffer = PyString_FromStringAndSize(buffer, cbBuffer);
  res = PyObject_CallMethodObjArgs ((PyObject *) sock, funcStr, pybuffer, NULL);
  Py_DECREF(funcStr);
  Py_DECREF(pybuffer);

  if (res == NULL)
  {
    return -1;
  }

  ret = (int) PyInt_AsLong(res);
  Py_DECREF(res);
  return ret;
}

