#include <amysql.h>
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

void *API_createSocket(int family, int type, int proto)
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

int API_getSocketFD(void *sock)
{
	int ret;
	PyObject *fdobj;
	PRINTMARK();
	
	fdobj = PyObject_CallMethod ((PyObject *) sock, "fileno", NULL);
	PRINTMARK();

	if (fdobj == NULL)
	{
		PRINTMARK();
		return -1;
	}

	if (!PyInt_Check(fdobj))
	{
		Py_XDECREF(fdobj);
		PRINTMARK();
		return -1;
	}

	ret = PyInt_AS_LONG(fdobj);
	
	Py_DECREF(fdobj);
	return ret;
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

int API_wouldBlock(void *sock, int fd, int ops, int timeout)
{
	struct timeval tv;
	int result;
	fd_set readSet;
	fd_set writeSet;

	tv.tv_sec = timeout;
	tv.tv_usec = 0;

	FD_ZERO(&writeSet);
	FD_ZERO(&readSet);

	switch (ops)
	{
		case AMC_READ:
			FD_SET (fd, &readSet);
			break;

		case AMC_WRITE:
			FD_SET (fd, &writeSet);
			break;
	}

	result = select (fd + 1, &readSet, &writeSet, NULL, &tv);

	if (result < 1)
	{
		return 0;
	}

	return 1;
}
