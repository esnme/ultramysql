#include <amysql.h>
#include <Python.h>
#include <stdio.h>

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
		sockmodule = PyImport_ImportModule ("gevent.socket");

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
	/* Setup gevent and yield to gevent hub */

	static int once = 1;
	static PyObject *sockmodule = NULL;
	static PyObject *waitread = NULL;
	static PyObject *waitwrite = NULL;

	PyObject *resObject = NULL;
	PyObject *argList;
	PyObject *kwargList;

	if (once)
	{
		/*FIXME: References for module, class or methods are never released */
		sockmodule = PyImport_ImportModule ("gevent.socket");

		if (sockmodule == NULL)
		{
			PRINTMARK();
			return -1;
		}

		waitread = PyObject_GetAttrString(sockmodule, "wait_read");
		waitwrite = PyObject_GetAttrString(sockmodule, "wait_write");

		if (waitread == NULL || waitwrite == NULL)
		{
			PRINTMARK();
			return -1;
		}

		if (!PyFunction_Check(waitread) || !PyFunction_Check(waitwrite))
		{
			PRINTMARK();
			return -1;
		}
		
		PRINTMARK();
		once = 0;
	}


	PRINTMARK();
	//FIXME: do this once
	argList = PyTuple_New(1);
	PyTuple_SET_ITEM(argList, 0, PyInt_FromLong(fd));
	kwargList = PyDict_New();
	PyDict_SetItemString(kwargList, "timeout", PyInt_FromLong(timeout));

	PRINTMARK();

	switch (ops)
	{
		case AMC_READ:
			PRINTMARK();

			resObject = PyObject_Call (waitread, argList, kwargList); 
			PRINTMARK();
			break;

		case AMC_WRITE:
			PRINTMARK();
			resObject = PyObject_Call (waitwrite, argList, kwargList); 
			PRINTMARK();
			break;
	}

	Py_DECREF(argList);
	Py_DECREF(kwargList);

	if (resObject == NULL)
	{
		PRINTMARK();
		return 0;
	}

	PRINTMARK();
	Py_DECREF(resObject);
	PRINTMARK();

	return 1;
}

