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
#include <structmember.h>
#include <stdio.h>
#include <string.h>
#include <datetime.h>
#include <stdlib.h>

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

//#define PRINTMARK() fprintf(stderr, "%08x:%s:%s MARK(%d)\n", GetTickCount(), __FILE__, __FUNCTION__, __LINE__)		
#define PRINTMARK() 		


static PyTypeObject ConnectionType;
static PyTypeObject ResultSetType;

PyObject *umysql_Error;
PyObject *umysql_SQLError;

typedef struct {
  PyObject_HEAD
    PyObject *fields; 
  PyObject *rows;
  PyObject *currRow;
  int numFields;

} ResultSet;

int ResultSet_setup(ResultSet *self, int columns);


typedef struct {
  PyObject_HEAD
    UMConnection conn;
  int sockfd;

  PyObject *Error;
  PyObject *SQLError;

  int txBufferSize;
  int rxBufferSize;
  int charset;
  PyObject *(*PFN_PyUnicode_Encode)(const Py_UNICODE *data, Py_ssize_t length, const char *errors);
} Connection;

void *API_getSocket();
void API_closeSocket(void *sock);
void API_deleteSocket(void *sock);
int API_connectSocket(void *sock, const char *host, int port);
int API_setTimeout(void *sock, int timeoutSec);
int API_recvSocket(void *sock, char *buffer, int cbBuffer);
int API_sendSocket(void *sock, const char *buffer, int cbBuffer);

void API_clearException(void)
{
  PyErr_Clear();
}

void *API_createResult(int columns)
{
  ResultSet *ret;
  ret = PyObject_New(ResultSet, &ResultSetType);
  ResultSet_setup(ret, columns);

  return ret;
}

void API_resultSetField(void *result, int column, UMTypeInfo *ti, void *_name, size_t _cbName)
{
  PyObject *field = PyTuple_New(2);
  PyTuple_SET_ITEM(field, 0, PyString_FromStringAndSize((const char *)_name, _cbName));
  PyTuple_SET_ITEM(field, 1, PyInt_FromLong(ti->type));
  PyTuple_SET_ITEM(((ResultSet *) result)->fields, column, field);
  PRINTMARK();
  return;
}

void API_resultRowBegin(void *result)
{
  PRINTMARK();
  ((ResultSet *)result)->currRow = PyTuple_New(((ResultSet *)result)->numFields);	
  PRINTMARK();
}

PyObject *HandleError(Connection *self, const char *funcName);

void API_resultRowEnd(void *result)
{
  PyList_Append(((ResultSet *)result)->rows, ((ResultSet *)result)->currRow);
  Py_DECREF((PyObject *)((ResultSet *)result)->currRow);
  ((ResultSet *)result)->currRow = NULL;
  return;
}

void API_destroyResult(void *result)
{
  PRINTMARK();
  Py_DECREF( (PyObject *) result);
  PRINTMARK();

  return;
}

void *API_resultOK(UINT64 affected, UINT64 insertId, int serverStatus, const char *_message, size_t len)
{
  PyObject *tuple = PyTuple_New(2);

  PyTuple_SET_ITEM(tuple, 0, PyLong_FromUnsignedLongLong(affected));
  PyTuple_SET_ITEM(tuple, 1, PyLong_FromUnsignedLongLong(insertId));

  return (void *) tuple;
}


INT32 parseINT32(char *start, char *end)
{
  INT32 intValue = 0;
  INT32 intNeg = 1;
  INT32 chr;

  if (*start == '-')
  {
    start ++;
    intNeg = -1;
  }

  while (start < end)
  {
    chr = (INT32) (unsigned char) *(start++);

    switch (chr)
    {
    case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
      intValue = intValue * 10 + (INT32) (chr - 48);
      break;

    default:
      break;
    }
  }

  return intValue * intNeg;
}

INT64 parseINT64(char *start, char *end)
{
  INT64 intValue = 0;
  INT64 intNeg = 1;
  INT64 chr;

  if (*start == '-')
  {
    start ++;
    intNeg = -1;
  }

  while (start < end)
  {
    chr = (INT32) (unsigned char) *(start++);

    switch (chr)
    {
    case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
      intValue = intValue * 10 + (INT64) (chr - 48);
      break;

    default:
      break;
    }
  }

  return intValue * intNeg;
}

static PyObject *DecodeString (UMTypeInfo *ti, char *value, size_t cbValue)
{
  //FIXME: This code must be endiness aware of system isn't little endian

  switch (ti->charset)
  {
  case MCS_big5_chinese_ci://1,
  case MCS_big5_bin://84,
  case MCS_dec8_swedish_ci://3,
  case MCS_dec8_bin://69,
  case MCS_cp850_general_ci://4,
  case MCS_cp850_bin://80,
  case MCS_hp8_english_ci://6,
  case MCS_hp8_bin://72,
  case MCS_koi8r_general_ci://7,
  case MCS_koi8r_bin://74,
    break;

  case MCS_latin1_german1_ci://5,
  case MCS_latin1_swedish_ci://8,
  case MCS_latin1_danish_ci://15,
  case MCS_latin1_german2_ci://31,
  case MCS_latin1_bin://47,
  case MCS_latin1_general_ci://48,
  case MCS_latin1_general_cs://49,
  case MCS_latin1_spanish_ci://94,
    return PyUnicode_DecodeLatin1 (value, cbValue, NULL);

  case MCS_latin2_czech_cs:// 2,
  case MCS_latin2_general_ci://9,
  case MCS_latin2_hungarian_ci://21,
  case MCS_latin2_croatian_ci://27,
  case MCS_latin2_bin://77,
  case MCS_swe7_swedish_ci://10,
  case MCS_swe7_bin://82,
    break;

  case MCS_ascii_general_ci://11,
  case MCS_ascii_bin://65,
    return PyUnicode_DecodeASCII(value, cbValue, NULL);

  case MCS_ujis_japanese_ci://12,
  case MCS_ujis_bin://91,
  case MCS_sjis_japanese_ci://13,
  case MCS_sjis_bin://88,
  case MCS_hebrew_general_ci://16,
  case MCS_hebrew_bin://71,
  case MCS_tis620_thai_ci://18,
  case MCS_tis620_bin://89,
  case MCS_euckr_korean_ci://19,
  case MCS_euckr_bin://85,
  case MCS_koi8u_general_ci://22,
  case MCS_koi8u_bin://75,
  case MCS_gb2312_chinese_ci://24,
  case MCS_gb2312_bin://86,
  case MCS_greek_general_ci://25,
  case MCS_greek_bin://70,
    break;

  case MCS_cp1250_general_ci://26,
  case MCS_cp1250_czech_cs://34,
  case MCS_cp1250_croatian_ci://44,
  case MCS_cp1250_bin://66,
  case MCS_cp1250_polish_ci://99,
    return PyUnicode_Decode(value, cbValue, "cp1250", NULL);

  case MCS_gbk_chinese_ci://28,
  case MCS_gbk_bin://87,
  case MCS_latin5_turkish_ci://30,
  case MCS_latin5_bin://78,
  case MCS_armscii8_general_ci://32,
  case MCS_armscii8_bin://64,
    break;

  case MCS_utf8_general_ci://33,
  case MCS_utf8_bin://83,
  case MCS_utf8_unicode_ci://192,
  case MCS_utf8_icelandic_ci://193,
  case MCS_utf8_latvian_ci://194,
  case MCS_utf8_romanian_ci://195,
  case MCS_utf8_slovenian_ci://196,
  case MCS_utf8_polish_ci://197,
  case MCS_utf8_estonian_ci://198,
  case MCS_utf8_spanish_ci://199,
  case MCS_utf8_swedish_ci://200,
  case MCS_utf8_turkish_ci://201,
  case MCS_utf8_czech_ci://202,
  case MCS_utf8_danish_ci://203,
  case MCS_utf8_lithuanian_ci://204,
  case MCS_utf8_slovak_ci://205,
  case MCS_utf8_spanish2_ci://206,
  case MCS_utf8_roman_ci://207,
  case MCS_utf8_persian_ci://208,
  case MCS_utf8_esperanto_ci://209,
  case MCS_utf8_hungarian_ci://210,
  case MCS_utf8_sinhala_ci://211,
    return PyUnicode_DecodeUTF8 (value, cbValue, NULL);

  case MCS_ucs2_general_ci://35,
  case MCS_ucs2_bin://90,
  case MCS_ucs2_unicode_ci://128,
  case MCS_ucs2_icelandic_ci://129,
  case MCS_ucs2_latvian_ci://130,
  case MCS_ucs2_romanian_ci://131,
  case MCS_ucs2_slovenian_ci://132,
  case MCS_ucs2_polish_ci://133,
  case MCS_ucs2_estonian_ci://134,
  case MCS_ucs2_spanish_ci://135,
  case MCS_ucs2_swedish_ci://136,
  case MCS_ucs2_turkish_ci://137,
  case MCS_ucs2_czech_ci://138,
  case MCS_ucs2_danish_ci://139,
  case MCS_ucs2_lithuanian_ci://140,
  case MCS_ucs2_slovak_ci://141,
  case MCS_ucs2_spanish2_ci://142,
  case MCS_ucs2_roman_ci://143,
  case MCS_ucs2_persian_ci://144,
  case MCS_ucs2_esperanto_ci://145,
  case MCS_ucs2_hungarian_ci://146,
  case MCS_ucs2_sinhala_ci://147,
    break;

  case MCS_cp866_general_ci://36,
  case MCS_cp866_bin://68,
  case MCS_keybcs2_general_ci://37,
  case MCS_keybcs2_bin://73,
  case MCS_macce_general_ci://38,
  case MCS_macce_bin://43,
  case MCS_macroman_general_ci://39,
  case MCS_macroman_bin://53,
  case MCS_cp852_general_ci://40,
  case MCS_cp852_bin://81,
  case MCS_latin7_estonian_cs://20,
  case MCS_latin7_general_ci://41,
  case MCS_latin7_general_cs://42,
  case MCS_latin7_bin://79,
  case MCS_utf8mb4_general_ci://45,
  case MCS_utf8mb4_bin://46,
  case MCS_utf8mb4_unicode_ci://224,
  case MCS_utf8mb4_icelandic_ci://225,
  case MCS_utf8mb4_latvian_ci://226,
  case MCS_utf8mb4_romanian_ci://227,
  case MCS_utf8mb4_slovenian_ci://228,
  case MCS_utf8mb4_polish_ci://229,
  case MCS_utf8mb4_estonian_ci://230,
  case MCS_utf8mb4_spanish_ci://231,
  case MCS_utf8mb4_swedish_ci://232,
  case MCS_utf8mb4_turkish_ci://233,
  case MCS_utf8mb4_czech_ci://234,
  case MCS_utf8mb4_danish_ci://235,
  case MCS_utf8mb4_lithuanian_ci://236,
  case MCS_utf8mb4_slovak_ci://237,
  case MCS_utf8mb4_spanish2_ci://238,
  case MCS_utf8mb4_roman_ci://239,
  case MCS_utf8mb4_persian_ci://240,
  case MCS_utf8mb4_esperanto_ci://241,
  case MCS_utf8mb4_hungarian_ci://242,
  case MCS_utf8mb4_sinhala_ci://243,
  case MCS_cp1251_bulgarian_ci://14,
  case MCS_cp1251_ukrainian_ci://23,
  case MCS_cp1251_bin://50,
  case MCS_cp1251_general_ci://51,
  case MCS_cp1251_general_cs://52,
    break;

  case MCS_utf16_general_ci://54,
  case MCS_utf16_bin://55,
  case MCS_utf16_unicode_ci://101,
  case MCS_utf16_icelandic_ci://102,
  case MCS_utf16_latvian_ci://103,
  case MCS_utf16_romanian_ci://104,
  case MCS_utf16_slovenian_ci://105,
  case MCS_utf16_polish_ci://106,
  case MCS_utf16_estonian_ci://107,
  case MCS_utf16_spanish_ci://108,
  case MCS_utf16_swedish_ci://109,
  case MCS_utf16_turkish_ci://110,
  case MCS_utf16_czech_ci://111,
  case MCS_utf16_danish_ci://112,
  case MCS_utf16_lithuanian_ci://113,
  case MCS_utf16_slovak_ci://114,
  case MCS_utf16_spanish2_ci://115,
  case MCS_utf16_roman_ci://116,
  case MCS_utf16_persian_ci://117,
  case MCS_utf16_esperanto_ci://118,
  case MCS_utf16_hungarian_ci://119,
  case MCS_utf16_sinhala_ci://120,
    //return PyUnicode_DecodeUTF16(value, cbValue / 2, NULL, NULL);
    break;

  case MCS_cp1256_general_ci://57,
  case MCS_cp1256_bin://67,
    break;

  case MCS_cp1257_lithuanian_ci://29,
  case MCS_cp1257_bin://58,
  case MCS_cp1257_general_ci://59,
    break;

  case MCS_utf32_general_ci://60,
  case MCS_utf32_bin://61,
  case MCS_utf32_unicode_ci://160,
  case MCS_utf32_icelandic_ci://161,
  case MCS_utf32_latvian_ci://162,
  case MCS_utf32_romanian_ci://163,
  case MCS_utf32_slovenian_ci://164,
  case MCS_utf32_polish_ci://165,
  case MCS_utf32_estonian_ci://166,
  case MCS_utf32_spanish_ci://167,
  case MCS_utf32_swedish_ci://168,
  case MCS_utf32_turkish_ci://169,
  case MCS_utf32_czech_ci://170,
  case MCS_utf32_danish_ci://171,
  case MCS_utf32_lithuanian_ci://172,
  case MCS_utf32_slovak_ci://173,
  case MCS_utf32_spanish2_ci://174,
  case MCS_utf32_roman_ci://175,
  case MCS_utf32_persian_ci://176,
  case MCS_utf32_esperanto_ci://177,
  case MCS_utf32_hungarian_ci://178,
  case MCS_utf32_sinhala_ci://179,
    //return PyUnicode_DecodeUTF32 (value, cbValue / 4, NULL, NULL);
    break;

  case MCS_geostd8_general_ci://92,
  case MCS_geostd8_bin://93,
  case MCS_cp932_japanese_ci://95,
  case MCS_cp932_bin://96,
  case MCS_eucjpms_japanese_ci://97,
  case MCS_eucjpms_bin://98,
    break;

  case MCS_binary:
    return PyString_FromStringAndSize(value, cbValue);

  default:
    break;
  }

  return PyErr_Format (PyExc_ValueError, "Unsupported character set %d when decoding string", (int) ti->charset);
}


int API_resultRowValue(void *result, int column, UMTypeInfo *ti, char *value, size_t cbValue)
{
  PyObject *valobj = NULL;

  PRINTMARK();
  //fprintf (stderr, "%s: Got %p (%08x) %08x\n", __FUNCTION__, value, ti->type, cbValue);

  if (value == NULL)
  {
    valobj = Py_None;
    Py_IncRef(valobj);
  }
  else
  {

    switch (ti->type)
    {
      //PyNone:
    case MFTYPE_NULL:
      valobj = Py_None;
      Py_IncRef(valobj);
      break;

      //PyInt
    case MFTYPE_TINY:
    case MFTYPE_SHORT:
    case MFTYPE_LONG:
    case MFTYPE_INT24:
      {
        valobj = PyInt_FromLong(parseINT32 (value, ((char *) value) + cbValue));
        break;
      }

      //PyLong
    case MFTYPE_LONGLONG:
      {
        valobj = PyLong_FromLongLong(parseINT64 (value, ((char *) value) + cbValue));
        break;
      }

      //PyFloat
    case MFTYPE_FLOAT:
    case MFTYPE_DOUBLE:
      {
        //FIXME: Too fucking slow
        PyObject *sobj = PyString_FromStringAndSize((char *) value, cbValue);
        valobj = PyFloat_FromString (sobj, NULL);
        Py_DECREF(sobj);
        break;
      }

    case MFTYPE_DATE:
      {
        int year;
        int month;
        int day;

        year = parseINT32 (value, value + 4);

        if (year < 1)
        {
          valobj = Py_None;
          Py_IncRef(valobj);
          break;
        }

        value += 5;
        month = parseINT32 (value, value + 2);
        value += 3;
        day = parseINT32 (value, value + 2);
        value += 3;

        valobj = PyDate_FromDate (year, month, day);
        break;
      }

    case MFTYPE_DATETIME:
      {
        int year;
        int month;
        int day;
        int hour;
        int minute;
        int second;

        //9999-12-31 23:59:59
        char temp[20];
        memcpy (temp, value, cbValue);
        temp[cbValue] = '\0';

        year = parseINT32 (value, value + 4);
        value += 5;
        month = parseINT32 (value, value + 2);
        value += 3;
        day = parseINT32 (value, value + 2);
        value += 3;
        hour = parseINT32 (value, value + 2);
        value += 3;
        minute = parseINT32 (value, value + 2);
        value += 3;
        second = parseINT32 (value, value + 2);
        value += 3;

        if (year < 1)
        {
          valobj = Py_None;
          Py_IncRef(valobj);
          break;
        }


        valobj = PyDateTime_FromDateAndTime (year, month, day, hour, minute, second, 0);
        break;
      }

      // We ignore these

    case MFTYPE_TIMESTAMP:
    case MFTYPE_TIME:
    case MFTYPE_YEAR:
    case MFTYPE_NEWDATE:
      // Fall through for string encoding

      //Blob goes as String
    case MFTYPE_TINY_BLOB:
    case MFTYPE_MEDIUM_BLOB:
    case MFTYPE_LONG_BLOB:
    case MFTYPE_BLOB:
      // Fall through for string encoding
      valobj = PyString_FromStringAndSize( (const char *) value, cbValue);
      break;

      //PyString family
    case MFTYPE_VAR_STRING:
    case MFTYPE_VARCHAR:
    case MFTYPE_STRING:
      valobj = DecodeString (ti, value, cbValue);
      break;

    case MFTYPE_ENUM:
    case MFTYPE_GEOMETRY:
    case MFTYPE_BIT:
    case MFTYPE_NEWDECIMAL:
    case MFTYPE_SET:
    case MFTYPE_DECIMAL:
      // Fall through for string encoding
      valobj = PyString_FromStringAndSize( (const char *) value, cbValue);
      break;

    }
  }

  if (valobj == NULL)
  {
    if (PyErr_Occurred())
    {
      return FALSE;
    }

    PyErr_Format (umysql_Error, "Unable to convert field of type %d", ti->type);
    return FALSE;
  }

  PyTuple_SET_ITEM(((ResultSet *)result)->currRow, column, valobj);
  PRINTMARK();

  return TRUE;
}



UMConnectionCAPI capi = {
  API_getSocket,
  API_deleteSocket,
  API_closeSocket,
  API_connectSocket,
  API_setTimeout,
  API_clearException,
  API_recvSocket,
  API_sendSocket,
  API_createResult,
  API_resultSetField,
  API_resultRowBegin,
  API_resultRowValue,
  API_resultRowEnd,
  API_destroyResult,
  API_resultOK
};


int Connection_init(Connection *self, PyObject *arg)
{
  self->conn = UMConnection_Create (&capi);

  self->rxBufferSize = UMConnection_GetRxBufferSize (self->conn);
  self->txBufferSize = UMConnection_GetTxBufferSize (self->conn);
  self->Error = umysql_Error;
  self->SQLError = umysql_SQLError;

  self->PFN_PyUnicode_Encode = NULL;

  if (PyErr_Occurred())
  {
    PyErr_Format(PyExc_RuntimeError, "Exception is set for no error in %s", __FUNCTION__);
    return -1;
  }

  return 0;
}

PyObject *Connection_setTimeout(Connection *self, PyObject *args)
{
  int timeout;

  if (!PyArg_ParseTuple (args, "i", &timeout))
  {
    return NULL;
  }

  if (!UMConnection_SetTimeout(self->conn, timeout))
  {
    return HandleError(self, "setTimeout");
  }

  Py_RETURN_NONE;
}	


PyObject *Connection_isConnected(Connection *self, PyObject *args)
{
  if (UMConnection_IsConnected(self->conn))
  {
    Py_RETURN_TRUE;
  }

  Py_RETURN_FALSE;
}

PyObject *PyUnicode_EncodeCP1250Helper(const Py_UNICODE *data, Py_ssize_t length, const char *errors)
{
  return PyUnicode_Encode (data, length, "cp1250", errors);
}


PyObject *HandleError(Connection *self, const char *funcName)
{
  PyObject *value;
  const char *errorMessage;
  int errCode;
  int errType;

  if (UMConnection_GetLastError (self->conn, &errorMessage, &errCode, &errType))
  {
    if (PyErr_Occurred())
    {
      value = Py_BuildValue("(s,o,i,i,s)", "Python exception when local error is set", PyErr_Occurred(), errCode, errType, errorMessage);
      PyErr_Clear();
      PyErr_SetObject(umysql_Error, value);
      Py_DECREF(value);
      return NULL;
    }

    switch (errType)
    {
    case UME_OTHER:
      value = Py_BuildValue("(i,s)", errCode, errorMessage);
      PyErr_SetObject(umysql_Error, value);
      Py_DECREF(value);
      return NULL;

    case UME_MYSQL:
      value = Py_BuildValue("(i,s)", errCode, errorMessage);
      PyErr_SetObject(umysql_SQLError, value);
      Py_DECREF(value);
      return NULL;

    default:
      return PyErr_Format(PyExc_RuntimeError, "Unexpected error type (%d) in function %s\n", (int) errType, funcName);
    }

    value = Py_BuildValue("(s,s)", funcName, "Should not happen");
    PyErr_SetObject(PyExc_RuntimeError, value);
    Py_DECREF(value);
    return NULL;
  }

  if (PyErr_Occurred())
  {
    return NULL;
  }

  value = Py_BuildValue("(s, s)", funcName, "No error or Python error specified");
  PyErr_SetObject(PyExc_RuntimeError, value);
  Py_DECREF(value);
  return NULL;
}



PyObject *Connection_connect(Connection *self, PyObject *args)
{
  /*
  Args:
  UMConnection conn, const char *_host, int _port, const char *_username, const char *_password, const char *_database, int _autoCommit, const char *_charset*/

  char *host;
  int port;
  char *username;
  char *password;
  char *database;

  int autoCommit;
  char *pstrCharset = NULL;
  int charset = MCS_UNDEFINED;
  PyObject *acObj = NULL;

  if (!PyArg_ParseTuple (args, "sisss|Os", &host, &port, &username, &password, &database, &acObj, &pstrCharset))
  {
    return NULL;
  }

  if (acObj)
  {
    PRINTMARK();
    autoCommit = (PyObject_IsTrue(acObj) == 1) ? 1 : 0;
  }
  if (pstrCharset)
  {
    if (strcmp (pstrCharset, "utf8") == 0)
    {
      self->charset = MCS_utf8_general_ci;
      self->PFN_PyUnicode_Encode = PyUnicode_EncodeUTF8;
    }
    else
      if (strcmp (pstrCharset, "latin1") == 0)
      {
        self->charset = MCS_latin1_general_ci;
        self->PFN_PyUnicode_Encode = PyUnicode_EncodeLatin1;
      }
      else
        if (strcmp (pstrCharset, "ascii") == 0)
        {
          self->charset = MCS_ascii_general_ci;
          self->PFN_PyUnicode_Encode = PyUnicode_EncodeASCII;
        }
        else
          if (strcmp (pstrCharset, "cp1250") == 0)
          {
            self->charset = MCS_cp1250_general_ci;
            self->PFN_PyUnicode_Encode = PyUnicode_EncodeCP1250Helper;
          }
          else
          {
            return PyErr_Format (PyExc_ValueError, "Unsupported character set '%s' specified", pstrCharset);
          }
  }
  else
  {
    self->charset = MCS_utf8_general_ci;
    self->PFN_PyUnicode_Encode = PyUnicode_EncodeUTF8;
  }

  if (!UMConnection_Connect (self->conn, host, port, username, password, database, acObj ? &autoCommit : NULL, charset))
  {
    return NULL;
  }

  Py_RETURN_NONE;
}


int AppendAndEscapeString(char *buffStart, char *buffEnd, const char *strStart, const char *strEnd, int quote)
{
  //{'\0': '\\0', '\n': '\\n', '\r': '\\r', '\\': '\\\\', "'": "\\'", '"': '\\"', '\x1a': '\\Z'}):
  char *buffOffset = buffStart;

  if (quote)
  {
    (*buffOffset++) = '\'';
  }

  while (strStart < strEnd)
  {
    switch (*strStart)
    {
    case '\0':	// NULL
      PRINTMARK();
      (*buffOffset++) = '\\';
      (*buffOffset++) = '0';
      break;
    case '\n':	// LF
      PRINTMARK();
      (*buffOffset++) = '\\';
      (*buffOffset++) = 'n';
      break;
    case '\r':	// CR
      PRINTMARK();
      (*buffOffset++) = '\\';
      (*buffOffset++) = 'r';
      break;
    case '\\':	// BACKSLASH
      PRINTMARK();
      (*buffOffset++) = '\\';
      (*buffOffset++) = '\\';
      break;
    case '\'':	// SINGLE QUOTE
      PRINTMARK();
      (*buffOffset++) = '\\';
      (*buffOffset++) = '\'';
      break;
    case '\"':	// DOUBLE QUOTE
      PRINTMARK();
      (*buffOffset++) = '\\';
      (*buffOffset++) = '\"';
      break;
    case '\x1a': // SUBSTITUTE CONTROL CHARACTER
      PRINTMARK();
      (*buffOffset++) = '\\';
      (*buffOffset++) = 'Z';
      break;

    default:
      (*buffOffset++) = (*strStart);
      break;
    }

    strStart ++;
  }

  if (quote)
  {
    (*buffOffset++) = '\'';
  }

  return (int) (buffOffset - buffStart);
}

int AppendEscapedArg (Connection *self, char *start, char *end, PyObject *obj)
{
  int ret;
  PyObject *strobj;

  /*
  FIXME: Surround strings with '' could be performed in this function to avoid extra logic in AppendAndEscapeString */
  PRINTMARK();

  if (PyString_Check(obj))
  {
    PRINTMARK();
    return AppendAndEscapeString(start, end, PyString_AS_STRING(obj), PyString_AS_STRING(obj) + PyString_GET_SIZE(obj), TRUE);
  }
  else
    if (PyUnicode_Check(obj))
    {
      PRINTMARK();
      strobj = self->PFN_PyUnicode_Encode(PyUnicode_AS_UNICODE(obj), PyUnicode_GET_SIZE(obj), NULL);

      if (strobj == NULL)
      {
        if (PyErr_Occurred())
        {
          return -1;
        }

        PyErr_SetObject (PyExc_ValueError, obj);
        return -1;
      }


      ret = AppendAndEscapeString(start, end, PyString_AS_STRING(strobj), PyString_AS_STRING(strobj) + PyString_GET_SIZE(strobj), TRUE);
      Py_DECREF(strobj);

      return ret;
    }
    else
      if (obj == Py_None)
      {
        (*start++) = 'n';
        (*start++) = 'u';
        (*start++) = 'l';
        (*start++) = 'l';
        return  4;
      }
      else
        if (PyDateTime_Check(obj))
        {
          int len = sprintf (start, "'%04d-%02d-%02d %02d:%02d:%02d'", 
            PyDateTime_GET_YEAR(obj),
            PyDateTime_GET_MONTH(obj),
            PyDateTime_GET_DAY(obj),
            PyDateTime_DATE_GET_HOUR(obj),
            PyDateTime_DATE_GET_MINUTE(obj),
            PyDateTime_DATE_GET_SECOND(obj));

          return len;
        }
        else
          if (PyDate_Check(obj))
          {
            int len = sprintf (start, "'%04d:%02d:%02d'", 
              PyDateTime_GET_YEAR(obj),
              PyDateTime_GET_MONTH(obj),
              PyDateTime_GET_DAY(obj));

            return len;
          }

          //FIXME: Might possible to avoid this?
          PRINTMARK();
          strobj = PyObject_Str(obj);
          ret = AppendAndEscapeString(start, end, PyString_AS_STRING(strobj), PyString_AS_STRING(strobj) + PyString_GET_SIZE(strobj), FALSE);
          Py_DECREF(strobj);
          return ret;
}

PyObject *EscapeQueryArguments(Connection *self, PyObject *inQuery, PyObject *iterable)
{
  size_t cbOutQuery = 0;
  char *obuffer;
  char *optr;
  char *iptr;
  int heap = 0;
  int hasArg = 0;
  int appendLen;
  PyObject *retobj;
  PyObject *iterator;
  PyObject *arg;

  // Estimate output length

  cbOutQuery += PyString_GET_SIZE(inQuery);

  iterator = PyObject_GetIter(iterable);

  while ( (arg = PyIter_Next(iterator)))
  {
    // Quotes;
    cbOutQuery += 2;

    // Worst case escape and utf-8
    if (PyString_Check(arg))
      cbOutQuery += (PyString_GET_SIZE(arg) * 2);
    else
      if (PyUnicode_Check(arg))
        cbOutQuery += (PyUnicode_GET_SIZE(arg) * 6);
      else
        cbOutQuery += 64;

    Py_DECREF(arg);
  }

  Py_DECREF(iterator);

  if (cbOutQuery > (1024 * 64))
  {
    /*
    FIXME: Allocate a PyString and resize it just like the Python code does it */
    obuffer = (char *) PyObject_Malloc(cbOutQuery);
    heap = 1;
  }
  else
  {
    obuffer = (char *) alloca (cbOutQuery);
  }


  optr = obuffer;
  iptr = PyString_AS_STRING(inQuery);

  hasArg = 0;


  iterator = PyObject_GetIter(iterable);

  while (1)
  {
    switch (*iptr)
    {
    case '\0':
      goto END_PARSE;

    case '%':

      iptr ++;

      if (*iptr != 's')
      {
        Py_DECREF(iterator);
        if (heap) PyObject_Free(obuffer);
        return PyErr_Format (PyExc_ValueError, "Found character %c expected %%", *iptr);
      }

      iptr ++;

      arg = PyIter_Next(iterator);

      if (arg == NULL)
      {
        Py_DECREF(iterator);
        if (heap) PyObject_Free(obuffer);
        return PyErr_Format (PyExc_ValueError, "Unexpected end of iterator found");
      }

      appendLen = AppendEscapedArg(self, optr, obuffer + cbOutQuery, arg);
      Py_DECREF(arg);

      if (appendLen == -1)
      {
        Py_DECREF(iterator);
        if (heap) PyObject_Free(obuffer);
        return NULL;
      }

      optr += appendLen;

      break;

    default:
      *(optr++) = *(iptr)++;
      break;
    }
  }


END_PARSE:
  Py_DECREF(iterator);

  retobj = PyString_FromStringAndSize (obuffer, (optr - obuffer));

  if (heap)
  {
    PyObject_Free(obuffer);
  }

  return retobj;
}



PyObject *Connection_query(Connection *self, PyObject *args)
{
  void *ret;
  PyObject *inQuery = NULL;
  PyObject *query = NULL;
  PyObject *iterable = NULL;
  PyObject *escapedQuery = NULL;

  if (!UMConnection_IsConnected(self->conn))
  {
    return PyErr_Format(PyExc_RuntimeError, "Not connected");
  }

  if (!PyArg_ParseTuple (args, "O|O", &inQuery, &iterable))
  {
    return NULL;
  }

  if (iterable)
  {
    PyObject *iterator = PyObject_GetIter(iterable);

    if (iterator == NULL)
    {
      PyErr_Clear();
      return PyErr_Format(PyExc_TypeError, "Expected iterable");
    }

    Py_DECREF(iterator);
  }

  if (!PyString_Check(inQuery))
  {
    if (!PyUnicode_Check(inQuery))
    {
      PRINTMARK();
      return PyErr_Format(PyExc_TypeError, "Query argument must be either String or Unicode");
    }

    query = self->PFN_PyUnicode_Encode(PyUnicode_AS_UNICODE(inQuery), PyUnicode_GET_SIZE(inQuery), NULL);

    if (query == NULL)
    {
      if (!PyErr_Occurred())
      {
        PyErr_SetObject(PyExc_ValueError, query);
        return NULL;
      }

      return NULL;
    }
  }
  else
  {
    query = inQuery;
    Py_INCREF(query);
  }

  if (iterable)
  {
    PRINTMARK();
    escapedQuery = EscapeQueryArguments(self, query, iterable);
    Py_DECREF(query);

    if (escapedQuery == NULL)
    {
      if (!PyErr_Occurred())
      {
        return PyErr_Format(PyExc_RuntimeError, "Exception not set in EscapeQueryArguments chain");
      }

      return NULL;
    }

  }
  else
  {
    escapedQuery = query;
  }

  ret =  UMConnection_Query(self->conn, PyString_AS_STRING(escapedQuery), PyString_GET_SIZE(escapedQuery));

  Py_DECREF(escapedQuery);

  PRINTMARK();
  if (ret == NULL)
  {
    return HandleError(self, "query");
  }

  PRINTMARK();
  return (PyObject *) ret;
}




PyObject *Connection_close(Connection *self, PyObject *notused)
{
  if (!UMConnection_Close(self->conn))
  {
    return HandleError(self, "close");
  }
  Py_RETURN_NONE;
}


static void Connection_Destructor(Connection *self)
{
  PRINTMARK();
  UMConnection_Destroy(self->conn);
  PRINTMARK();
  PyObject_Del(self);
  PRINTMARK();
  if (PyErr_Occurred()) PyErr_Format(PyExc_RuntimeError, "Exception is set for no error in %s", __FUNCTION__);
  PRINTMARK();
}

static PyMethodDef Connection_methods[] = {
  {"connect", (PyCFunction)			Connection_connect,			METH_VARARGS, "Connects to database server. Arguments: host, port, username, password, database, autocommit, charset"},
  {"query", (PyCFunction)				Connection_query,				METH_VARARGS, "Performs a query. Arguments: query, arguments to escape"},
  {"close", (PyCFunction)	Connection_close,	METH_NOARGS, "Closes connection"},
  {"is_connected", (PyCFunction) Connection_isConnected, METH_NOARGS, "Check connection status"},
  {"settimeout", (PyCFunction) Connection_setTimeout, METH_VARARGS, "Sets connection timeout in seconds"},
  {NULL}
};
static PyMemberDef Connection_members[] = {

  {"Error", T_OBJECT, offsetof(Connection, Error), READONLY},
  {"SQLError", T_OBJECT, offsetof(Connection, SQLError), READONLY},
  {"txBufferSize", T_INT, offsetof(Connection, txBufferSize), READONLY, "Size of tx buffer in bytes"},
  {"rxBufferSize", T_INT, offsetof(Connection, rxBufferSize), READONLY, "Size of rx buffer in bytes"},
  {NULL}
};



static PyTypeObject ConnectionType = { 
  PyObject_HEAD_INIT(NULL)
  0,				/* ob_size        */
  "umysql.Connection",		/* tp_name        */
  sizeof(Connection),		/* tp_basicsize   */
  0,				/* tp_itemsize    */
  Connection_Destructor,		/* tp_dealloc     */
  0,				/* tp_print       */
  0,				/* tp_getattr     */
  0,				/* tp_setattr     */
  0,				/* tp_compare     */
  0,				/* tp_repr        */
  0,				/* tp_as_number   */
  0,				/* tp_as_sequence */
  0,				/* tp_as_mapping  */
  0,				/* tp_hash        */
  0,				/* tp_call        */
  0,				/* tp_str         */
  0,				/* tp_getattro    */
  0,				/* tp_setattro    */
  0,				/* tp_as_buffer   */
  Py_TPFLAGS_DEFAULT,		/* tp_flags       */
  "",	/* tp_doc         */
  0,				/* tp_traverse       */
  0,				/* tp_clear          */
  0,				/* tp_richcompare    */
  0,				/* tp_weaklistoffset */
  0,				/* tp_iter           */
  0,				/* tp_iternext       */
  Connection_methods,	     		/* tp_methods        */
  Connection_members,			/* tp_members        */
  0,				/* tp_getset         */
  0,				/* tp_base           */
  0,				/* tp_dict           */
  0,				/* tp_descr_get      */
  0,				/* tp_descr_set      */
  0,				/* tp_dictoffset     */
  (initproc)Connection_init,		/* tp_init           */
};

int ResultSet_setup(ResultSet *self, int columns)
{
  self->currRow = NULL;
  self->fields = PyTuple_New(columns);
  self->rows = PyList_New(0);
  self->numFields = columns;

  PRINTMARK();
  return 0;
}


int ResultSet_init(ResultSet *self, PyObject *args)
{
  int numFields;
  PRINTMARK();

  if (!PyArg_ParseTuple(args, "i", &numFields))
  {
    PRINTMARK();
    return -1;
  }

  return ResultSet_setup(self, numFields);
}

void ResultSet_Destructor(ResultSet *self)
{
  PRINTMARK();
  Py_XDECREF(self->currRow);
  PRINTMARK();
  Py_XDECREF(self->fields);
  PRINTMARK();
  Py_XDECREF(self->rows);
  PRINTMARK();

  PyObject_Del(self);
  PRINTMARK();
}

static PyMethodDef ResultSet_methods[] = {
  //{"connect", (PyCFunction)			Connection_connect,			METH_VARARGS, ""},
  //{"query", (PyCFunction)				Connection_query,				METH_O, ""},
  //{"disconnect", (PyCFunction)	Connection_disconnect,	METH_NOARGS, ""},
  {NULL}
};

static PyMemberDef ResultSet_members[] = {
  {"rows", T_OBJECT_EX, offsetof(ResultSet, rows), 0, "Rows in result"},
  {"fields", T_OBJECT_EX, offsetof(ResultSet, fields), 0, "Fields in result"},
  //{"connect", (PyCFunction)			Connection_connect,			METH_VARARGS, ""},
  //{"query", (PyCFunction)				Connection_query,				METH_O, ""},
  //{"disconnect", (PyCFunction)	Connection_disconnect,	METH_NOARGS, ""},
  {NULL}
};

static PyTypeObject ResultSetType = { 
  PyObject_HEAD_INIT(NULL)
  0,				/* ob_size        */
  "umysql.ResultSet",		/* tp_name        */
  sizeof(ResultSet),		/* tp_basicsize   */
  0,				/* tp_itemsize    */
  ResultSet_Destructor,		/* tp_dealloc     */
  0,				/* tp_print       */
  0,				/* tp_getattr     */
  0,				/* tp_setattr     */
  0,				/* tp_compare     */
  0,				/* tp_repr        */
  0,				/* tp_as_number   */
  0,				/* tp_as_sequence */
  0,				/* tp_as_mapping  */
  0,				/* tp_hash        */
  0,				/* tp_call        */
  0,				/* tp_str         */
  0,				/* tp_getattro    */
  0,				/* tp_setattro    */
  0,				/* tp_as_buffer   */
  Py_TPFLAGS_DEFAULT,		/* tp_flags       */
  "",	/* tp_doc         */
  0,				/* tp_traverse       */
  0,				/* tp_clear          */
  0,				/* tp_richcompare    */
  0,				/* tp_weaklistoffset */
  0,				/* tp_iter           */
  0,				/* tp_iternext       */
  ResultSet_methods, //ResultSet_methods,	     		/* tp_methods        */
  ResultSet_members,			/* tp_members        */
  0,				/* tp_getset         */
  0,				/* tp_base           */
  0,				/* tp_dict           */
  0,				/* tp_descr_get      */
  0,				/* tp_descr_set      */
  0,				/* tp_dictoffset     */
  (initproc)ResultSet_init,		/* tp_init           */
};

static PyMethodDef methods[] = {
  {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
  initumysql(void) 
{
  PyObject* m;
  PyObject *dict;
  PyDateTime_IMPORT;

  m = Py_InitModule3("umysql", methods, "");
  if (m == NULL)
    return;

  dict = PyModule_GetDict(m);

  ConnectionType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&ConnectionType) < 0)
    return;
  Py_INCREF(&ConnectionType);
  PyModule_AddObject(m, "Connection", (PyObject *)&ConnectionType);

  ResultSetType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&ResultSetType) < 0)
    return;
  Py_INCREF(&ResultSetType);
  PyModule_AddObject(m, "ResultSet", (PyObject *)&ResultSetType);

  umysql_Error = PyErr_NewException("umysql.Error", PyExc_StandardError, NULL);
  umysql_SQLError = PyErr_NewException("umysql.SQLError", umysql_Error, NULL);

  PyDict_SetItemString(dict, "Error", umysql_Error);
  PyDict_SetItemString(dict, "SQLError", umysql_SQLError);
}
