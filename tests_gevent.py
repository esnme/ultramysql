# -*- coding: utf-8 -*-
"""
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
"""

from __future__ import with_statement

import time
import datetime
import logging
import unittest
import gevent
import gevent.socket
import amysql

DB_HOST = '127.0.0.1'
DB_PORT = 3306
DB_USER = 'gevent_test'
DB_PASSWD = 'gevent_test'
DB_DB = 'gevent_test'

class TestMySQL(unittest.TestCase):
    log = logging.getLogger('TestMySQL')
    
    def testUnique(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblunique")
        cnn.query("create table tblunique (name VARCHAR(32) UNIQUE)")

        # We insert the same character using two different encodings
        cnn.query("insert into tblunique set name=\"kaka\"")

        count = 0
        
        for x in xrange(0, 10):
            try:
                cnn.query("insert into tblunique set name=\"kaka\"")
            except(RuntimeError):
                count = count + 1
            
        self.assertEquals(count, 10)

    def testMySQLTimeout(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        rs = cnn.query("select sleep(2)")
        list(rs.rows)

        from gevent import Timeout

        start = time.time()
        try:
            def delay():
                cnn.query("select sleep(4)")
                self.fail('expected timeout')
            gevent.with_timeout(2, delay)
        except Timeout:
            end = time.time()
            self.assertAlmostEqual(2.0, end - start, places = 1)

        cnn.close()

    def testDoubleConnect(self):
        cnn = amysql.Connection()
        cnn.connect(DB_HOST, DB_PORT, DB_USER, DB_PASSWD, DB_DB)
        time.sleep(11)
        cnn.close()
        time.sleep(1)
        cnn = amysql.Connection()
        cnn.connect(DB_HOST, DB_PORT, DB_USER, DB_PASSWD, DB_DB)

    def testConnectTimeout(self):
        cnn = amysql.Connection()
        cnn.settimeout(1)
        
        start = time.clock()
        try:
            cnn.connect (DB_HOST, 31481, DB_USER, DB_PASSWD, DB_DB)

        except(gevent.socket.error):
            elapsed = time.clock() - start

            if (elapsed > 2):
                assert False, "Timeout isn't working"
            return
        
        assert False, "Expected expection"
        
    
    
    def testConnectFails(self):
        cnn = amysql.Connection()

        try:
            cnn.connect (DB_HOST, 31337, DB_USER, DB_PASSWD, DB_DB)
            assert False, "Expected exception"
        except(gevent.socket.error):
            pass
        pass

    def testConnectDNSFails(self):
        cnn = amysql.Connection()

        try:
            cnn.connect ("thisplaceisnowere", 31337, DB_USER, DB_PASSWD, DB_DB)
            assert False, "Expected exception"
        except(gevent.socket.error):
            pass
        pass
        
    def testConcurrencyQueryError(self):
        connection = amysql.Connection()
        connection.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        errorCount = [ 0 ]

        def query(cnn):
            try:
                cnn.query("select sleep(5)")
            except(RuntimeError):
                errorCount[0] = errorCount[0] + 1
                return

        ch1 = gevent.spawn(query, connection)
        ch2 = gevent.spawn(query, connection)
        ch3 = gevent.spawn(query, connection)
        gevent.joinall([ch1, ch2, ch3])
        
        self.assertTrue(errorCount[0] > 0)

    def testConcurrencyConnectError(self):
        connection = amysql.Connection()
        errorCount = [ 0 ]

        def query(cnn):
            try:
                cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
            except(RuntimeError):
                errorCount[0] = errorCount[0] + 1
                return

        ch1 = gevent.spawn(query, connection)
        ch2 = gevent.spawn(query, connection)
        ch3 = gevent.spawn(query, connection)
        gevent.joinall([ch1, ch2, ch3])
        
        self.assertTrue(errorCount[0] > 0)

        
    def testParallelQuery(self):

        def query(s):
            cnn = amysql.Connection()
            cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
            cnn.query("select sleep(%d)" % s)
            cnn.close()

        start = time.time()
        ch1 = gevent.spawn(query, 1)
        ch2 = gevent.spawn(query, 2)
        ch3 = gevent.spawn(query, 3)
        gevent.joinall([ch1, ch2, ch3])

        end = time.time()
        self.assertAlmostEqual(3.0, end - start, places = 0)


 
        
    def testConnectTwice(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        try:
            cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
            assert False, "Expected exception"
        except(RuntimeError):
            pass
        pass


        
    def testConnectClosed(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        self.assertEquals(True, cnn.is_connected())
        cnn.close()
        self.assertEquals(False, cnn.is_connected())

    def testConnectCloseQuery(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        self.assertEquals(True, cnn.is_connected())
        cnn.close()
        self.assertEquals(False, cnn.is_connected())
        
        try:
            cnn.query("SELECT 1")
            assert False, "Expected exception"
        except(RuntimeError):
            pass
        
    def testMySQLClient(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        rs = cnn.query("select 1")
        self.assertEqual([(1,)], rs.rows)
        cnn.close()

    def testConnectNoDb(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, "")

        rs = cnn.query("select 1")

        self.assertEqual([(1,)], rs.rows)
        cnn.close()
    
    def testConnectAutoCommitOff(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, "", False)

        rs = cnn.query("select 1")

        self.assertEqual([(1,)], rs.rows)
        cnn.close()

    def testMySQLClient2(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("truncate tbltest")

        for i in range(10):
            self.assertEquals((1, 0), cnn.query("insert into tbltest (test_id, test_string) values (%d, 'test%d')" % (i, i)))

        rs = cnn.query("select test_id, test_string from tbltest")

        #trying to close it now would give an error, e.g. we always need to read
        #the result from the database otherwise connection would be in wrong stat

        for i, row in enumerate(rs.rows):
            self.assertEquals((i, 'test%d' % i), row)

        cnn.close()

    def testMySQLDBAPI(self):

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("truncate tbltest")

        for i in range(10):
            cnn.query("insert into tbltest (test_id, test_string) values (%d, 'test%d')" % (i, i))

        rs = cnn.query("select test_id, test_string from tbltest")

        self.assertEquals((0, 'test0'), rs.rows[0])

        #check that fetchall gets the remainder
        self.assertEquals([(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5'), (6, 'test6'), (7, 'test7'), (8, 'test8'), (9, 'test9')], rs.rows[1:])

        #another query on the same cursor should work
        rs = cnn.query("select test_id, test_string from tbltest")

        #fetch some but not all
        self.assertEquals((0, 'test0'), rs.rows[0])
        self.assertEquals((1, 'test1'), rs.rows[1])
        self.assertEquals((2, 'test2'), rs.rows[2])

        #this should not work, cursor was closed

    def testLargePackets(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        cnn.query("truncate tbltest")

        c = 16 * 1024

        blob = '0123456789'
        while 1:
            cnn.query("insert into tbltest (test_id, test_blob) values (%d, '%s')" % (len(blob), blob))
            if len(blob) > (c * 2): break
            blob = blob * 2

        rs = cnn.query("select test_id, test_blob from tbltest")
        for row in rs.rows:
            self.assertEquals(row[0], len(row[1]))
            #self.assertEquals(blob[:row[0]], row[1])

        #reread, second time, oversize packet is already present
        rs = cnn.query("select test_id, test_blob from tbltest")
        for row in rs.rows:
            self.assertEquals(row[0], len(row[1]))
            self.assertEquals(blob[:row[0]], row[1])
                        
        cnn.close()

        #have a very low max packet size for oversize packets
        #and check that exception is thrown when trying to read larger packets

    def testEscapeArgs(self):
        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("truncate tbltest")
  
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (1, 'piet'))
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (2, 'klaas'))
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (3, "pi'et"))

        #classic sql injection, would return all rows if no proper escaping is done
        rs = cnn.query("select test_id, test_string from tbltest where test_string = %s", ("piet' OR 'a' = 'a",))
        self.assertEquals([], rs.rows) #assert no rows are found

        #but we should still be able to find the piet with the apostrophe in its name
        rs = cnn.query("select test_id, test_string from tbltest where test_string = %s", ("pi'et",))
        self.assertEquals([(3, "pi'et")], rs.rows)
  
        #also we should be able to insert and retrieve blob/string with all possible bytes transparently
        chars = ''.join([chr(i) for i in range(256)])

    
        cnn.query("insert into tbltest (test_id, test_string, test_blob) values (%s, %s, %s)", (4, chars[:80], chars))
        #cnn.query("insert into tbltest (test_id, test_blob) values (%s, %s)", (4, chars))

        rs = cnn.query("select test_blob, test_string from tbltest where test_id = %s", (4,))
        #self.assertEquals([(chars, chars)], cur.fetchall())
        b, s = rs.rows[0]
 
        #test blob
        self.assertEquals(256, len(b))
        self.assertEquals(chars, b)

        self.assertEquals(80, len(s))
        self.assertEquals(chars[:80], s)        
        
        cnn.close()

    def testSelectUnicode(self):
        s = u'r\xc3\xa4ksm\xc3\xb6rg\xc3\xa5s'

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("truncate tbltest")
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (1, 'piet'))
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (2, s))
        cnn.query(u"insert into tbltest (test_id, test_string) values (%s, %s)", (3, s))

        rs = cnn.query("select test_id, test_string from tbltest")

        result = rs.rows
        self.assertEquals([(1, u'piet'), (2, s), (3, s)], result)

        #test that we can still cleanly roundtrip a blob, (it should not be encoded if we pass
        #it as 'str' argument), eventhough we pass the qry itself as unicode
        blob = ''.join([chr(i) for i in range(256)])

        cnn.query(u"insert into tbltest (test_id, test_blob) values (%s, %s)", (4, blob))
        rs = cnn.query("select test_blob from tbltest where test_id = %s", (4,))
        b2 = rs.rows[0][0]
        self.assertEquals(str, type(b2))
        self.assertEquals(256, len(b2))
        self.assertEquals(blob, b2)
    
    def testAutoInc(self):

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("truncate tblautoincint")

        cnn.query("ALTER TABLE tblautoincint AUTO_INCREMENT = 100")
        rowcount, lastrowid = cnn.query("insert into tblautoincint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, rowcount)
        self.assertEqual(100, lastrowid)
        rowcount, lastrowid = cnn.query("insert into tblautoincint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, rowcount)
        self.assertEqual(101, lastrowid)

        cnn.query("ALTER TABLE tblautoincint AUTO_INCREMENT = 4294967294")
        rowcount, lastrowid = cnn.query("insert into tblautoincint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, rowcount)
        self.assertEqual(4294967294, lastrowid)
        rowcount, lastrowid = cnn.query("insert into tblautoincint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, rowcount)
        self.assertEqual(4294967295, lastrowid)

        cnn.query("truncate tblautoincbigint")

        cnn.query("ALTER TABLE tblautoincbigint AUTO_INCREMENT = 100")
        rowcount, lastrowid = cnn.query("insert into tblautoincbigint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, rowcount)
        self.assertEqual(100, lastrowid)
        rowcount, lastrowid = cnn.query("insert into tblautoincbigint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, rowcount)
        self.assertEqual(101, lastrowid)

        cnn.query("ALTER TABLE tblautoincbigint AUTO_INCREMENT = 18446744073709551614")
        rowcount, lastrowid = cnn.query("insert into tblautoincbigint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, rowcount)
        self.assertEqual(18446744073709551614, lastrowid)
        #this fails on mysql, but that is a mysql problem
        #cur.execute("insert into tblautoincbigint (test_string) values (%s)", ('piet',))
        #self.assertEqual(1, cur.rowcount)
        #self.assertEqual(18446744073709551615, cur.lastrowid)

        cnn.close()

    def testBigInt(self):
        #Tests the behaviour of insert/select with bigint/long.

        BIGNUM = 112233445566778899

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblbigint")
        cnn.query("create table tblbigint (test_id int(11) DEFAULT NULL, test_bigint bigint DEFAULT NULL, test_bigint2 bigint DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")
        cnn.query("insert into tblbigint (test_id, test_bigint, test_bigint2) values (%s, " + str(BIGNUM) + ", %s)", (1, BIGNUM))
        cnn.query(u"insert into tblbigint (test_id, test_bigint, test_bigint2) values (%s, " + str(BIGNUM) + ", %s)", (2, BIGNUM))

        # Make sure both our inserts where correct (ie, the big number was not truncated/modified on insert)
        rs = cnn.query("select test_id from tblbigint where test_bigint = test_bigint2")
        result = rs.rows
        self.assertEquals([(1, ), (2, )], result)

        # Make sure select gets the right values (ie, the big number was not truncated/modified when retrieved)
        rs = cnn.query("select test_id, test_bigint, test_bigint2 from tblbigint where test_bigint = test_bigint2")
        result = rs.rows
        self.assertEquals([(1, BIGNUM, BIGNUM), (2, BIGNUM, BIGNUM)], result)

    def testDate(self):
        # Tests the behaviour of insert/select with mysql/DATE <-> python/datetime.date

        d_date = datetime.date(2010, 02, 11)
        d_string = "2010-02-11"

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tbldate")
        cnn.query("create table tbldate (test_id int(11) DEFAULT NULL, test_date date DEFAULT NULL, test_date2 date DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")

        cnn.query("insert into tbldate (test_id, test_date, test_date2) values (%s, '" + d_string + "', %s)", (1, d_date))

        # Make sure our insert was correct
        rs = cnn.query("select test_id from tbldate where test_date = test_date2")
        result = rs.rows
        self.assertEquals([(1, )], result)

        # Make sure select gets the right value back
        rs = cnn.query("select test_id, test_date, test_date2 from tbldate where test_date = test_date2")
        result = rs.rows
        self.assertEquals([(1, d_date, d_date)], result)

    def testDateTime(self):
        # Tests the behaviour of insert/select with mysql/DATETIME <-> python/datetime.datetime

        d_date = datetime.datetime(2010, 02, 11, 13, 37, 42)
        d_string = "2010-02-11 13:37:42"

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)


        cnn.query("drop table if exists tbldate")
        cnn.query("create table tbldate (test_id int(11) DEFAULT NULL, test_date datetime DEFAULT NULL, test_date2 datetime DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")

        cnn.query("insert into tbldate (test_id, test_date, test_date2) values (%s, '" + d_string + "', %s)", (1, d_date))

        # Make sure our insert was correct
        rs = cnn.query("select test_id from tbldate where test_date = test_date2")
        result = rs.rows
        self.assertEquals([(1, )], result)

        # Make sure select gets the right value back
        rs = cnn.query("select test_id, test_date, test_date2 from tbldate where test_date = test_date2")
        result = rs.rows
        self.assertEquals([(1, d_date, d_date)], result)

    def testZeroDates(self):
        # Tests the behaviour of zero dates
        zero_datetime = "0000-00-00 00:00:00" 
        zero_date = "0000-00-00"

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tbldate")
        cnn.query("create table tbldate (test_id int(11) DEFAULT NULL, test_date date DEFAULT NULL, test_datetime datetime DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")

        cnn.query("insert into tbldate (test_id, test_date, test_datetime) values (%s, %s, %s)", (1, zero_date, zero_datetime))

        # Make sure we get None-values back
        rs = cnn.query("select test_id, test_date, test_datetime from tbldate where test_id = 1")
        result = rs.rows
        self.assertEquals([(1, None, None)], result)

    def testUnicodeUTF8(self):
        peacesign_unicode = u"\u262e"
        peacesign_utf8 = "\xe2\x98\xae"

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblutf")
        cnn.query("create table tblutf (test_id int(11) DEFAULT NULL, test_string VARCHAR(32) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8")

        cnn.query("insert into tblutf (test_id, test_string) values (%s, %s)", (1, peacesign_unicode)) # This should be encoded in utf8
        cnn.query("insert into tblutf (test_id, test_string) values (%s, %s)", (2, peacesign_utf8))

        rs = cnn.query("select test_id, test_string from tblutf")
        result = rs.rows

        # We expect unicode strings back
        self.assertEquals([(1, peacesign_unicode), (2, peacesign_unicode)], result)
  
    def testBinary(self):
        peacesign_binary = "\xe2\x98\xae"
        peacesign_binary2 = "\xe2\x98\xae" * 10

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblbin")
        cnn.query("create table tblbin (test_id int(11) DEFAULT NULL, test_binary VARBINARY(30) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8")

        cnn.query("insert into tblbin (test_id, test_binary) values (%s, %s)", (1, peacesign_binary))
        cnn.query("insert into tblbin (test_id, test_binary) values (%s, %s)", (2, peacesign_binary2))

        rs = cnn.query("select test_id, test_binary from tblbin")
        result = rs.rows

        # We expect binary strings back
        self.assertEquals([(1, peacesign_binary),(2, peacesign_binary2)], result)

    
    def testBlob(self):
        peacesign_binary = "\xe2\x98\xae"
        peacesign_binary2 = "\xe2\x98\xae" * 1024

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblblob")
        cnn.query("create table tblblob (test_id int(11) DEFAULT NULL, test_blob BLOB DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8")

        cnn.query("insert into tblblob (test_id, test_blob) values (%s, %s)", (1, peacesign_binary))
        cnn.query("insert into tblblob (test_id, test_blob) values (%s, %s)", (2, peacesign_binary2))

        rs = cnn.query("select test_id, test_blob from tblblob")
        result = rs.rows

        # We expect binary strings back
        self.assertEquals([(1, peacesign_binary),(2, peacesign_binary2)], result)    

    def testCharsets(self):
        aumlaut_unicode = u"\u00e4"
        aumlaut_utf8 = "\xc3\xa4"
        aumlaut_latin1 = "\xe4"

        cnn = amysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblutf")
        cnn.query("create table tblutf (test_mode VARCHAR(32) DEFAULT NULL, test_utf VARCHAR(32) DEFAULT NULL, test_latin1 VARCHAR(32)) ENGINE=MyISAM DEFAULT CHARSET=utf8")

        # We insert the same character using two different encodings
        cnn.query("set names utf8")
        cnn.query("insert into tblutf (test_mode, test_utf, test_latin1) values ('utf8', _utf8'" + aumlaut_utf8 + "', _latin1'" + aumlaut_latin1 + "')")
        
        cnn.query("set names latin1")
        cnn.query("insert into tblutf (test_mode, test_utf, test_latin1) values ('latin1', _utf8'" + aumlaut_utf8 + "', _latin1'" + aumlaut_latin1 + "')")

        # We expect the driver to always give us unicode strings back
        expected = [(u"utf8", aumlaut_unicode, aumlaut_unicode), (u"latin1", aumlaut_unicode, aumlaut_unicode)]

        # Fetch and test with different charsets
        for charset in ("latin1", "utf8", "cp1250"):
            cnn.query("set names " + charset)
            rs = cnn.query("select test_mode, test_utf, test_latin1 from tblutf")
            result = rs.rows
            self.assertEquals(result, expected)

if __name__ == '__main__':
    unittest.main()
            
"""            
if __name__ == '__main__':
    from guppy import hpy
    hp = hpy()
    hp.setrelheap()
    while True:
        unittest.main()
        heap = hp.heapu()
        print heap
"""        
