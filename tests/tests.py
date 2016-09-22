# -*- coding: utf-8 -*-
"""
Copyright (c) 2016, Arpit Bhayani
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
3. The name of Arpit Bhayani may not be used to endorse or promote products
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY ARPIT BHAYANI "AS IS" AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
import socket

import umysql3 as umysql

DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWD = ''
DB_DB = 'test'

class TestMySQL(unittest.TestCase):
    log = logging.getLogger('TestMySQL')

    def testCorruption(self):
        try:
            a.b = umysql.Connection()
        except NameError:
            pass

    def testConnectWithNoDB(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, "")

    def testConnectWithWrongDB(self):
        cnn = umysql.Connection()
        try:
            cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, "DBNOTFOUND")
        except umysql.SQLError as e:
            # 1049 = ER_BAD_DB_ERROR
            self.assertEqual(e.args[0], 1049)

    def testConnectWrongCredentials(self):
        cnn = umysql.Connection()
        try:
            cnn.connect (DB_HOST, 3306, "UserNotFound", "PasswordYeah", DB_DB)
        except umysql.SQLError as e:
            self.assertEqual(e.args[0], 1045)

    def testUnique(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblunique")
        cnn.query("create table tblunique (name VARCHAR(32) UNIQUE)")

        # We insert the same character using two different encodings
        cnn.query("insert into tblunique set name=\"kaka\"")

        count = 0

        try:
            cnn.query("insert into tblunique set name=\"kaka\"")
            self.fail('expected timeout')
        except umysql.SQLError as e:
            pass
        cnn.query("select * from tblunique")
        cnn.close()

    def testDoubleConnect(self):
        cnn = umysql.Connection()
        cnn.connect(DB_HOST, DB_PORT, DB_USER, DB_PASSWD, DB_DB)
        time.sleep(11)
        cnn.close()
        time.sleep(1)
        cnn = umysql.Connection()
        cnn.connect(DB_HOST, DB_PORT, DB_USER, DB_PASSWD, DB_DB)

    def testConnectTimeout(self):
        cnn = umysql.Connection()
        cnn.settimeout(1)

        start = time.clock()
        try:
            cnn.connect (DB_HOST, 31481, DB_USER, DB_PASSWD, DB_DB)
            assert False, "Expected exception"
        except(socket.error):
            elapsed = time.clock() - start

            if (elapsed > 2):
                assert False, "Timeout isn't working"

        try:
            res = cnn.query("select * from kaka");
            assert False, "Expected exception"
        except(socket.error):
            pass
        cnn.close()

    def testConnectFails(self):
        cnn = umysql.Connection()

        try:
            cnn.connect (DB_HOST, 31337, DB_USER, DB_PASSWD, DB_DB)
            assert False, "Expected exception"
        except(socket.error):
            pass
        cnn.close()

    def testConnectDNSFails(self):
        cnn = umysql.Connection()

        try:
            cnn.connect ("thisplaceisnowere", 31337, DB_USER, DB_PASSWD, DB_DB)
            assert False, "Expected exception"
        except(socket.error):
            pass
        cnn.close()

    # def testConcurrentQueryError(self):
    #     connection = umysql.Connection()
    #     connection.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
    #     errorCount = [ 0 ]
    #
    #     def query(cnn):
    #         try:
    #             cnn.query("select sleep(3)")
    #         except(umysql.Error):
    #             errorCount[0] = errorCount[0] + 1
    #             return
    #
    #     ch1 = gevent.spawn(query, connection)
    #     ch2 = gevent.spawn(query, connection)
    #     ch3 = gevent.spawn(query, connection)
    #     gevent.joinall([ch1, ch2, ch3])
    #
    #     self.assertTrue(errorCount[0] > 0)
    #     connection.close()

    # def testConcurrentConnectError(self):
    #     connection = umysql.Connection()
    #     errorCount = [ 0 ]
    #
    #     def query(cnn):
    #         try:
    #             cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
    #         except(umysql.Error):
    #             errorCount[0] = errorCount[0] + 1
    #             return
    #
    #     ch1 = gevent.spawn(query, connection)
    #     ch2 = gevent.spawn(query, connection)
    #     ch3 = gevent.spawn(query, connection)
    #     gevent.joinall([ch1, ch2, ch3])
    #
    #     self.assertTrue(errorCount[0] > 0)
    #     connection.close()

    # def testMySQLTimeout(self):
    #     cnn = umysql.Connection()
    #     cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
    #
    #     rs = cnn.query("select sleep(2)")
    #     list(rs.rows)
    #
    #     from gevent import Timeout
    #
    #     start = time.time()
    #     try:
    #         def delay():
    #             cnn.query("select sleep(4)")
    #             self.fail('expected timeout')
    #         gevent.with_timeout(2, delay)
    #     except Timeout:
    #         end = time.time()
    #         self.assertAlmostEqual(2.0, end - start, places = 1)
    #
    #     cnn.close()

    # def testParallelQuery(self):
    #
    #     def query(s):
    #         cnn = umysql.Connection()
    #         cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
    #         cnn.query("select sleep(%d)" % s)
    #         cnn.close()
    #
    #     start = time.time()
    #     ch1 = gevent.spawn(query, 1)
    #     ch2 = gevent.spawn(query, 2)
    #     ch3 = gevent.spawn(query, 3)
    #     gevent.joinall([ch1, ch2, ch3])
    #
    #     end = time.time()
    #     self.assertAlmostEqual(3.0, end - start, places = 0)

    def testConnectTwice(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        try:
            cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
            assert False, "Expected exception"
        except:
            pass
        pass
        cnn.close()

    def testConnectClosed(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        self.assertEqual(True, cnn.is_connected())
        cnn.close()
        self.assertEqual(False, cnn.is_connected())

    def testConnectCloseQuery(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        self.assertEqual(True, cnn.is_connected())
        cnn.close()
        self.assertEqual(False, cnn.is_connected())

        try:
            cnn.query("SELECT 1")
            assert False, "Expected exception"
        except(RuntimeError):
            pass
        cnn.close()


    def testMySQLClient(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        rs = cnn.query("select 1")
        self.assertEqual([(1,)], rs.rows)
        cnn.close()

    def testConnectNoDb(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, "")

        rs = cnn.query("select 1")

        self.assertEqual([(1,)], rs.rows)
        cnn.close()

    def testConnectAutoCommitOff(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, "", False)

        rs = cnn.query("select 1")

        self.assertEqual([(1,)], rs.rows)
        cnn.close()

    def testMySQLClient2(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("truncate tbltest")

        for i in range(10):
            self.assertEqual((1, 0), cnn.query("insert into tbltest (test_id, test_string) values (%s, 'test%s')", (i, i,)))

        rs = cnn.query("select test_id, test_string from tbltest")

        #trying to close it now would give an error, e.g. we always need to read
        #the result from the database otherwise connection would be in wrong stat

        for i, row in enumerate(rs.rows):
            self.assertEqual((i, 'test%s' % i), row)

        cnn.close()


    # Working and too long
    # def testMySQLClientManyInserts(self):
    #     cnn = umysql.Connection()
    #     cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
    #
    #     cnn.query("DROP TABLE IF EXISTS tbltestmanyinserts")
    #     cnn.query("CREATE TABLE tbltestmanyinserts (i int, j int, f double)")
    #
    #     cnti = cntj = 0
    #     print(datetime.datetime.now(),'\tstarting test')
    #     try:
    #         for i in range(200):
    #             cnti += 1
    #             cntj = 0
    #             for j in range(10000):
    #                 cntj += 1
    #                 self.assertEqual((1, 0), cnn.query('''INSERT INTO tbltestmanyinserts
    #                                                     VALUES (%d, %d, %s)''' %
    #                                                     (i, j, 0.000012345)))
    #                 #cnn.query("INSERT INTO tbltestmanyinserts VALUES (%d, %d, %s)" %
    #                 #          (i, j, 0.000012345))
    #             if not cnti % 10:
    #                 print(datetime.datetime.now(),'\t',str(cnti))
    #         print(datetime.datetime.now(),'\tfinished')
    #     finally:
    #         print(datetime.datetime.now(),'\tcnti =',cnti,'\tcntj =',cntj)
    #         cnn.close()


    def testMySQLDBAPI(self):

        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("truncate tbltest")

        for i in range(10):
            cnn.query("insert into tbltest (test_id, test_string) values (%s, 'test%s')", (i, i,))

        rs = cnn.query("select test_id, test_string from tbltest")

        self.assertEqual((0, 'test0'), rs.rows[0])

        #check that fetchall gets the remainder
        self.assertEqual([(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5'), (6, 'test6'), (7, 'test7'), (8, 'test8'), (9, 'test9')], rs.rows[1:])

        #another query on the same cursor should work
        rs = cnn.query("select test_id, test_string from tbltest")

        #fetch some but not all
        self.assertEqual((0, 'test0'), rs.rows[0])
        self.assertEqual((1, 'test1'), rs.rows[1])
        self.assertEqual((2, 'test2'), rs.rows[2])

        #this should not work, cursor was closed
        cnn.close()

    def testLargePackets(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        cnn.query("truncate tbltest")

        c = 16 * 1024

        blob = '0123456789'
        while 1:
            cnn.query("insert into tbltest (test_id, test_blob) values (%s, %s)", (len(blob), blob,))
            if len(blob) > (c * 2): break
            blob = blob * 2

        rs = cnn.query("select test_id, test_blob from tbltest")
        for row in rs.rows:
            self.assertEqual(row[0], len(row[1]))
            #self.assertEqual(blob[:row[0]], row[1])

        #reread, second time, oversize packet is already present
        rs = cnn.query("select test_id, test_blob from tbltest")
        for row in rs.rows:
            self.assertEqual(row[0], len(row[1]))
            self.assertEqual(blob[:row[0]], row[1])

        cnn.close()

        #have a very low max packet size for oversize packets
        #and check that exception is thrown when trying to read larger packets

    def testEscapeArgs(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("truncate tbltest")

        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (1, 'piet'))
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (2, 'klaas'))
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (3, "pi'et"))

        #classic sql injection, would return all rows if no proper escaping is done
        rs = cnn.query("select test_id, test_string from tbltest where test_string = %s", ("piet' OR 'a' = 'a",))
        self.assertEqual([], rs.rows) #assert no rows are found

        #but we should still be able to find the piet with the apostrophe in its name
        rs = cnn.query("select test_id, test_string from tbltest where test_string = %s", ("pi'et",))
        self.assertEqual([(3, "pi'et")], rs.rows)

        #also we should be able to insert and retrieve blob/string with all possible bytes transparently
        chars = ''.join([chr(i) for i in range(256)])

        cnn.query("insert into tbltest (test_id, test_string, test_blob) values (%s, %s, %s)", (4, chars[:80], chars))
        #cnn.query("insert into tbltest (test_id, test_blob) values (%s, %s)", (4, chars))

        rs = cnn.query("select test_blob, test_string from tbltest where test_id = %s", (4,))
        #self.assertEqual([(chars, chars)], cur.fetchall())
        b, s = rs.rows[0]

        #test blob
        self.assertEqual(256, len(b))
        self.assertEqual(chars, b)

        self.assertEqual(80, len(s))
        self.assertEqual(chars[:80], s)

        cnn.close()

    def testSelectUnicode(self):
        s = 'r\xc3\xa4ksm\xc3\xb6rg\xc3\xa5s'

        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("truncate tbltest")
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (1, 'piet'))
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (2, s))
        cnn.query("insert into tbltest (test_id, test_string) values (%s, %s)", (3, s))

        rs = cnn.query("select test_id, test_string from tbltest")

        result = rs.rows
        self.assertEqual([(1, 'piet'), (2, s), (3, s)], result)

        #test that we can still cleanly roundtrip a blob, (it should not be encoded if we pass
        #it as 'str' argument), eventhough we pass the qry itself as unicode

        blob = ''.join([chr(i) for i in range(256)])

        cnn.query("insert into tbltest (test_id, test_blob) values (%s, %s)", (4, blob))
        rs = cnn.query("select test_blob from tbltest where test_id = %s", (4,))
        b2 = rs.rows[0][0]
        self.assertEqual(str, type(b2))
        self.assertEqual(256, len(b2))
        self.assertEqual(blob, b2)
        cnn.close()

    def testAutoInc(self):

        cnn = umysql.Connection()
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

        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblbigint")
        cnn.query("create table tblbigint (test_id int(11) DEFAULT NULL, test_bigint bigint DEFAULT NULL, test_bigint2 bigint DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")
        cnn.query("insert into tblbigint (test_id, test_bigint, test_bigint2) values (%s, " + str(BIGNUM) + ", %s)", (1, BIGNUM,))
        cnn.query("insert into tblbigint (test_id, test_bigint, test_bigint2) values (%s, " + str(BIGNUM) + ", %s)", (2, BIGNUM,))

        # Make sure both our inserts where correct (ie, the big number was not truncated/modified on insert)
        rs = cnn.query("select test_id from tblbigint where test_bigint = test_bigint2")
        result = rs.rows
        self.assertEqual([(1, ), (2, )], result)

        # Make sure select gets the right values (ie, the big number was not truncated/modified when retrieved)
        rs = cnn.query("select test_id, test_bigint, test_bigint2 from tblbigint where test_bigint = test_bigint2")
        result = rs.rows
        self.assertEqual([(1, BIGNUM, BIGNUM), (2, BIGNUM, BIGNUM)], result)
        cnn.close()

    def testDate(self):
        # Tests the behaviour of insert/select with mysql/DATE <-> python/datetime.date

        d_date = datetime.date(2010, 2, 11)
        d_string = "2010-02-11"

        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tbldate")
        cnn.query("create table tbldate (test_id int(11) DEFAULT NULL, test_date date DEFAULT NULL, test_date2 date DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")

        cnn.query("insert into tbldate (test_id, test_date, test_date2) values (%s, %s, %s)", (1, d_string, d_date))

        # Make sure our insert was correct
        rs = cnn.query("select test_id from tbldate where test_date = test_date2")
        result = rs.rows
        self.assertEqual([(1, )], result)

        # Make sure select gets the right value back
        rs = cnn.query("select test_id, test_date, test_date2 from tbldate where test_date = test_date2")
        result = rs.rows
        self.assertEqual([(1, d_date, d_date)], result)
        cnn.close()

    def testDateTime(self):
        # Tests the behaviour of insert/select with mysql/DATETIME <-> python/datetime.datetime

        d_date = datetime.datetime(2010, 2, 11, 13, 37, 42)
        d_string = "2010-02-11 13:37:42"

        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)


        cnn.query("drop table if exists tbldate")
        cnn.query("create table tbldate (test_id int(11) DEFAULT NULL, test_date datetime DEFAULT NULL, test_date2 datetime DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")

        cnn.query("insert into tbldate (test_id, test_date, test_date2) values (%s, %s, %s)", (1, d_string, d_date))

        # Make sure our insert was correct
        rs = cnn.query("select test_id from tbldate where test_date = test_date2")
        result = rs.rows
        self.assertEqual([(1, )], result)

        # Make sure select gets the right value back
        rs = cnn.query("select test_id, test_date, test_date2 from tbldate where test_date = test_date2")
        result = rs.rows
        self.assertEqual([(1, d_date, d_date)], result)
        cnn.close()

    def testZeroDates(self):
        # Tests the behaviour of zero dates
        zero_datetime = "0000-00-00 00:00:00"
        zero_date = "0000-00-00"

        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tbldate")
        cnn.query("create table tbldate (test_id int(11) DEFAULT NULL, test_date date DEFAULT NULL, test_datetime datetime DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")

        cnn.query("insert into tbldate (test_id, test_date, test_datetime) values (%s, %s, %s)", (1, zero_date, zero_datetime))

        # Make sure we get None-values back
        rs = cnn.query("select test_id, test_date, test_datetime from tbldate where test_id = 1")
        result = rs.rows
        self.assertEqual([(1, None, None)], result)
        cnn.close()

    def testUnicodeUTF8(self):
        peacesign_unicode = '\u262e'
        peacesign_utf8 = b'\xe2\x98\xae'

        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblutf")
        cnn.query("create table tblutf (test_id int(11) DEFAULT NULL, test_string VARCHAR(32) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8")

        cnn.query("insert into tblutf (test_id, test_string) values (%s, %s)", (1, peacesign_unicode,)) # This should be encoded in utf8
        cnn.query("insert into tblutf (test_id, test_string) values (%s, %s)", (2, peacesign_utf8,))

        rs = cnn.query("select test_id, test_string from tblutf")
        result = rs.rows

        # We expect unicode strings back
        self.assertEqual([(1, peacesign_unicode), (2, peacesign_unicode)], result)
        cnn.close()

    def testBinary(self):
        peacesign_binary = b"\xe2\x98\xae"
        peacesign_binary2 = b"\xe2\x98\xae" * 10

        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblbin")
        cnn.query("create table tblbin (test_id int(11) DEFAULT NULL, test_binary VARBINARY(30) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8")

        cnn.query("insert into tblbin (test_id, test_binary) values (%s, %s)", (1, peacesign_binary))
        cnn.query("insert into tblbin (test_id, test_binary) values (%s, %s)", (2, peacesign_binary2))

        rs = cnn.query("select test_id, test_binary from tblbin")
        result = rs.rows

        # We expect binary strings back
        self.assertEqual([(1, peacesign_binary),(2, peacesign_binary2)], result)
        cnn.close()


    def testBlob(self):
        peacesign_binary = b"\xe2\x98\xae"
        peacesign_binary2 = b"\xe2\x98\xae" * 1024

        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblblob")
        cnn.query("create table tblblob (test_id int(11) DEFAULT NULL, test_blob BLOB DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8")

        cnn.query("insert into tblblob (test_id, test_blob) values (%s, %s)", (1, peacesign_binary))
        cnn.query("insert into tblblob (test_id, test_blob) values (%s, %s)", (2, peacesign_binary2))

        rs = cnn.query("select test_id, test_blob from tblblob")
        result = rs.rows

        # We expect binary strings back
        self.assertEqual([(1, peacesign_binary.decode('utf-8')),(2, peacesign_binary2.decode('utf-8'))], result)
        cnn.close()

    # TODO: Port this one int Python 3
    # def testCharsets(self):
    #     aumlaut_unicode = u"\u00e4"
    #     aumlaut_utf8 = "\xc3\xa4"
    #     aumlaut_latin1 = "\xe4"
    #
    #     cnn = umysql.Connection()
    #     cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
    #
    #     cnn.query("drop table if exists tblutf")
    #     cnn.query("create table tblutf (test_mode VARCHAR(32) DEFAULT NULL, test_utf VARCHAR(32) DEFAULT NULL, test_latin1 VARCHAR(32)) ENGINE=MyISAM DEFAULT CHARSET=utf8")
    #
    #     # We insert the same character using two different encodings
    #     cnn.query("set names utf8")
    #     cnn.query("insert into tblutf (test_mode, test_utf, test_latin1) values ('utf8', _utf8'" + aumlaut_utf8 + "', _latin1'" + aumlaut_latin1 + "')")
    #
    #     cnn.query("set names latin1")
    #     cnn.query("insert into tblutf (test_mode, test_utf, test_latin1) values ('latin1', _utf8'" + aumlaut_utf8 + "', _latin1'" + aumlaut_latin1 + "')")
    #
    #     # We expect the driver to always give us unicode strings back
    #     expected = [("utf8", aumlaut_unicode, aumlaut_unicode), ("latin1", aumlaut_unicode, aumlaut_unicode)]
    #
    #     # Fetch and test with different charsets
    #     for charset in ("latin1", "utf8", "cp1250"):
    #         cnn.query("set names " + charset)
    #         rs = cnn.query("select test_mode, test_utf, test_latin1 from tblutf")
    #         result = rs.rows
    #         self.assertEqual(result, expected)
    #
    #     cnn.close()

    # TODO: Port this one int Python 3
    # def testTextCharsets(self):
    #     aumlaut_unicode = "\u00e4"
    #     aumlaut_utf8 = "\xc3\xa4"
    #     aumlaut_latin1 = "\xe4"
    #
    #     cnn = umysql.Connection()
    #     cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
    #
    #     cnn.query("drop table if exists tblutf")
    #     cnn.query("create table tblutf (test_mode TEXT DEFAULT NULL, test_utf TEXT DEFAULT NULL, test_latin1 TEXT) ENGINE=MyISAM DEFAULT CHARSET=utf8")
    #
    #     # We insert the same character using two different encodings
    #     cnn.query("set names utf8")
    #     cnn.query("insert into tblutf (test_mode, test_utf, test_latin1) values ('utf8', _utf8'" + aumlaut_utf8 + "', _latin1'" + aumlaut_latin1 + "')")
    #
    #     cnn.query("set names latin1")
    #     cnn.query("insert into tblutf (test_mode, test_utf, test_latin1) values ('latin1', _utf8'" + aumlaut_utf8 + "', _latin1'" + aumlaut_latin1 + "')")
    #
    #     # We expect the driver to always give us unicode strings back
    #     expected = [("utf8", aumlaut_unicode, aumlaut_unicode), ("latin1", aumlaut_unicode, aumlaut_unicode)]
    #
    #     # Fetch and test with different charsets
    #     for charset in ("latin1", "utf8", "cp1250"):
    #         cnn.query("set names " + charset)
    #         rs = cnn.query("select test_mode, test_utf, test_latin1 from tblutf")
    #         result = rs.rows
    #         self.assertEqual(result, expected)
    #
    #     cnn.close()

    def testUtf8mb4(self):
        utf8mb4chr = '\U0001f603'

        # We expected we can insert utf8mb4 character, than fetch it back
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)

        cnn.query("drop table if exists tblutf8mb4")
        cnn.query("create table tblutf8mb4 (test_text TEXT DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4")

        cnn.query("insert into tblutf8mb4 (test_text) values ('" + utf8mb4chr + "')")
        cnn.query("set names utf8mb4")
        cnn.query("insert into tblutf8mb4 (test_text) values ('" + utf8mb4chr + "')")

        rs = cnn.query("select test_text from tblutf8mb4;")
        result = rs.rows
        self.assertNotEqual(result[0][0], utf8mb4chr)
        self.assertEqual(result[1][0], utf8mb4chr)

        cnn.query("set names utf8")
        rs = cnn.query("select test_text from tblutf8mb4;")
        result = rs.rows
        self.assertNotEqual(result[1][0], utf8mb4chr)

        cnn.close()

    def testPercentEscaping(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        rs = cnn.query("SELECT * FROM `tblautoincint` WHERE `test_id` LIKE '%%10%%'")
        self.assertEqual([(100, 'piet'), (101, 'piet')], rs.rows)

        rs = cnn.query("SELECT * FROM `tblautoincint` WHERE `test_id` LIKE '%%%s%%'", [10])
        self.assertEqual([(100, 'piet'), (101, 'piet')], rs.rows)

        # SqlAlchemy query style
        rs = cnn.query("SELECT * FROM `tblautoincint` WHERE `test_id` LIKE concat(concat('%%', %s), '%%')", [10])
        self.assertEqual([(100, 'piet'), (101, 'piet')], rs.rows)

        cnn.close()

    def testTimestamp(self):
        ts = datetime.datetime(2013, 4, 26, 9, 53, 48)
        ts_val = ts.strftime("%Y-%m-%d %H:%M:%S")
        drop = 'drop table if exists tbltimestamp'
        create = 'create table tbltimestamp (idx int not null, ts timestamp)'
        insert = 'insert into tbltimestamp (idx, ts) values (%s, %s)'
        select = 'select idx, ts from tbltimestamp where idx = %s'
        idx = 1

        cnn = umysql.Connection()
        cnn.connect(DB_HOST, 3306, DB_USER, DB_PASSWD, DB_DB)
        cnn.query(drop)
        cnn.query(create)
        cnn.query(insert, (idx, ts_val,))
        result = cnn.query(select, (idx,))
        self.assertEqual(result.rows[0], (idx, ts))

    def testUnsignedInt(self):
        cnn = umysql.Connection()
        cnn.connect (DB_HOST, DB_PORT, DB_USER, DB_PASSWD, DB_DB)
        cnn.query('DROP TABLE IF EXISTS tblunsignedint')
        cnn.query('''CREATE TABLE tblunsignedint(
            `big` BIGINT UNSIGNED NOT NULL,
            `int` INT UNSIGNED NOT NULL,
            `medium` MEDIUMINT UNSIGNED NOT NULL,
            `short` SMALLINT UNSIGNED NOT NULL,
            `tiny` TINYINT UNSIGNED NOT NULL
        )''')
        values1 = (0xffffffffffffffff, 0xffffffff, 0xffffff, 0xffff, 0xff, )
        values2 = (0x8000000000000000, 0x80000000, 0x800000, 0x8000, 0x80, )
        values3 = (0x8fedcba098765432, 0x8fedcba0, 0x8fedcb, 0x8fed, 0x8f, )
        rc, rid = cnn.query('INSERT INTO `tblunsignedint` VALUES(%s, %s, %s, %s, %s)', values1)
        self.assertEqual(rc, 1)
        rc, rid = cnn.query('INSERT INTO `tblunsignedint` VALUES(%s, %s, %s, %s, %s)', values2)
        self.assertEqual(rc, 1)
        rc, rid = cnn.query('INSERT INTO `tblunsignedint` VALUES(%s, %s, %s, %s, %s)', values3)
        self.assertEqual(rc, 1)
        rs = cnn.query('SELECT * FROM `tblunsignedint`')
        self.assertEqual([values1, values2, values3, ], rs.rows)
        cnn.close()

if __name__ == '__main__':
    unittest.main()
