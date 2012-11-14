use gevent_test;

CREATE TABLE `tbltest` (
  `test_id` int(11),
  `test_string` varchar(1024),
  `test_blob` longblob
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `tblautoincint` (
  `test_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `test_string` varchar(1024),
  PRIMARY KEY(test_id)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `tblautoincbigint` (
  `test_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `test_string` varchar(1024),
  PRIMARY KEY(test_id)  
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

DELIMITER //
CREATE PROCEDURE CreateTest(IN id int(11), IN str varchar(1024))
BEGIN
  INSERT INTO tbltest VALUES (id, str, NULL);
END //

CREATE PROCEDURE QueryTest(IN str varchar(1024), OUT rowcount int(11))
BEGIN
  SELECT COUNT(*) INTO rowcount FROM tbltest WHERE test_string=str;
  SELECT * FROM tbltest WHERE test_string=str;
END //

CREATE PROCEDURE TestMultiResult()
BEGIN
    SELECT 1;
    SELECT 2;
    SELECT 3;
END //
DELIMITER ;

GRANT ALL on gevent_test.* to 'gevent_test'@'localhost' identified by 'gevent_test';
