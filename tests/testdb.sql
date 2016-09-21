use test;

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
