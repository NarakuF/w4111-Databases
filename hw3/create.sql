CREATE SCHEMA `CSVCatalog` ;

CREATE TABLE `CSVCatalog`.`csvtables` (
  `table_name` varchar(32) NOT NULL,
  `file_name` varchar(256) NOT NULL,
  PRIMARY KEY (`table_name`),
  UNIQUE KEY `file_name_UNIQUE` (`file_name`));

CREATE TABLE `CSVCatalog`.`columns` (
  `table_name` varchar(32) NOT NULL,
  `column_name` varchar(32) NOT NULL,
  `type` enum('text','number') NOT NULL,
  `nullable` tinyint(4) NOT NULL,
  PRIMARY KEY (`table_name`,`column_name`),
  CONSTRAINT `c_to_t` FOREIGN KEY (`table_name`) REFERENCES `tables` (`table_name`));

CREATE TABLE `CSVCatalog`.`indexes` (
  `table_name` varchar(32) NOT NULL,
  `column_name` varchar(32) NOT NULL,
  `index_name` varchar(32) NOT NULL,
  `type` enum('PRIMARY','UNIQUE','INDEX') NOT NULL,
  `position` int(11) NOT NULL,
  PRIMARY KEY (`table_name`,`column_name`),
  CONSTRAINT `i_to_c` FOREIGN KEY (`table_name`, `column_name`) REFERENCES `columns` (`table_name`, `column_name`));