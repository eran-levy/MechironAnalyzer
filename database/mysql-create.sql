CREATE DATABASE `retailprices` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin */;
CREATE TABLE `items` (
  `item_code` varchar(25) CHARACTER SET utf8 DEFAULT NULL,
  `item_name` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `manufacture_name` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `manufacture_country` varchar(30) COLLATE utf8_bin DEFAULT NULL,
  `manufacture_item_desc` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  KEY `item_code_indx` (`item_code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
CREATE TABLE `prices` (
  `chain_id` varchar(20) COLLATE utf8_bin DEFAULT NULL,
  `sub_chain_id` varchar(20) COLLATE utf8_bin DEFAULT NULL,
  `store_id` varchar(20) COLLATE utf8_bin DEFAULT NULL,
  `item_id` varchar(20) COLLATE utf8_bin DEFAULT NULL,
  `item_price` double DEFAULT NULL,
  `qty` double DEFAULT NULL,
  `item_code` varchar(25) COLLATE utf8_bin DEFAULT NULL,
  `price_update_date` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
CREATE TABLE `store` (
  `chain_id` tinytext COLLATE utf8_bin,
  `chain_name` tinytext COLLATE utf8_bin,
  `store_id` tinytext COLLATE utf8_bin,
  `store_name` tinytext COLLATE utf8_bin,
  `store_address` tinytext COLLATE utf8_bin,
  `store_city` tinytext COLLATE utf8_bin,
  `store_zipcode` tinytext COLLATE utf8_bin
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

