CREATE TABLE `Dim_Time` (
  `time_id` int auto_increment,
  `date` date,
  `day_of_week` varchar(10),
  `month` varchar(10),
  `quarter` varchar(10),
  `year` int,
  PRIMARY KEY (`time_id`)
);

CREATE TABLE `Dim_bonds` (
  `bond_ID` bigint,
  `treasury_name` varchar(300),
  PRIMARY KEY (`bond_ID`)
);

CREATE TABLE `Fact_Bond_Prices` (
  `fact_id` int auto_increment,
  `time_id` int,
  `bond_id` bigint,
  `one_month` float,
  `two_month` float,
  `three_month` float,
  `six_month` float,
  `one_year` float,
  `two_year` float,
  `three_year` float,
  `five_year` float,
  `ten_year` float,
  `twenty_year` float,
  `thirty_year` float,
  PRIMARY KEY (`fact_id`),
  FOREIGN KEY (`time_id`) REFERENCES `Dim_Time`(`time_id`),
  FOREIGN KEY (`bond_id`) REFERENCES `Dim_bonds`(`bond_ID`)
);

CREATE TABLE `Dim_Indexes` (
  `index_ID` bigint,
  `index_name` varchar(300),
  `symbol` varchar(10)
  PRIMARY KEY (`index_ID`)
);

CREATE TABLE `Dim_Commodites` (
  `commodity_ID` bigint,
  `commodity_name` varchar(30),
  `symbol` varchar(10)
  PRIMARY KEY (`commodity_ID`)
);

CREATE TABLE `Fact_Commodity_Prices` (
  `fact_id` int auto_increment,
  `time_id` int,
  `commodity_id` bigint,
  `open` float,
  `high` float,
  `low` float,
  `close` float,
  `adjClose` float,
  `volume` bigint,
  `unadjusted_volume` bigint,
  `change` float,
  `change_percentage` float,
  `vwap` float,
  `change_over_time` float,
  PRIMARY KEY (`fact_id`),
  FOREIGN KEY (`commodity_id`) REFERENCES `Dim_Commodites`(`commodity_ID`)
);

CREATE TABLE `Fact_Index_Prices` (
  `fact_id` int auto_increment,
  `time_id` int,
  `index_id` bigint,
  `open` float,
  `high` float,
  `low` float,
  `close` float,
  `adjClose` float,
  `volume` bigint,
  `unadjustedVolume` bigint,
  `change` float,
  `change_percent` float,
  `vwap` float,
  `changeOverTime` float,
  PRIMARY KEY (`fact_id`),
  FOREIGN KEY (`index_id`) REFERENCES `Dim_Indexes`(`index_ID`)
);

CREATE TABLE `Dim_company_statements` (
  `company_id` bigint,
  `company_name` varchar(300),
  `symbol` varchar(10),
  `currency` varchar(10),
  `beta` float,
  `volAvg` float,
  `mktCap` bigint,
  `cik` varchar(16),
  `isin` varchar(16),
  `cusip` varchar(16),
  `exchangeFullName` varchar(100),
  `exchange` varchar(10),
  `industry` varchar(300),
  `ceo` varchar(100),
  `sector` varchar(300),
  `country` varchar(300),
  `fullTimeEmployees` bigint,
  `phone` integer,
  `address` varchar(100),
  `city` varchar(100),
  `state` varchar(30),
  `zip` varchar(10),
  `ipoDate` date,
  `isEtf` bool,
  `dcfDiff` float,
  `dcf` float,
  `isActivelyTrading` bool,
  `isFund` bool,
  PRIMARY KEY (`company_id`)
);

CREATE TABLE `Fact_Stock_Prices` (
  `fact_id` int auto_increment,
  `time_id` int,
  `company_id` bigint,
  `lastDiv` float,
  `open` float,
  `high` float,
  `low` float,
  `close` float,
  `adj_close` float,
  `volume` bigint,
  `unadjusted_volume` float,
  `change` float,
  `change_percent` float,
  `vwap` float,
  `change_over_time` float,
  PRIMARY KEY (`fact_id`),
  FOREIGN KEY (`company_id`) REFERENCES `Dim_company_statements`(`company_id`)
);


