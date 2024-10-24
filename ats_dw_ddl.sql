CREATE TABLE `Dim_Time` (
  `time_id ` int,
  `date` date,
  `day_of_week` varchar(10,
  `month` varchar(10),
  `quarter` varchar(10),
  `year` int,
  PRIMARY KEY (`time_id `)
);

CREATE TABLE `Dim_bonds` (
  `bond_ID` bigint,
  `treasury_name` varchar(300),
  PRIMARY KEY (`bond_ID`)
);

CREATE TABLE `Fact_Bond_Prices` (
  `fact_id` int,
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
  FOREIGN KEY (`time_id`) REFERENCES `Dim_Time`(`time_id `),
  FOREIGN KEY (`bond_id`) REFERENCES `Dim_bonds`(`bond_ID`)
);
