# MechironAnalyzer
## Overview
[Mechiron] (http://www.mechiron.org) has been developed in order to unify the products information, its prices and other relevant metadata being published by the largest grocery stores in Israel on a daily basis and visualize into beautiful dashboards.
Now that its all **unified** into one place, anyone can navigate through the data, slice & dice and drill down into specific product's information easily.The information published by law and being updated frequently during the day (2~ hours). 

## Goal
It is a **non-profit initiative** and the goal is to give the power to the public! So anyone can navigate through the prices, manufacturers' details and other interesting information to spot trends and see in a **bird's eye-view** market information. 
Whats the price of a specific product over time?
Who are the main manufacturers that compete on the money that we spend as consumers?
What are the pricing trends for a specific products category?
and much more!

**Ah, hmmm, Its also an opensource project!** You can use it and I will be more than happy if you could contribute to it...

## The overall project structure
The **Mechiron** project has been seperated into 2 opensource GitHub repositories:
- [Mechiron repository] (https://github.com/eran-levy/Mechiron) - is responsible to gather the information published from the configured FTP sources and export into a unified CSV format. Written in Python and can be extended easily. For more information see the GitHub repository README.
- MechironAnalyzer (this GitHub repository) - which is reponsible for the data processing. 

## MechironAnalyzer project structure
This project currently separated into 3 main units:
- The MechironImporter: is in charge of processing the downloaded raw CSV data, joining it with the existing data, validation, cleansing, transformation and importing the items, prices and stores. 
- The CsvLoader: load the raw CSV data files into HDFS and load into the "dwdata" Hive table that will be used by the different data enhancers.
- The Enhancer: utilize the large dataset we collect over time in order to enhance the processed data, i.e. item names, manufacture names, etc. 

**Since its an experimental, currently its very basic and have to be improved.** For example, the names enhancer performs unification based on simple frequecy or the imported can be improved with better validation, cleansing and exception handling.

Documentation will be improved, see more information in the code itself.

## Installation
### Prerequisite
1. MySQL 5.17
2. Hadoop 2.6.4
3. Hive 1.2.1
4. Spark 1.4.1
5. Scala 2.10
6. Java 1.8

### Steps
* Create a MySQL Database using the sql script available under this repository. The script will create a database named: **retailprices** and 3 tables: items, prices, store 
* Create a new Hive table named **dwdata**  with the following statement:
```
create table dwdata (chain_id STRING, sub_chain_id STRING, store_id STRING, item_id STRING, item_price DOUBLE, qty DOUBLE, manufacture_name STRING, manufacture_country STRING, manufacture_item_desc STRING, item_name STRING, item_code STRING, price_update_date TIMESTAMP) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|";
```
* Configure Spark to access Hive and make sure you are able to create a new HiveContext in spark-shell
* You can configure Hive on Spark as its execution engine. Obviously you can use others
* Clone the GitHub repository into your local projects folder
* Its possible to open the Scala project using InellijIDEA or any one of your favorite IDEs
* Build sources and download dependencies using sbt (see the build.sbt for more information):
```
sbt compile
```
* Configure the relevant properties under: src/main/resources/config.properties
* Run one of the relevant units: MechironImporter, CsvLoader or the EnhanceRunner

For comments or further detilas, please don't hesitate to mail me anytime: mceranlevy@gmail.com
I will continue to maintain the project - new features, unit tests, better error handling, etc.
Stay tuned,
Eran Levy

