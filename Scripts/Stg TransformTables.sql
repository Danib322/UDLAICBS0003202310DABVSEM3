use dabvdbstg;
CREATE TABLE PORCES_ETL
(
   PROCESO_ID INTEGER NOT NULL AUTO_INCREMENT,
   FECHA_EJECUCION DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
   primary key(PROCESO_ID)
);

CREATE TABLE CHANNELS_TRAN 
    (
     ID INTEGER NOT NULL AUTO_INCREMENT,
     CHANNEL_ID INTEGER  NOT NULL , 
     CHANNEL_DESC VARCHAR (20)  NOT NULL , 
     CHANNEL_CLASS VARCHAR (20)  NOT NULL , 
     CHANNEL_CLASS_ID INTEGER NOT NULL,
     PROCESOETL_ID INTEGER NOT NULL,
     PRIMARY KEY (ID)
    )
;


CREATE TABLE COUNTRIES_TRAN 
    (
     ID INTEGER NOT NULL AUTO_INCREMENT,
     COUNTRY_ID INTEGER  NOT NULL , 
     COUNTRY_NAME VARCHAR (40)  NOT NULL , 
     COUNTRY_REGION VARCHAR (20)  NOT NULL , 
     COUNTRY_REGION_ID INTEGER  NOT NULL,
     PROCESOETL_ID INTEGER NOT NULL,
     PRIMARY KEY (ID)
    ) 
;



CREATE TABLE CUSTOMERS_TRAN 
    ( 
     ID INTEGER NOT NULL AUTO_INCREMENT,
     CUST_ID INTEGER  NOT NULL , 
     CUST_FIRST_NAME VARCHAR (20)  NOT NULL , 
     CUST_LAST_NAME VARCHAR (40)  NOT NULL , 
     CUST_GENDER CHAR (1)  NOT NULL , 
     CUST_YEAR_OF_BIRTH INTEGER (4)  NOT NULL , 
     CUST_MARITAL_STATUS VARCHAR (20) , 
     CUST_STREET_ADDRESS VARCHAR (40)  NOT NULL , 
     CUST_POSTAL_CODE VARCHAR (10)  NOT NULL , 
     CUST_CITY VARCHAR (30)  NOT NULL , 
     CUST_STATE_PROVINCE VARCHAR (40)  NOT NULL , 
     COUNTRY_ID INTEGER NOT NULL , 
     CUST_MAIN_PHONE_INTEGER VARCHAR (25)  NOT NULL , 
     CUST_INCOME_LEVEL VARCHAR (30) , 
     CUST_CREDIT_LIMIT INTEGER , 
     CUST_EMAIL VARCHAR (30),
     PROCESOETL_ID INTEGER NOT NULL,
     PRIMARY KEY (ID)
    )
;

CREATE TABLE PRODUCTS_TRAN
    (
     ID INTEGER NOT NULL AUTO_INCREMENT, 
     PROD_ID INTEGER  NOT NULL , 
     PROD_NAME VARCHAR (50)  NOT NULL , 
     PROD_DESC VARCHAR (4000)  NOT NULL , 
     PROD_CATEGORY VARCHAR (50)  NOT NULL , 
     PROD_CATEGORY_ID INTEGER  NOT NULL , 
     PROD_CATEGORY_DESC VARCHAR (2000)  NOT NULL , 
     PROD_WEIGHT_CLASS INTEGER (3)  NOT NULL , 
     SUPPLIER_ID INTEGER   NOT NULL , 
     PROD_STATUS VARCHAR (20)  NOT NULL , 
     PROD_LIST_PRICE DECIMAL (8,2)  NOT NULL , 
     PROD_MIN_PRICE DECIMAL (8,2)  NOT NULL,
     PROCESOETL_ID INTEGER NOT NULL,
	PRIMARY KEY (ID)
    )
;

CREATE TABLE PROMOTIONS_TRAN 
    (
     ID INTEGER NOT NULL AUTO_INCREMENT,
     PROMO_ID INTEGER   NOT NULL , 
     PROMO_NAME VARCHAR (30)  NOT NULL , 
     PROMO_COST DECIMAL (10,2)  NOT NULL , 
     PROMO_BEGIN_DATE DATE  NOT NULL , 
     PROMO_END_DATE DATE  NOT NULL,
     PROCESOETL_ID INTEGER NOT NULL,
     PRIMARY KEY (ID)
    )
;


CREATE TABLE SALES_TRAN 
    (
     ID INTEGER NOT NULL AUTO_INCREMENT,
     PROD_ID INTEGER  NOT NULL , 
     CUST_ID INTEGER  NOT NULL , 
     TIME_ID DATE  NOT NULL , 
     CHANNEL_ID INTEGER  NOT NULL , 
     PROMO_ID INTEGER   NOT NULL , 
     QUANTITY_SOLD DECIMAL (10,2)  NOT NULL , 
     AMOUNT_SOLD DECIMAL (10,2)  NOT NULL,
     PROCESOETL_ID INTEGER NOT NULL,
     PRIMARY KEY (ID)
    ) 
;

CREATE TABLE TIMES 
    (
     ID INTEGER NOT NULL AUTO_INCREMENT,
     TIME_ID DATE  NOT NULL , 
     DAY_NAME VARCHAR (9)  NOT NULL , 
     DAY_INTEGER_IN_WEEK INTEGER (1)  NOT NULL , 
     DAY_INTEGER_IN_MONTH INTEGER (2)  NOT NULL , 
     CALENDAR_WEEK_INTEGER INTEGER (2)  NOT NULL , 
     CALENDAR_MONTH_INTEGER INTEGER (2)  NOT NULL , 
     CALENDAR_MONTH_DESC VARCHAR (8)  NOT NULL , 
     END_OF_CAL_MONTH DATE  NOT NULL , 
     CALENDAR_MONTH_NAME VARCHAR (9)  NOT NULL , 
     CALENDAR_QUARTER_DESC CHAR (7)  NOT NULL , 
     CALENDAR_YEAR INTEGER (4)  NOT NULL,
     PROCESOETL_ID INTEGER NOT NULL,
     PRIMARY KEY (ID)
    ) 
;