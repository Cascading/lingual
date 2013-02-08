Build Instructions
==================

    gradle clean jar
    hadoop jar build/libs/lingual-examples-1.0.0-wip-dev.jar

This sample app uses Lingual to open a JDBC connection and run a SQL query:

    SELECT *
      FROM "EXAMPLE"."SALES_FACT_1997" AS s
      JOIN "EXAMPLE"."EMPLOYEE" AS e
        ON e."EMPID\" = s."CUST_ID"
    ;

Example data is listed in the `src/main/resources/data/example/` directory,
as CSV files.

The results should look like:

    CUST_ID=100; PROD_ID=10; EMPID=100; NAME=Bill
    CUST_ID=150; PROD_ID=20; EMPID=150; NAME=Sebastian