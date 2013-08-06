Build Instructions
==================

    gradle clean fatJar
    hadoop jar build/libs/lingual-examples-1.0.0-wip-dev.jar cascading.lingual.examples.foodmart.JdbcExample
    hadoop jar build/libs/lingual-examples-1.0.0-wip-dev.jar cascading.lingual.examples.foodmart.FlowExample

This sample app uses Lingual to open a JDBC connection and run a SQL query
against a local filesystem platform:

    SELECT *
      FROM "example"."sales_fact_1997" AS s
      JOIN "example"."employee" AS e
        ON e."EMPID\" = s."CUST_ID"
    ;

Example data is listed in the `src/main/resources/data/example/` directory,
as CSV files.

The results should look like:

    CUST_ID=100; PROD_ID=10; EMPID=100; NAME=Bill
    CUST_ID=150; PROD_ID=20; EMPID=150; NAME=Sebastian