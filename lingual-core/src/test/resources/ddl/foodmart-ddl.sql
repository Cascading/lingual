DROP TABLE "sales_fact_1997";
CREATE TABLE "sales_fact_1997"(
    "product_id" INTEGER NOT NULL,
    "time_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "promotion_id" INTEGER NOT NULL,
    "store_id" INTEGER NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL);
DROP TABLE "sales_fact_1998";
CREATE TABLE "sales_fact_1998"(
    "product_id" INTEGER NOT NULL,
    "time_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "promotion_id" INTEGER NOT NULL,
    "store_id" INTEGER NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL);
DROP TABLE "sales_fact_dec_1998";
CREATE TABLE "sales_fact_dec_1998"(
    "product_id" INTEGER NOT NULL,
    "time_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "promotion_id" INTEGER NOT NULL,
    "store_id" INTEGER NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL);
DROP TABLE "inventory_fact_1997";
CREATE TABLE "inventory_fact_1997"(
    "product_id" INTEGER NOT NULL,
    "time_id" INTEGER,
    "warehouse_id" INTEGER,
    "store_id" INTEGER,
    "units_ordered" INTEGER,
    "units_shipped" INTEGER,
    "warehouse_sales" DECIMAL(10,4),
    "warehouse_cost" DECIMAL(10,4),
    "supply_time" SMALLINT,
    "store_invoice" DECIMAL(10,4));
DROP TABLE "inventory_fact_1998";
CREATE TABLE "inventory_fact_1998"(
    "product_id" INTEGER NOT NULL,
    "time_id" INTEGER,
    "warehouse_id" INTEGER,
    "store_id" INTEGER,
    "units_ordered" INTEGER,
    "units_shipped" INTEGER,
    "warehouse_sales" DECIMAL(10,4),
    "warehouse_cost" DECIMAL(10,4),
    "supply_time" SMALLINT,
    "store_invoice" DECIMAL(10,4));
DROP TABLE "agg_pl_01_sales_fact_1997";
CREATE TABLE "agg_pl_01_sales_fact_1997"(
    "product_id" INTEGER NOT NULL,
    "time_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "store_sales_sum" DECIMAL(10,4) NOT NULL,
    "store_cost_sum" DECIMAL(10,4) NOT NULL,
    "unit_sales_sum" DECIMAL(10,4) NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_ll_01_sales_fact_1997";
CREATE TABLE "agg_ll_01_sales_fact_1997"(
    "product_id" INTEGER NOT NULL,
    "time_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_l_03_sales_fact_1997";
CREATE TABLE "agg_l_03_sales_fact_1997"(
    "time_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_l_04_sales_fact_1997";
CREATE TABLE "agg_l_04_sales_fact_1997"(
    "time_id" INTEGER NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL,
    "customer_count" INTEGER NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_l_05_sales_fact_1997";
CREATE TABLE "agg_l_05_sales_fact_1997"(
    "product_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "promotion_id" INTEGER NOT NULL,
    "store_id" INTEGER NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_c_10_sales_fact_1997";
CREATE TABLE "agg_c_10_sales_fact_1997"(
    "month_of_year" SMALLINT NOT NULL,
    "quarter" VARCHAR(30) NOT NULL,
    "the_year" SMALLINT NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL,
    "customer_count" INTEGER NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_c_14_sales_fact_1997";
CREATE TABLE "agg_c_14_sales_fact_1997"(
    "product_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "store_id" INTEGER NOT NULL,
    "promotion_id" INTEGER NOT NULL,
    "month_of_year" SMALLINT NOT NULL,
    "quarter" VARCHAR(30) NOT NULL,
    "the_year" SMALLINT NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_lc_100_sales_fact_1997";
CREATE TABLE "agg_lc_100_sales_fact_1997"(
    "product_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "quarter" VARCHAR(30) NOT NULL,
    "the_year" SMALLINT NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_c_special_sales_fact_1997";
CREATE TABLE "agg_c_special_sales_fact_1997"(
    "product_id" INTEGER NOT NULL,
    "promotion_id" INTEGER NOT NULL,
    "customer_id" INTEGER NOT NULL,
    "store_id" INTEGER NOT NULL,
    "time_month" SMALLINT NOT NULL,
    "time_quarter" VARCHAR(30) NOT NULL,
    "time_year" SMALLINT NOT NULL,
    "store_sales_sum" DECIMAL(10,4) NOT NULL,
    "store_cost_sum" DECIMAL(10,4) NOT NULL,
    "unit_sales_sum" DECIMAL(10,4) NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_g_ms_pcat_sales_fact_1997";
CREATE TABLE "agg_g_ms_pcat_sales_fact_1997"(
    "gender" VARCHAR(30) NOT NULL,
    "marital_status" VARCHAR(30) NOT NULL,
    "product_family" VARCHAR(30),
    "product_department" VARCHAR(30),
    "product_category" VARCHAR(30),
    "month_of_year" SMALLINT NOT NULL,
    "quarter" VARCHAR(30) NOT NULL,
    "the_year" SMALLINT NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL,
    "customer_count" INTEGER NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "agg_lc_06_sales_fact_1997";
CREATE TABLE "agg_lc_06_sales_fact_1997"(
    "time_id" INTEGER NOT NULL,
    "city" VARCHAR(30) NOT NULL,
    "state_province" VARCHAR(30) NOT NULL,
    "country" VARCHAR(30) NOT NULL,
    "store_sales" DECIMAL(10,4) NOT NULL,
    "store_cost" DECIMAL(10,4) NOT NULL,
    "unit_sales" DECIMAL(10,4) NOT NULL,
    "fact_count" INTEGER NOT NULL);
DROP TABLE "currency";
CREATE TABLE "currency"(
    "currency_id" INTEGER NOT NULL,
    "date" DATE NOT NULL,
    "currency" VARCHAR(30) NOT NULL,
    "conversion_ratio" DECIMAL(10,4) NOT NULL);
DROP TABLE "account";
CREATE TABLE "account"(
    "account_id" INTEGER NOT NULL,
    "account_parent" INTEGER,
    "account_description" VARCHAR(30),
    "account_type" VARCHAR(30) NOT NULL,
    "account_rollup" VARCHAR(30) NOT NULL,
    "Custom_Members" VARCHAR(255));
DROP TABLE "category";
CREATE TABLE "category"(
    "category_id" VARCHAR(30) NOT NULL,
    "category_parent" VARCHAR(30),
    "category_description" VARCHAR(30) NOT NULL,
    "category_rollup" VARCHAR(30));
DROP TABLE "customer";
CREATE TABLE "customer"(
    "customer_id" INTEGER NOT NULL,
    "account_num" BIGINT NOT NULL,
    "lname" VARCHAR(30) NOT NULL,
    "fname" VARCHAR(30) NOT NULL,
    "mi" VARCHAR(30),
    "address1" VARCHAR(30),
    "address2" VARCHAR(30),
    "address3" VARCHAR(30),
    "address4" VARCHAR(30),
    "city" VARCHAR(30),
    "state_province" VARCHAR(30),
    "postal_code" VARCHAR(30) NOT NULL,
    "country" VARCHAR(30) NOT NULL,
    "customer_region_id" INTEGER NOT NULL,
    "phone1" VARCHAR(30) NOT NULL,
    "phone2" VARCHAR(30) NOT NULL,
    "birthdate" DATE NOT NULL,
    "marital_status" VARCHAR(30) NOT NULL,
    "yearly_income" VARCHAR(30) NOT NULL,
    "gender" VARCHAR(30) NOT NULL,
    "total_children" SMALLINT NOT NULL,
    "num_children_at_home" SMALLINT NOT NULL,
    "education" VARCHAR(30) NOT NULL,
    "date_accnt_opened" DATE NOT NULL,
    "member_card" VARCHAR(30),
    "occupation" VARCHAR(30),
    "houseowner" VARCHAR(30),
    "num_cars_owned" INTEGER,
    "fullname" VARCHAR(60) NOT NULL);
DROP TABLE "days";
CREATE TABLE "days"(
    "day" INTEGER NOT NULL,
    "week_day" VARCHAR(30) NOT NULL);
DROP TABLE "department";
CREATE TABLE "department"(
    "department_id" INTEGER NOT NULL,
    "department_description" VARCHAR(30) NOT NULL);
DROP TABLE "employee";
CREATE TABLE "employee"(
    "employee_id" INTEGER NOT NULL,
    "full_name" VARCHAR(30) NOT NULL,
    "first_name" VARCHAR(30) NOT NULL,
    "last_name" VARCHAR(30) NOT NULL,
    "position_id" INTEGER,
    "position_title" VARCHAR(30),
    "store_id" INTEGER NOT NULL,
    "department_id" INTEGER NOT NULL,
    "birth_date" DATE NOT NULL,
    "hire_date" TIMESTAMP,
    "end_date" TIMESTAMP,
    "salary" DECIMAL(10,4) NOT NULL,
    "supervisor_id" INTEGER,
    "education_level" VARCHAR(30) NOT NULL,
    "marital_status" VARCHAR(30) NOT NULL,
    "gender" VARCHAR(30) NOT NULL,
    "management_role" VARCHAR(30));
DROP TABLE "employee_closure";
CREATE TABLE "employee_closure"(
    "employee_id" INTEGER NOT NULL,
    "supervisor_id" INTEGER NOT NULL,
    "distance" INTEGER);
DROP TABLE "expense_fact";
CREATE TABLE "expense_fact"(
    "store_id" INTEGER NOT NULL,
    "account_id" INTEGER NOT NULL,
    "exp_date" TIMESTAMP NOT NULL,
    "time_id" INTEGER NOT NULL,
    "category_id" VARCHAR(30) NOT NULL,
    "currency_id" INTEGER NOT NULL,
    "amount" DECIMAL(10,4) NOT NULL);
DROP TABLE "position";
CREATE TABLE "position"(
    "position_id" INTEGER NOT NULL,
    "position_title" VARCHAR(30) NOT NULL,
    "pay_type" VARCHAR(30) NOT NULL,
    "min_scale" DECIMAL(10,4) NOT NULL,
    "max_scale" DECIMAL(10,4) NOT NULL,
    "management_role" VARCHAR(30) NOT NULL);
DROP TABLE "product";
CREATE TABLE "product"(
    "product_class_id" INTEGER NOT NULL,
    "product_id" INTEGER NOT NULL,
    "brand_name" VARCHAR(60),
    "product_name" VARCHAR(60) NOT NULL,
    "SKU" BIGINT NOT NULL,
    "SRP" DECIMAL(10,4),
    "gross_weight" REAL,
    "net_weight" REAL,
    "recyclable_package" SMALLINT,
    "low_fat" SMALLINT,
    "units_per_case" SMALLINT,
    "cases_per_pallet" SMALLINT,
    "shelf_width" REAL,
    "shelf_height" REAL,
    "shelf_depth" REAL);
DROP TABLE "product_class";
CREATE TABLE "product_class"(
    "product_class_id" INTEGER NOT NULL,
    "product_subcategory" VARCHAR(30),
    "product_category" VARCHAR(30),
    "product_department" VARCHAR(30),
    "product_family" VARCHAR(30));
DROP TABLE "promotion";
CREATE TABLE "promotion"(
    "promotion_id" INTEGER NOT NULL,
    "promotion_district_id" INTEGER,
    "promotion_name" VARCHAR(30),
    "media_type" VARCHAR(30),
    "cost" DECIMAL(10,4),
    "start_date" TIMESTAMP,
    "end_date" TIMESTAMP);
DROP TABLE "region";
CREATE TABLE "region"(
    "region_id" INTEGER NOT NULL,
    "sales_city" VARCHAR(30),
    "sales_state_province" VARCHAR(30),
    "sales_district" VARCHAR(30),
    "sales_region" VARCHAR(30),
    "sales_country" VARCHAR(30),
    "sales_district_id" INTEGER);
DROP TABLE "reserve_employee";
CREATE TABLE "reserve_employee"(
    "employee_id" INTEGER NOT NULL,
    "full_name" VARCHAR(30) NOT NULL,
    "first_name" VARCHAR(30) NOT NULL,
    "last_name" VARCHAR(30) NOT NULL,
    "position_id" INTEGER,
    "position_title" VARCHAR(30),
    "store_id" INTEGER NOT NULL,
    "department_id" INTEGER NOT NULL,
    "birth_date" TIMESTAMP NOT NULL,
    "hire_date" TIMESTAMP,
    "end_date" TIMESTAMP,
    "salary" DECIMAL(10,4) NOT NULL,
    "supervisor_id" INTEGER,
    "education_level" VARCHAR(30) NOT NULL,
    "marital_status" VARCHAR(30) NOT NULL,
    "gender" VARCHAR(30) NOT NULL);
DROP TABLE "salary";
CREATE TABLE "salary"(
    "pay_date" TIMESTAMP NOT NULL,
    "employee_id" INTEGER NOT NULL,
    "department_id" INTEGER NOT NULL,
    "currency_id" INTEGER NOT NULL,
    "salary_paid" DECIMAL(10,4) NOT NULL,
    "overtime_paid" DECIMAL(10,4) NOT NULL,
    "vacation_accrued" REAL NOT NULL,
    "vacation_used" REAL NOT NULL);
DROP TABLE "store";
CREATE TABLE "store"(
    "store_id" INTEGER NOT NULL,
    "store_type" VARCHAR(30),
    "region_id" INTEGER,
    "store_name" VARCHAR(30),
    "store_number" INTEGER,
    "store_street_address" VARCHAR(30),
    "store_city" VARCHAR(30),
    "store_state" VARCHAR(30),
    "store_postal_code" VARCHAR(30),
    "store_country" VARCHAR(30),
    "store_manager" VARCHAR(30),
    "store_phone" VARCHAR(30),
    "store_fax" VARCHAR(30),
    "first_opened_date" TIMESTAMP,
    "last_remodel_date" TIMESTAMP,
    "store_sqft" INTEGER,
    "grocery_sqft" INTEGER,
    "frozen_sqft" INTEGER,
    "meat_sqft" INTEGER,
    "coffee_bar" SMALLINT,
    "video_store" SMALLINT,
    "salad_bar" SMALLINT,
    "prepared_food" SMALLINT,
    "florist" SMALLINT);
DROP TABLE "store_ragged";
CREATE TABLE "store_ragged"(
    "store_id" INTEGER NOT NULL,
    "store_type" VARCHAR(30),
    "region_id" INTEGER,
    "store_name" VARCHAR(30),
    "store_number" INTEGER,
    "store_street_address" VARCHAR(30),
    "store_city" VARCHAR(30),
    "store_state" VARCHAR(30),
    "store_postal_code" VARCHAR(30),
    "store_country" VARCHAR(30),
    "store_manager" VARCHAR(30),
    "store_phone" VARCHAR(30),
    "store_fax" VARCHAR(30),
    "first_opened_date" TIMESTAMP,
    "last_remodel_date" TIMESTAMP,
    "store_sqft" INTEGER,
    "grocery_sqft" INTEGER,
    "frozen_sqft" INTEGER,
    "meat_sqft" INTEGER,
    "coffee_bar" SMALLINT,
    "video_store" SMALLINT,
    "salad_bar" SMALLINT,
    "prepared_food" SMALLINT,
    "florist" SMALLINT);
DROP TABLE "time_by_day";
CREATE TABLE "time_by_day"(
    "time_id" INTEGER NOT NULL,
    "the_date" TIMESTAMP,
    "the_day" VARCHAR(30),
    "the_month" VARCHAR(30),
    "the_year" SMALLINT,
    "day_of_month" SMALLINT,
    "week_of_year" INTEGER,
    "month_of_year" SMALLINT,
    "quarter" VARCHAR(30),
    "fiscal_period" VARCHAR(30));
DROP TABLE "warehouse";
CREATE TABLE "warehouse"(
    "warehouse_id" INTEGER NOT NULL,
    "warehouse_class_id" INTEGER,
    "stores_id" INTEGER,
    "warehouse_name" VARCHAR(60),
    "wa_address1" VARCHAR(30),
    "wa_address2" VARCHAR(30),
    "wa_address3" VARCHAR(30),
    "wa_address4" VARCHAR(30),
    "warehouse_city" VARCHAR(30),
    "warehouse_state_province" VARCHAR(30),
    "warehouse_postal_code" VARCHAR(30),
    "warehouse_country" VARCHAR(30),
    "warehouse_owner_name" VARCHAR(30),
    "warehouse_phone" VARCHAR(30),
    "warehouse_fax" VARCHAR(30));
DROP TABLE "warehouse_class";
CREATE TABLE "warehouse_class"(
    "warehouse_class_id" INTEGER NOT NULL,
    "description" VARCHAR(30));
