select * from "example"."products";
insert into "results"."results" select * from "example"."products" where SKU is not null;
