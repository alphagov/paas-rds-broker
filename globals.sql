--
-- PostgreSQL database cluster dump
--

SET default_transaction_read_only = off;

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Roles
--

CREATE ROLE "UpCHB6aPJ9VVRBsn";
ALTER ROLE "UpCHB6aPJ9VVRBsn" WITH NOSUPERUSER INHERIT CREATEROLE CREATEDB LOGIN NOREPLICATION NOBYPASSRLS VALID UNTIL 'infinity';
CREATE ROLE rds_ad;
ALTER ROLE rds_ad WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB NOLOGIN NOREPLICATION NOBYPASSRLS;
CREATE ROLE rds_iam;
ALTER ROLE rds_iam WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB NOLOGIN NOREPLICATION NOBYPASSRLS;
CREATE ROLE rds_password;
ALTER ROLE rds_password WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB NOLOGIN NOREPLICATION NOBYPASSRLS;
CREATE ROLE rds_replication;
ALTER ROLE rds_replication WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB NOLOGIN NOREPLICATION NOBYPASSRLS;
CREATE ROLE rds_superuser;
ALTER ROLE rds_superuser WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB NOLOGIN NOREPLICATION NOBYPASSRLS;
CREATE ROLE rdsadmin;
ALTER ROLE rdsadmin WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS VALID UNTIL 'infinity';
CREATE ROLE rdsrepladmin;
ALTER ROLE rdsrepladmin WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN REPLICATION NOBYPASSRLS;
ALTER ROLE rdsadmin SET "TimeZone" TO 'utc';
ALTER ROLE rdsadmin SET log_statement TO 'all';
ALTER ROLE rdsadmin SET log_min_error_statement TO 'debug5';
ALTER ROLE rdsadmin SET log_min_messages TO 'panic';
ALTER ROLE rdsadmin SET exit_on_error TO '0';
ALTER ROLE rdsadmin SET statement_timeout TO '0';
ALTER ROLE rdsadmin SET role TO 'rdsadmin';
ALTER ROLE rdsadmin SET "auto_explain.log_min_duration" TO '-1';
ALTER ROLE rdsadmin SET temp_file_limit TO '-1';
ALTER ROLE rdsadmin SET search_path TO 'pg_catalog', 'public';
ALTER ROLE rdsadmin SET "pg_hint_plan.enable_hint" TO 'off';
ALTER ROLE rdsadmin SET default_transaction_read_only TO 'off';


--
-- Role memberships
--

GRANT pg_monitor TO rds_superuser WITH ADMIN OPTION GRANTED BY rdsadmin;
GRANT pg_signal_backend TO rds_superuser WITH ADMIN OPTION GRANTED BY rdsadmin;
GRANT rds_password TO rds_superuser WITH ADMIN OPTION GRANTED BY rdsadmin;
GRANT rds_replication TO rds_superuser WITH ADMIN OPTION GRANTED BY rdsadmin;
GRANT rds_superuser TO "UpCHB6aPJ9VVRBsn" GRANTED BY rdsadmin;




--
-- Per-Database Role Settings
--

ALTER ROLE rdsadmin IN DATABASE rdsadmin SET log_min_messages TO 'panic';


--
-- PostgreSQL database cluster dump complete
--




