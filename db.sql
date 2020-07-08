--
-- PostgreSQL database dump
--

-- Dumped from database version 10.11
-- Dumped by pg_dump version 10.13 (Ubuntu 10.13-1.pgdg16.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: citext; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;


--
-- Name: EXTENSION citext; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION citext IS 'data type for case-insensitive character strings';


--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: 
--

-- CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

-- COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- Data for Name: spatial_ref_sys; Type: TABLE DATA; Schema: public; Owner: rdsadmin
--

-- COPY public.spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) FROM stdin;
-- \.


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: UpCHB6aPJ9VVRBsn
--

REVOKE ALL ON SCHEMA public FROM rdsadmin;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO "UpCHB6aPJ9VVRBsn";
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

--
-- SET Passowrds for uploaded users
--

ALTER USER "UpCHB6aPJ9VVRBsn" WITH PASSWORD 'secret';
ALTER USER "rdsadmin" WITH PASSWORD 'secret';

--
-- CHANING the tablespace owner
--
ALTER TABLESPACE pg_default OWNER TO rdsadmin;
ALTER TABLESPACE pg_global OWNER TO rdsadmin;

--
-- CHANGING the SCHEMA OWNER
--
--ALTER SCHEMA public OWNER TO "UpCHB6aPJ9VVRBsn";

ALTER SCHEMA public OWNER TO rdsadmin;

--
-- CHANGING the DBs owner
--

ALTER DATABASE pgdb OWNER TO "UpCHB6aPJ9VVRBsn";
ALTER DATABASE rdsadmin OWNER TO rdsadmin;
ALTER DATABASE postgres OWNER TO "UpCHB6aPJ9VVRBsn";
ALTER DATABASE template1 OWNER TO "UpCHB6aPJ9VVRBsn";
ALTER DATABASE template0 OWNER TO rdsadmin;
