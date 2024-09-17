-- Create DB, schemas

use role sysadmin; -- Remember PoLP
use warehouse wh_cortex;

create database DB_CORTEX_SNOWFLAKE_LIVE;

use database DB_CORTEX_SNOWFLAKE_LIVE;
create schema RAW;
create schema WRK;
create schema PUB;
drop schema public;


-- Upload data

-- Check available data

SELECT DISTINCT TO_NUMERIC(ID_SCIENTIST) FROM RAW.RAW_EXPERIMENT_RESULTS;
SELECT * FROM RAW.RAW_SCIENTISTS;

-- Create auxiliary table for translation capability
CREATE TABLE WRK.WRK_SYSTEM_CORTEX_LANG AS
select 'EN' SYSTEM, 'en' CORTEX union all
select 'ES' SYSTEM, 'es' CORTEX union all
select 'IT' SYSTEM, 'it' CORTEX;

select * from WRK.WRK_SYSTEM_CORTEX_LANG;

-- Translate non-english phrases to english
CREATE OR REPLACE TABLE WRK.WRK_EXPERIMENT_RESULTS_LANG AS
SELECT a.*,
CASE WHEN LANGUAGE <> 'EN' AND A.RESULT <> '' AND A.RESULT IS NOT NULL THEN SNOWFLAKE.CORTEX.TRANSLATE(a.RESULT, b.cortex, 'en') else a.RESULT END RESULT_ENGLISH
FROM RAW.RAW_EXPERIMENT_RESULTS a
inner join WRK.WRK_SYSTEM_CORTEX_LANG b
ON a.LANGUAGE = b.SYSTEM; --


SELECT * FROM WRK.WRK_EXPERIMENT_RESULTS_LANG WHERE LANGUAGE <>'EN'

-- Find if results are stored in a separate file
CREATE OR REPLACE TABLE WRK.WRK_EXPERIMENT_ATTACHMENT AS
SELECT ID_EXPERIMENT,
CASE WHEN result_english <> '' AND result_english IS NOT NULL THEN SNOWFLAKE.CORTEX.TRY_COMPLETE('llama3.1-8b',  'This is the description of an experiment result: "' || result_english || '". Only return TRUE/FALSE boolean values. Return "TRUE" if an attachment or document is referenced, "FALSE" else.') ELSE '' END AS ATTACHMENT
FROM WRK.WRK_EXPERIMENT_RESULTS_LANG ;

select a.id_experiment,a.result_english,b.attachment
from WRK.WRK_EXPERIMENT_RESULTS_LANG a
inner join WRK.WRK_EXPERIMENT_ATTACHMENT b
on a.id_experiment = b.id_experiment;

-- Extract chemicals
CREATE OR REPLACE TABLE WRK.WRK_RESULT_CHEMICALS AS
SELECT ID_EXPERIMENT,
CASE WHEN result_english <> '' AND result_english IS NOT NULL THEN SNOWFLAKE.CORTEX.TRY_COMPLETE('llama3.1-8b',  'Extract all available chemical formulations from the following text: "' || result_english || '". Your response should be only a JSON object such as ["chemical1","chemical2", ...] with no further text') ELSE '' END AS CHEMICALS
from WRK.WRK_EXPERIMENT_RESULTS_LANG;

SELECT a.ID_EXPERIMENT, b.result_english, A.CHEMICALS
FROM WRK.WRK_RESULT_CHEMICALS a
INNER JOIN WRK.WRK_EXPERIMENT_RESULTS_LANG b
ON a.id_experiment = b.ID_EXPERIMENT;

-- Extract chemicals with stronger model
CREATE OR REPLACE TABLE WRK.WRK_RESULT_CHEMICALS2 AS
SELECT ID_EXPERIMENT,
CASE WHEN result_english <> '' AND result_english IS NOT NULL THEN SNOWFLAKE.CORTEX.TRY_COMPLETE('llama3.1-70b',  'Extract all available chemical formulations from the following text: "' || result_english || '". Your response should be only a JSON object such as ["chemical1","chemical2", ...] with no further text') ELSE '' END AS CHEMICALS
from WRK.WRK_EXPERIMENT_RESULTS_LANG;

SELECT a.ID_EXPERIMENT, b.result_english, A.CHEMICALS, c.CHEMICALS CHEMICALS2
FROM WRK.WRK_RESULT_CHEMICALS a
INNER JOIN WRK.WRK_EXPERIMENT_RESULTS_LANG b
ON a.id_experiment = b.ID_EXPERIMENT
INNER JOIN WRK.WRK_RESULT_CHEMICALS2 c
ON a.ID_EXPERIMENT = c.ID_EXPERIMENT;



-- Create training set
create or replace table WRK.TRN_DATASET as
select b.result_english,'Extract all available chemical formulations from the following text: "' || b.result_english || '". Your response should be only a JSON object such as ["chemical1","chemical2", ...] with no further text' PROMPT, CHEMICALS as RESPONSE
FROM WRK.WRK_RESULT_CHEMICALS a
INNER JOIN WRK.WRK_EXPERIMENT_RESULTS_LANG b
ON a.id_experiment = b.ID_EXPERIMENT
where a.id_experiment>200 and b.result_english is not null;

SELECT * FROM WRK.TRN_DATASET;

-- Fine tune in Studio

-- LLAMA38B_FINETUNED

-- Extract chemicals with finetuned model
CREATE OR REPLACE TABLE WRK.WRK_RESULT_CHEMICALS3 AS
SELECT ID_EXPERIMENT,
CASE WHEN result_english <> '' AND result_english IS NOT NULL THEN SNOWFLAKE.CORTEX.COMPLETE('DB_CORTEX.SC_CORTEX.LLAMA38B_FINETUNED',  'Extract all available chemical formulations from the following text: "' || result_english || '". Your response should be only a JSON object such as ["chemical1","chemical2", ...] with no further text') ELSE '' END AS CHEMICALS
from WRK.WRK_EXPERIMENT_RESULTS_LANG;

-- Compare all models
SELECT a.ID_EXPERIMENT, b.result_english, A.CHEMICALS, c.CHEMICALS CHEMICALS2, d.CHEMICALS CHEMICALS3
FROM WRK.WRK_RESULT_CHEMICALS a
INNER JOIN WRK.WRK_EXPERIMENT_RESULTS_LANG b
ON a.id_experiment = b.ID_EXPERIMENT
INNER JOIN WRK.WRK_RESULT_CHEMICALS2 c
ON a.ID_EXPERIMENT = c.ID_EXPERIMENT
INNER JOIN WRK.WRK_RESULT_CHEMICALS3 d
ON a.ID_EXPERIMENT = d.ID_EXPERIMENT;

-- Retrieve status of the result
CREATE OR REPLACE TABLE WRK.WRK_EXPERIMENT_STATUS AS
SELECT ID_EXPERIMENT,
CASE WHEN result_english <> '' AND result_english IS NOT NULL THEN SNOWFLAKE.CORTEX.TRY_COMPLETE('llama3.1-70b',  'Categorize this experiment result as SUCCESS, FAILURE or INCONCLUSIVE: "' || result_english || '". Response must only be the elected category.') ELSE 'INCONCLUSIVE' END AS STATUS
FROM WRK.WRK_EXPERIMENT_RESULTS_LANG;

SELECT STATUS,COUNT(1) FROM WRK.WRK_EXPERIMENT_STATUS GROUP BY STATUS;

-- Create Experiment - Chemical relationship view
CREATE OR REPLACE VIEW PUB.RLTN_EXPERIMENT_CHEMICAL AS
SELECT DISTINCT ID_EXPERIMENT, b.value::VARCHAR CHEMICAL 
FROM WRK.WRK_RESULT_CHEMICALS2,
LATERAL FLATTEN(input => try_parse_json(CHEMICALS)) b;

-- Create scientist dimension
CREATE OR REPLACE VIEW PUB.DIM_SCIENTIST AS
SELECT * FROM RAW.RAW_SCIENTISTS;

-- Create Results fact table
CREATE OR REPLACE VIEW PUB.FCT_RESULTS AS
SELECT a.ID_EXPERIMENT,b.SCIENTIST_NAME,a.RESULT_ENGLISH as RESULT_TEXT,c.STATUS as EXPERIMENT_STATUS, TRY_TO_BOOLEAN(d.ATTACHMENT) as ATTACHMENT
SELECT COUNT(1)
FROM WRK.WRK_EXPERIMENT_RESULTS_LANG a
INNER JOIN PUB.DIM_SCIENTIST b
ON a.id_scientist = b.id_scientist
INNER JOIN WRK.WRK_EXPERIMENT_STATUS c
ON a.id_experiment = c.id_experiment
INNER JOIN WRK.WRK_EXPERIMENT_ATTACHMENT d
ON a.id_experiment = d.id_experiment;

--

use schema PUB;
select * from FCT_RESULTS;
select * from RLTN_EXPERIMENT_CHEMICAL;

-- Business questions

-- What is the success rate of experiments?
SELECT 
SUM(CASE WHEN EXPERIMENT_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS COUNT_SUCCESS,
SUM(CASE WHEN EXPERIMENT_STATUS = 'INCONCLUSIVE' THEN 1 ELSE 0 END) AS COUNT_INCONCLUSIVE,
SUM(CASE WHEN EXPERIMENT_STATUS = 'FAILURE' THEN 1 ELSE 0 END) AS COUNT_FAILURE,
ROUND((COUNT_SUCCESS/COUNT(1))*100,2) SUCCESS_RATE,
ROUND((COUNT_SUCCESS/(COUNT_SUCCESS + COUNT_FAILURE))*100,2) SUCCESS_RATE_CONCLUSIVE
FROM FCT_RESULTS;

-- What chemicals yield the most nº of successful experiments?
SELECT TOP 15 b.chemical,
SUM(CASE WHEN EXPERIMENT_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS COUNT_SUCCESS,
FROM FCT_RESULTS a
INNER JOIN RLTN_EXPERIMENT_CHEMICAL b
ON a.id_experiment = b.id_experiment
GROUP BY b.chemical
ORDER BY COUNT_SUCCESS DESC;

-- Which results are storing information in files / external sources?

SELECT *
FROM FCT_RESULTS
WHERE ATTACHMENT;

-- What users are not being compliant the most?

select TOP 10 SCIENTIST_NAME, COUNT(1) NUM_NONCOMPLIANT
FROM FCT_RESULTS
WHERE RESULT_TEXT IS NULL
OR RESULT_TEXT = ''
OR ATTACHMENT
GROUP BY SCIENTIST_NAME
ORDER BY COUNT(1) DESC;

-- Usage

use role accountadmin;
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY WHERE SERVICE_TYPE='AI_SERVICES'; 
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY; 
-- No query - token correlation






