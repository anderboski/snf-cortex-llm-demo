-- Upload data

-- Grants
use role accountadmin;
grant ownership on table RAW_RESULTS_DATA TO role sysadmin;
grant ownership on table DIM_SCIENTISTS TO role sysadmin;

-- Slight changes from source data
ALTER TABLE DIM_SCIENTISTS RENAME COLUMN SCIENTIST_ID TO ID_SCIENTIST;

SELECT * FROM RAW_RESULTS_DATA where language = 'ES';
UPDATE RAW_RESULTS_DATA
SET LANGUAGE ='EN'
WHERE ID_EXPERIMENT=10;

-- Use role SYSADMIN (lways use PoLP)

use role SYSADMIN;
USE WAREHOUSE WH_CORTEX;

-- Check available data

SELECT * FROM RAW_RESULTS_DATA;
SELECT * FROM DIM_SCIENTIST;

-- Create auxiliary table for translation capability
CREATE TABLE DIM_LANGUAGE AS
select 'EN' SYSTEM, 'en' CORTEX union all
select 'ES' SYSTEM, 'es' CORTEX union all
select 'IT' SYSTEM, 'it' CORTEX;

select * from DIM_LANGUAGE;

-- Translate non-english phrases to english
CREATE OR REPLACE TABLE WRK_RESULT_DATA_LANG AS
SELECT a.*,
CASE WHEN LANGUAGE <> 'EN' AND A.RESULT <> '' AND A.RESULT IS NOT NULL THEN SNOWFLAKE.CORTEX.TRANSLATE(a.RESULT, b.cortex, 'en') else a.RESULT END RESULT_ENGLISH
FROM RAW_RESULTS_DATA a
inner join DIM_LANGUAGE b
ON a.LANGUAGE = b.SYSTEM;

SELECT * FROM WRK_RESULT_DATA_LANG WHERE LANGUAGE <>'EN'

-- Find if results are stored in a separate file
CREATE OR REPLACE TABLE WRK_RESULT_DATA_ATTACH AS
SELECT *,
CASE WHEN result_english <> '' AND result_english IS NOT NULL THEN SNOWFLAKE.CORTEX.TRY_COMPLETE('llama3.1-8b',  'This is the description of an experiment result: "' || result_english || '". Only return TRUE/FALSE values. TRUE if an attachment is referenced (ppt, word, file location, etc.), FALSE else.') ELSE '' END AS ATTACHMENT
FROM WRK_RESULT_DATA_LANG;

SELECT SNOWFLAKE.CORTEX.TRY_COMPLETE(
    'llama3.1-8b','Return a random number between 3 and 9'
);

CREATE OR REPLACE TABLE WRK_RESULT_DATA_ATTACH2 AS
SELECT *,
REGEXP_COUNT(result_english, '\\.pdf|\\.docx|\\.ppt|sharepoint', 1,'i') > 0 ATTACHMENT_REGEX
FROM WRK_RESULT_DATA_ATTACH;

-- Match rate
select 
SUM(CASE WHEN lower(ATTACHMENT) = ATTACHMENT_REGEX::varchar THEN 1 ELSE 0 END) AS COUNT_MATCH,
COUNT(1) COUNT_TOTAL,
COUNT_MATCH/COUNT_TOTAL MATCH_RATE
from WRK_RESULT_DATA_ATTACH2;

select id_experiment,result_english,attachment attach_cortex, attachment_regex from WRK_RESULT_DATA_ATTACH2 
where lower(attachment) <> attachment_regex::varchar;

-- Extract chemicals
CREATE OR REPLACE TABLE WKR_RESULT_DATA_LANG_CHEM AS
SELECT *,
CASE WHEN result_english <> '' AND result_english IS NOT NULL THEN SNOWFLAKE.CORTEX.TRY_COMPLETE('llama3.1-8b',  'Extract all available chemical formulations from the following text: "' || result_english || '". Your response should be only a JSON object such as ["chemical1","chemical2", ...] with no further text') ELSE '' END AS CHEMICALS
from WRK_RESULT_DATA_LANG;

-- Extract chemicals with stronger model
CREATE OR REPLACE TABLE WKR_RESULT_DATA_LANG_CHEM2 AS
SELECT *,
CASE WHEN result_english <> '' AND result_english IS NOT NULL THEN SNOWFLAKE.CORTEX.TRY_COMPLETE('llama3.1-70b',  'Extract all available chemical formulations from the following text: "' || result_english || '". Your response should be only a JSON object such as ["chemical1","chemical2", ...] with no further text') ELSE '' END AS CHEMICALS
from WRK_RESULT_DATA_LANG;

-- Different
select a.id_experiment,a.result_english,a.chemicals,b.chemicals from WKR_RESULT_DATA_LANG_CHEM a
inner join WKR_RESULT_DATA_LANG_CHEM2 b
on a.id_experiment = b.id_experiment
where a.chemicals<>b.chemicals;

-- Create training set
create or replace table TRN_DATASET as
select result_english,'Extract all available chemical formulations from the following text: "' || result_english || '". Your response should be only a JSON object such as ["chemical1","chemical2", ...] with no further text' PROMPT, CHEMICALS as RESPONSE
from WKR_RESULT_DATA_LANG_CHEM2
where id_experiment>200 and result_english is not null;

-- Create Experiment - Chemical relationship view
CREATE OR REPLACE VIEW RLTN_EXPERIMENT_CHEM AS
SELECT DISTINCT ID_EXPERIMENT, b.value::VARCHAR CHEMICAL 
FROM WKR_RESULT_DATA_LANG_CHEM2,
LATERAL FLATTEN(input => try_parse_json(CHEMICALS)) b;

-- Retrieve status of the result
CREATE OR REPLACE TABLE WRK_EXPERIMENT_STATUS AS
SELECT ID_EXPERIMENT,
CASE WHEN result_english <> '' AND result_english IS NOT NULL THEN SNOWFLAKE.CORTEX.TRY_COMPLETE('llama3.1-70b',  'Categorize this experiment result as SUCCESS, FAILURE or INCONCLUSIVE: "' || result_english || '". Response must only be the elected category.') ELSE 'INCONCLUSIVE' END AS STATUS
FROM WRK_RESULT_DATA_LANG;

SELECT STATUS,COUNT(1) FROM WRK_EXPERIMENT_STATUS GROUP BY STATUS;

-- Create Results fact table
CREATE OR REPLACE VIEW FCT_RESULTS AS
SELECT a.ID_EXPERIMENT,b.SCIENTIST_NAME,a.RESULT_ENGLISH as RESULT_TEXT,c.STATUS as EXPERIMENT_STATUS, TRY_TO_BOOLEAN(d.ATTACHMENT) as ATTACHMENT, TRY_TO_BOOLEAN(d.ATTACHMENT_REGEX) as ATTACHMENT_REGEX  FROM WRK_RESULT_DATA_LANG a
INNER JOIN DIM_SCIENTISTS b
ON a.id_scientist = b.id_scientist
INNER JOIN WRK_EXPERIMENT_STATUS c
ON a.id_experiment = c.id_experiment
INNER JOIN WRK_RESULT_DATA_ATTACH2 d
ON a.id_experiment = d.id_experiment;

--
select * from FCT_RESULTS;
select * from RLTN_EXPERIMENT_CHEM;

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
INNER JOIN RLTN_EXPERIMENT_CHEM b
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






