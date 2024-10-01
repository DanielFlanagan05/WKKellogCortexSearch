CREATE DATABASE IF NOT EXISTS CC_QUICKSTART_CORTEX_SEARCH_DOCS;
CREATE SCHEMA IF NOT EXISTS CC_QUICKSTART_CORTEX_SEARCH_DOCS.DATA;

CREATE OR REPLACE FUNCTION pdf_text_chunker(file_url STRING)
RETURNS TABLE (chunk VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'pdf_text_chunker'
PACKAGES = ('snowflake-snowpark-python', 'PyPDF2', 'langchain')
AS
$$
from langchain.text_splitter import RecursiveCharacterTextSplitter
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2, io
import logging
import pandas as pd

class pdf_text_chunker:

    def read_pdf(self, file_url: str) -> str:
        logger is logging.getLogger("udf_logger")
        logger.info(f"Opening file {file_url}")
        with SnowflakeFile.open(file_url, 'rb') as f:
            buffer = io.BytesIO(f.readall())
        reader = PyPDF2.PdfReader(buffer)
        text = ""
        for page in reader.pages:
            try:
                text += page.extract_text().replace('\n', ' ').replace('\0', ' ')
            except:
                text = "Unable to Extract"
                logger.warn(f"Unable to extract from file {file_url}, page {page}")
        return text

    def process(self, file_url: str):
        text = self.read_pdf(file_url)
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1512,
            chunk_overlap=256,
            length_function=len
        )
        chunks = text_splitter.split_text(text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        yield from df.itertuples(index=False, name=None)
$$;


CREATE OR REPLACE STAGE docs 
ENCRYPTION = TYPE = 'SNOWFLAKE_SSE' 
DIRECTORY = ENABLE = TRUE;

CREATE OR REPLACE TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), 
    SIZE NUMBER(38,0), 
    FILE_URL VARCHAR(16777216), 
    SCOPED_FILE_URL VARCHAR(16777216), 
    CHUNK VARCHAR(16777216), 
    CATEGORY VARCHAR(16777216)
);

-- Fixed the alias issue here
INSERT INTO DOCS_CHUNKS_TABLE (relative_path, size, file_url, scoped_file_url, chunk)
    SELECT relative_path, 
           size,
           file_url, 
           build_scoped_file_url(@docs, relative_path),
           chunk
    FROM directory(@docs),
         TABLE(pdf_text_chunker(build_scoped_file_url(@docs, relative_path)));

-- Creating temporary table for categories
CREATE OR REPLACE TEMPORARY TABLE docs_categories AS 
WITH unique_documents AS (
    SELECT DISTINCT relative_path FROM docs_chunks_table
),
docs_category_cte AS (
    SELECT relative_path,
           TRIM(snowflake.cortex.COMPLETE (
             'llama3-70b',
             'Given the name of the file between <file> and </file> determine if it is related to bikes or snow or gdp or equity or income or sales. Use only one word <file> ' || relative_path || '</file>'
           ), '\n') AS category
    FROM unique_documents
)
SELECT * FROM docs_category_cte;

-- Updating categories in main table
UPDATE docs_chunks_table 
SET category = docs_categories.category
FROM docs_categories
WHERE docs_chunks_table.relative_path = docs_categories.relative_path;

-- Cortex Search Service creation
CREATE OR REPLACE CORTEX SEARCH SERVICE CC_SEARCH_SERVICE_CS
ON chunk
ATTRIBUTES category
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 minute'
AS (
    SELECT chunk,
           relative_path,
           file_url,
           category
    FROM docs_chunks_table
);

-- Query the final table
SELECT relative_path, file_url FROM docs_chunks_table;
