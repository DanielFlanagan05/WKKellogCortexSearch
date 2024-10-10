-- Setting the database and schema context
USE DATABASE CC_QUICKSTART_CORTEX_SEARCH_DOCS;
USE SCHEMA DATA;

-- Creating a PDF text chunker function
CREATE OR REPLACE FUNCTION pdf_text_chunker(file_url STRING)
RETURNS TABLE (chunk VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'pdf_text_chunker'
PACKAGES = ('snowflake-snowpark-python', 'PyPDF2', 'langchain')
AS
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2, io
import logging
import pandas as pd

from PyPDF2.errors import PdfReadError

class pdf_text_chunker:
    def read_pdf(self, file_url: str) -> str:
        logger = logging.getLogger("udf_logger")
        logger.info(f"Opening file {file_url}")
        
        try:
            with SnowflakeFile.open(file_url, 'rb') as f:
                buffer = io.BytesIO(f.readall())
            reader = PyPDF2.PdfReader(buffer)
            text = ""
            for page in reader.pages:
                try:
                    text += page.extract_text().replace('\n', ' ').replace('\0', ' ')
                except Exception as e:
                    logger.warn(f"Unable to extract text from file {file_url}, page {page}: {e}")
                    text = "Unable to Extract"
        except PdfReadError as e:
            logger.error(f"Failed to read PDF file {file_url}: {e}")
            text = "PDF Read Error"
        except Exception as e:
            logger.error(f"Unexpected error while reading PDF file {file_url}: {e}")
            text = "General Error"
        
        return text


    def process(self, file_url: str):
        text = self.read_pdf(file_url)
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1512, chunk_overlap  = 256, length_function = len
        )
        chunks = text_splitter.split_text(text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        yield from df.itertuples(index=False, name=None)
$$;

-- Creating a stage for documents
CREATE STAGE IF NOT EXISTS docs
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
DIRECTORY = (ENABLE = TRUE);

-- Creating a table to store document chunks
CREATE TABLE IF NOT EXISTS DOCS_CHUNKS_TABLE (
    RELATIVE_PATH VARCHAR(16777216),
    SIZE NUMBER(38,0),
    FILE_URL VARCHAR(16777216),
    SCOPED_FILE_URL VARCHAR(16777216),
    CHUNK VARCHAR(16777216),
    CATEGORY VARCHAR(16777216)
);


-- INSERT INTO DOCS_CHUNKS_TABLE (relative_path, size, file_url, scoped_file_url, chunk)
-- SELECT relative_path, 
--        size,
--        file_url, 
--        build_scoped_file_url(@docs, relative_path) AS scoped_file_url,
--        func.chunk AS chunk
-- FROM 
--     directory(@docs),
--     TABLE(pdf_text_chunker(build_scoped_file_url(@docs, relative_path))) AS func;
-- WHERE NOT EXISTS (
--     SELECT 1
--     FROM DOCS_CHUNKS_TABLE dct
--     WHERE dct.relative_path = directory.relative_path
-- );

-- Categorizing documents
CREATE OR REPLACE TEMPORARY TABLE docs_categories AS WITH unique_documents AS (
  SELECT DISTINCT relative_path FROM DOCS_CHUNKS_TABLE
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

UPDATE docs_chunks_table 
SET category = docs_categories.category
FROM docs_categories
WHERE docs_chunks_table.relative_path = docs_categories.relative_path;

-- Creating the Cortex Search Service
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

-- Selecting documents and their URLs
SELECT relative_path, file_url 
FROM docs_chunks_table;