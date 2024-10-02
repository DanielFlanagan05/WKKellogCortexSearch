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

class pdf_text_chunker:
    def read_pdf(self, file_url: str) -> str:
        logger = logging.getLogger("udf_logger")
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
CREATE OR REPLACE TABLE IF NOT EXISTS DOCS_CHUNKS_TABLE (
    RELATIVE_PATH VARCHAR(16777216),
    SIZE NUMBER(38,0),
    FILE_URL VARCHAR(16777216),
    SCOPED_FILE_URL VARCHAR(16777216),
    CHUNK VARCHAR(16777216),
    CATEGORY VARCHAR(16777216)
);
