import streamlit as st  # Import python packages
from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.cortex import Complete
from snowflake.snowpark.functions import col
import pandas as pd
import json

pd.set_option("max_colwidth", None)

### Default Values
NUM_CHUNKS = 3  # Num-chunks provided as context. Play with this to check how it affects your accuracy
slide_window = 7  # how many last conversations to remember. This is the slide window.

# Service parameters
CORTEX_SEARCH_DATABASE = "CC_QUICKSTART_CORTEX_SEARCH_DOCS"
CORTEX_SEARCH_SCHEMA = "DATA"
CORTEX_SEARCH_SERVICE = "CC_SEARCH_SERVICE_CS"

# Columns to query in the service
COLUMNS = [
    "chunk",
    "relative_path",
    "category"
]

# --- Snowflake connection setup ---
def create_snowflake_session():
    # Fetching Snowflake credentials from Streamlit secrets
    connection_parameters = {
        "account": st.secrets["snowflake"]["account"],
        "user": st.secrets["snowflake"]["user"],
        "password": st.secrets["snowflake"]["password"],
        "authenticator": st.secrets["snowflake"]["authenticator"],
        "database": "CC_QUICKSTART_CORTEX_SEARCH_DOCS",
        "schema": "DATA",
        "role": st.secrets.get("snowflake", {}).get("role", None),
        "warehouse": st.secrets.get("snowflake", {}).get("warehouse", None),
        "schema": st.secrets.get("snowflake", {}).get("schema", None)
    }
    # Creating Snowpark session
    session = Session.builder.configs(connection_parameters).create()
    return session

# Ensures only one session is created and used
if 'session' not in st.session_state:
    st.session_state['session'] = create_snowflake_session()
session = st.session_state['session']

# SQL script to create stage, tables, and functions
sql_script = """
CREATE DATABASE IF NOT EXISTS CC_QUICKSTART_CORTEX_SEARCH_DOCS;
CREATE SCHEMA IF NOT EXISTS DATA;

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

CREATE STAGE IF NOT EXISTS docs
STORAGE_INTEGRATION = 'SNOWFLAKE_SSE'
DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TABLE IF NOT EXISTS DOCS_CHUNKS_TABLE (
    RELATIVE_PATH VARCHAR(16777216),
    SIZE NUMBER(38,0),
    FILE_URL VARCHAR(16777216),
    SCOPED_FILE_URL VARCHAR(16777216),
    CHUNK VARCHAR(16777216),
    CATEGORY VARCHAR(16777216)
);
"""

# Run the SQL script when the app starts
def run_sql_script():
    sql_commands = [
        "CREATE DATABASE IF NOT EXISTS CC_QUICKSTART_CORTEX_SEARCH_DOCS",
        "USE DATABASE CC_QUICKSTART_CORTEX_SEARCH_DOCS",  # Set the current database
        "USE SCHEMA DATA",  # Set the current schema to DATA
        """
        CREATE STAGE IF NOT EXISTS docs
        STORAGE_INTEGRATION = 'SNOWFLAKE_SSE'
        DIRECTORY = (ENABLE = TRUE);
        """,
        """
        CREATE OR REPLACE TABLE IF NOT EXISTS DOCS_CHUNKS_TABLE (
            RELATIVE_PATH VARCHAR(16777216),
            SIZE NUMBER(38,0),
            FILE_URL VARCHAR(16777216),
            SCOPED_FILE_URL VARCHAR(16777216),
            CHUNK VARCHAR(16777216),
            CATEGORY VARCHAR(16777216)
        )
        """
    ]
    
    for command in sql_commands:
        command = command.strip()
        if command:
            try:
                # Log the SQL command being executed
                st.write(f"Executing SQL command: {command[:100]}...")  # Show first 100 characters for brevity
                session.sql(command).collect()  # Execute the SQL command
            except Exception as e:
                st.error(f"Error executing SQL command: {command[:100]}...")  # Show first 100 characters for brevity
                st.error(f"Exception: {e}")  # Display the exception for debugging
                break  # Stop further execution if an error occurs

# Run the SQL setup
run_sql_script()

root = Root(session) 

svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

### Functions

def config_options():
    st.sidebar.selectbox('Select your model:', (
        'mixtral-8x7b',
        'snowflake-arctic',
        'mistral-large',
        'llama3-8b',
        'llama3-70b',
        'reka-flash',
        'mistral-7b',
        'llama2-70b-chat',
        'gemma-7b'), key="model_name")

    categories = session.table('docs_chunks_table').select('category').distinct().collect()

    cat_list = ['ALL']
    for cat in categories:
        cat_list.append(cat.CATEGORY)

    st.sidebar.selectbox('Select what products you are looking for', cat_list, key="category_value")
    st.sidebar.checkbox('Do you want that I remember the chat history?', key="use_chat_history", value=True)
    st.sidebar.checkbox('Debug: Click to see summary generated of previous conversation', key="debug", value=True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
    st.sidebar.expander("Session State").write(st.session_state)

def init_messages():
    # Initialize chat history
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def get_similar_chunks_search_service(query):
    if st.session_state.category_value == "ALL":
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    else:
        filter_obj = {"@eq": {"category": st.session_state.category_value}}
        response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)

    st.sidebar.json(response.json())
    return response.json()

def get_chat_history():
    chat_history = []
    start_index = max(0, len(st.session_state.messages) - slide_window)
    for i in range(start_index, len(st.session_state.messages) - 1):
        chat_history.append(st.session_state.messages[i])
    return chat_history

def summarize_question_with_history(chat_history, question):
    prompt = f"""
        Based on the chat history below and the question, generate a query that extends the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add any explanation.

        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
    """

    try:
        # Use the Complete function in the Snowflake session
        model = st.session_state.model_name
        options = None  # Add any options you need for the Complete function
        
        # Execute the Complete function in the Snowflake session
        result_df = session.table_function(Complete(model, prompt, options=options, session=session))
        
        # Get the result from the DataFrame
        response = result_df.collect()[0]['COMPLETE']

        if st.session_state.debug:
            st.sidebar.text("Summary to be used to find similar chunks in the docs:")
            st.sidebar.caption(response)

        return response

    except Exception as e:
        st.error(f"Error generating response with snowflake.cortex.Complete function: {e}")
        return ""

def answer_question(myquestion):
    prompt, relative_paths = create_prompt(myquestion)

    try:
        # Use the Complete function to generate an answer
        model = st.session_state.model_name
        options = None  # Specify any necessary options here
        
        # Pass the active session to the Complete function
        response = Complete(model, prompt, options=options, session=session)
        
        # Execute the response and collect results
        result_df = session.table_function(response)
        result = result_df.collect()[0]['COMPLETE']
        
        return result, relative_paths
    except Exception as e:
        st.error(f"Error generating answer with snowflake.cortex.Complete function: {e}")
        return "", relative_paths

def create_prompt(myquestion):
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history != []:  # There is chat history, so not first question
            question_summary = summarize_question_with_history(chat_history, myquestion)
            prompt_context = get_similar_chunks_search_service(question_summary)
        else:
            prompt_context = get_similar_chunks_search_service(myquestion)  # First question when using history
    else:
        prompt_context = get_similar_chunks_search_service(myquestion)
        chat_history = ""

    prompt = f"""
           You are an expert chat assistant that extracts information from the CONTEXT provided
           between <context> and </context> tags.
           You offer a chat experience considering the information included in the CHAT HISTORY
           provided between <chat_history> and </chat_history> tags.
           When answering the question contained between <question> and </question> tags,
           be concise and do not hallucinate. 
           If you don't have the information, just say so.
           
           <chat_history>
           {chat_history}
           </chat_history>
           <context>
           {prompt_context}
           </context>
           <question>
           {myquestion}
           </question>
           Answer:
           """

    json_data = json.loads(prompt_context)
    relative_paths = set(item['relative_path'] for item in json_data['results'])

    return prompt, relative_paths

# def answer_question(myquestion):
#     prompt, relative_paths = create_prompt(myquestion)
#     response = Complete(st.session_state.model_name, prompt)
#     return response, relative_paths

def main():
    st.title(":speech_balloon: Chat Document Assistant with Snowflake Cortex")

    # Querying the docs_chunks_table instead of ls @docs
    docs_available = session.sql("SELECT relative_path, file_url FROM docs_chunks_table").collect()
    list_docs = [doc["RELATIVE_PATH"] for doc in docs_available]
    
    st.write("Available Documents:")
    st.dataframe(list_docs)

    config_options()
    init_messages()

    # Further logic for chat input and response...

    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Accept user input
    if question := st.chat_input("What do you want to know about your products?"):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": question})
        # Display user message in chat message container
        with st.chat_message("user"):
            st.markdown(question)
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            message_placeholder = st.empty()

            question = question.replace("'", "")

            with st.spinner(f"{st.session_state.model_name} thinking..."):
                response, relative_paths = answer_question(question)
                response = response.replace("'", "")
                message_placeholder.markdown(response)

                if relative_paths != "None":
                    with st.sidebar.expander("Related Documents"):
                        for path in relative_paths:
                            cmd2 = f"select GET_PRESIGNED_URL(@docs, '{path}', 360) as URL_LINK from directory(@docs)"
                            df_url_link = session.sql(cmd2).to_pandas()
                            url_link = df_url_link._get_value(0, 'URL_LINK')

                            display_url = f"Doc: [{path}]({url_link})"
                            st.sidebar.markdown(display_url)

        st.session_state.messages.append({"role": "assistant", "content": response})


if __name__ == "__main__":
    main()
