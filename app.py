import streamlit as st  
from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.cortex import Complete
from snowflake.snowpark.functions import col
import pandas as pd
import json

pd.set_option("max_colwidth", None)

### Default Values
NUM_CHUNKS = 3  # Num-chunks provided as context. Play with this to check how it affects your accuracy
SLIDE_WINDOW = 7  # how many last conversations to remember. This is the slide window.

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

# Load styling and Kellogg AI logo
def add_custom_styles():
    st.markdown(
        """
        <style>
            .main {
                background: linear-gradient(to bottom, #ffcccc, #ff4d4d, #ffffff);
                color: black;
            }
            .stSidebar {
                background: linear-gradient(to bottom, #ffcccc, #ff4d4d, #ffffff);
            }
            /* Additional styling */
        </style>
        """, unsafe_allow_html=True
    )
    
def add_logo():
    st.markdown(
        "<div class='fixed-header'><h2>Ask KAI!</h2></div>", unsafe_allow_html=True
    )
add_custom_styles()
add_logo()

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


if 'root' not in st.session_state:
    st.session_state['root'] = Root(st.session_state['session'])
    st.session_state['svc'] = st.session_state['root'].databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
    
svc = st.session_state['svc']



# Run the SQL setup script
def run_sql_file(file_path):
    st.write(f"Loading SQL file: {file_path}")
    with open(file_path, 'r') as file:
        sql_commands = file.read().split(';')
        for i, command in enumerate(sql_commands):
            command = command.strip()
            if command:
                try:
                    st.write(f"Executing SQL command {i+1}: {command[:100]}...")
                    st.session_state['session'].sql(command).collect()  # Use session from session_state
                    st.write(f"SQL command {i+1} executed successfully.")
                except Exception as e:
                    st.error(f"Error executing SQL command {i+1}: {command[:100]}...")
                    st.error(f"Exception: {e}")
                    break
    st.write("Finished executing all SQL commands.")


# Ensure the session is using the correct database and schema
session.sql("USE DATABASE CC_QUICKSTART_CORTEX_SEARCH_DOCS").collect()
session.sql("USE SCHEMA DATA").collect()

# Run the SQL setup script
run_sql_file('sql/setup_snowflakecortex.sql')

### Functions

def config_options():
    st.sidebar.selectbox('Select your model:', (
        'mixtral-8x7b', 'snowflake-arctic', 'mistral-large', 'llama3-8b', 'llama3-70b', 'reka-flash', 'mistral-7b', 'llama2-70b-chat', 'gemma-7b'), key="model_name")
    categories = session.table('docs_chunks_table').select('category').distinct().collect()
    cat_list = ['ALL'] + [cat.CATEGORY for cat in categories]
    st.sidebar.selectbox('Select product category', cat_list, key="category_value")
    st.sidebar.checkbox('Remember chat history?', key="use_chat_history", value=True)
    st.sidebar.checkbox('Show debug info', key="debug", value=True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
    st.sidebar.expander("Session State").write(st.session_state)

def init_messages():
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.show_welcome_message = True
    else:
        st.session_state.show_welcome_message = False

def get_similar_chunks_search_service(query):
    if st.session_state.category_value == "ALL":
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    else:
        filter_obj = {"@eq": {"category": st.session_state.category_value}}
        response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)
    st.sidebar.json(response.json())
    return response.json()

# Summarize chat history with the current question
def summarize_question_with_history(chat_history, question):
    prompt = f"<chat_history>{chat_history}</chat_history><question>{question}</question>"
    return Complete(model = st.session_state.model_name, prompt = prompt, session=session)

# Create a prompt for the assistant
def create_prompt(myquestion):
    chat_history = get_chat_history() if st.session_state.use_chat_history else []
    if chat_history:
        question_summary = summarize_question_with_history(chat_history, myquestion)
        prompt_context = get_similar_chunks_search_service(question_summary)
    else:
        prompt_context = get_similar_chunks_search_service(myquestion)
    prompt = f"<context>{prompt_context}</context><question>{myquestion}</question>Answer:"
    return prompt, json.loads(prompt_context)['results']

# Answer the question using the assistant
def answer_question(myquestion):
    prompt, relative_paths = create_prompt(myquestion)
    response = Complete(st.session_state.model_name, prompt, session=session)
    return response, relative_paths

# Get chat history
def get_chat_history():
    start_index = max(0, len(st.session_state.messages) - SLIDE_WINDOW)
    return [msg["content"] for msg in st.session_state.messages[start_index:]]
# Main function
def main():
    # Lists available documents
    # docs_available = st.session.sql("SELECT relative_path, file_url FROM docs_chunks_table").collect()
    # list_docs = [doc["RELATIVE_PATH"] for doc in docs_available]
    
    # st.write("Available Documents:")
    # st.dataframe(list_docs)

    config_options()
    init_messages()
    
    if st.session_state.show_welcome_message:
        st.markdown(
            """
            <div style='text-align:center;'>
                <h1>Hi! I am Kai, your Cereal Industry Analysis Tool</h1>
                <p>Type a question or select one from below to begin.</p>
            </div>
            """, unsafe_allow_html=True
        )
    
    if prompt := st.chat_input("Ask a question:"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        answer, relative_paths = answer_question(prompt)
        with st.chat_message("assistant"):
            st.markdown(answer)
        
        st.session_state.messages.append({"role": "assistant", "content": answer})
    
    # Show chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# Run the app
if __name__ == "__main__":
    main()