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
        .fixed-header {
            position: fixed;
            top: 0; width: 100%;
            background-color: #BA0144; z-index: 999; padding: 10px 0;
            display: flex; justify-content: center; align-items: center;
        }
        .fixed-header h2 { color: white; font-size: 32px; font-weight: bold; }
        .content { margin-top: 80px; padding: 20px; }
        .st-chat-message { padding: 15px; border-radius: 15px; font-size: 18px; }
        .st-chat-message.user { background-color: #f0f0f0; color: black; text-align: right; }
        .st-chat-message.assistant { background-color: #ff4d4d; color: white; }
        .stButton > button { background-color: #ff0000; color: white; border-radius: 12px; }
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


root = Root(session) 
svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

# Run the SQL setup script
def run_sql_file(file_path):
    st.write(f"Loading SQL file: {file_path}")  # Inform the user that the file is being loaded
    with open(file_path, 'r') as file:
        sql_commands = file.read().split(';')
        for i, command in enumerate(sql_commands):
            command = command.strip()
            if command:
                try:
                    st.write(f"Executing SQL command {i+1}: {command[:100]}...")  # Log the SQL command being executed (first 100 characters)
                    session.sql(command).collect()  # Execute the SQL command
                    st.write(f"SQL command {i+1} executed successfully.")  # Success message
                except Exception as e:
                    st.error(f"Error executing SQL command {i+1}: {command[:100]}...")
                    st.error(f"Exception: {e}")  # Display the exception for debugging
                    break  # Stop further execution if an error occurs
    st.write("Finished executing all SQL commands.")

# Ensure the session is using the correct database and schema
session.sql("USE DATABASE CC_QUICKSTART_CORTEX_SEARCH_DOCS").collect()
session.sql("USE SCHEMA DATA").collect()

# Run the SQL setup script
run_sql_file('sql/setup_snowflakecortex.sql')


root = Root(session) 

svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

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

# def config_options():
#     st.sidebar.selectbox('Select your model:', (
#         'mixtral-8x7b',
#         'snowflake-arctic',
#         'mistral-large',
#         'llama3-8b',
#         'llama3-70b',
#         'reka-flash',
#         'mistral-7b',
#         'llama2-70b-chat',
#         'gemma-7b'), key="model_name")

#     categories = session.table('docs_chunks_table').select('category').distinct().collect()

#     cat_list = ['ALL']
#     for cat in categories:
#         cat_list.append(cat.CATEGORY)

#     st.sidebar.selectbox('Select what products you are looking for', cat_list, key="category_value")
#     st.sidebar.checkbox('Do you want that I remember the chat history?', key="use_chat_history", value=True)
#     st.sidebar.checkbox('Debug: Click to see summary generated of previous conversation', key="debug", value=True)
#     st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
#     st.sidebar.expander("Session State").write(st.session_state)

# def init_messages():
#     # Initialize chat history
#     if st.session_state.clear_conversation or "messages" not in st.session_state:
#         st.session_state.messages = []


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
    return Complete(st.session_state.model_name, prompt)

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
    response = Complete(st.session_state.model_name, prompt)
    return response, relative_paths

# Get chat history
def get_chat_history():
    start_index = max(0, len(st.session_state.messages) - SLIDE_WINDOW)
    return [msg["content"] for msg in st.session_state.messages[start_index:]]

# def get_similar_chunks_search_service(query):
#     if st.session_state.category_value == "ALL":
#         response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
#     else:
#         filter_obj = {"@eq": {"category": st.session_state.category_value}}
#         response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)

#     st.sidebar.json(response.json())
#     return response.json()

# def get_chat_history():
#     chat_history = []
#     start_index = max(0, len(st.session_state.messages) - slide_window)
#     for i in range(start_index, len(st.session_state.messages) - 1):
#         chat_history.append(st.session_state.messages[i])
#     return chat_history

# def summarize_question_with_history(chat_history, question):
#     prompt = f"""
#         Based on the chat history below and the question, generate a query that extends the question
#         with the chat history provided. The query should be in natural language.
#         Answer with only the query. Do not add any explanation.

#         <chat_history>
#         {chat_history}
#         </chat_history>
#         <question>
#         {question}
#         </question>
#     """

#     try:
#         # Use the Complete function in the Snowflake session
#         model = st.session_state.model_name
#         options = None  # Add any options you need for the Complete function
        
#         # Execute the Complete function in the Snowflake session
#         result_df = session.table_function(Complete(model, prompt, options=options, session=session))
        
#         # Get the result from the DataFrame
#         response = result_df.collect()[0]['COMPLETE']

#         if st.session_state.debug:
#             st.sidebar.text("Summary to be used to find similar chunks in the docs:")
#             st.sidebar.caption(response)

#         return response

#     except Exception as e:
#         st.error(f"Error generating response with snowflake.cortex.Complete function: {e}")
#         return ""

# def answer_question(myquestion):
#     prompt, relative_paths = create_prompt(myquestion)

#     try:
#         # Use the Complete function to generate an answer
#         model = st.session_state.model_name
#         options = None  # Specify any necessary options here
        
#         # Execute the Complete function in the Snowflake session
#         result_df = session.table_function(Complete(model, prompt, options=options, session=session))
        
#         # Get the result from the DataFrame
#         response = result_df.collect()[0]['COMPLETE']
        
#         return response, relative_paths
#     except Exception as e:
#         st.error(f"Error generating answer with snowflake.cortex.Complete function: {e}")
#         return "", relative_paths

# def answer_question(myquestion):
#     prompt, relative_paths = create_prompt(myquestion)

#     try:
#         # Use the Complete function to generate an answer
#         model = st.session_state.model_name
#         options = None  # Specify any necessary options here
        
#         # Pass the active session to the Complete function
#         response = Complete(model, prompt, options=options, session=session)
        
#         # Execute the response and collect results
#         result_df = session.table_function(response)
#         result = result_df.collect()[0]['COMPLETE']
        
#         return result, relative_paths
#     except Exception as e:
#         st.error(f"Error generating answer with snowflake.cortex.Complete function: {e}")
#         return "", relative_paths

# def answer_question(myquestion):
#     prompt, relative_paths = create_prompt(myquestion)
#     response = Complete(st.session_state.model_name, prompt)
#     return response, relative_paths




# def create_prompt(myquestion):
#     if st.session_state.use_chat_history:
#         chat_history = get_chat_history()
#         if chat_history != []:  # There is chat history, so not first question
#             question_summary = summarize_question_with_history(chat_history, myquestion)
#             prompt_context = get_similar_chunks_search_service(question_summary)
#         else:
#             prompt_context = get_similar_chunks_search_service(myquestion)  # First question when using history
#     else:
#         prompt_context = get_similar_chunks_search_service(myquestion)
#         chat_history = ""

# def create_prompt(myquestion):
#     if st.session_state.use_chat_history:
#         chat_history = get_chat_history()
#         if chat_history:
#             question_summary = summarize_question_with_history(chat_history, myquestion)
#             prompt_context = get_similar_chunks_search_service(question_summary)
#         else:
#             prompt_context = get_similar_chunks_search_service(myquestion)
#     else:
#         prompt_context = get_similar_chunks_search_service(myquestion)
#     prompt = f"""
#         You are an expert chat assistant that extracts information from the CONTEXT.
#         Only answer if you can extract it from the CONTEXT provided.
#         <context>{prompt_context}</context><question>{myquestion}</question>Answer:
#     """
#     return prompt, json.loads(prompt_context)['results']

# def answer_question(myquestion):
#     prompt, relative_paths = create_prompt(myquestion)
#     response = Complete(st.session_state.model_name, prompt)
#     return response, relative_paths

# def main():
#     st.title(":speech_balloon: Chat Document Assistant with Snowflake Cortex")

#     # Querying the docs_chunks_table instead of ls @docs
#     docs_available = session.sql("SELECT relative_path, file_url FROM docs_chunks_table").collect()
#     list_docs = [doc["RELATIVE_PATH"] for doc in docs_available]
    
#     st.write("Available Documents:")
#     st.dataframe(list_docs)

#     config_options()
#     init_messages()

#     # Further logic for chat input and response...

#     # Display chat messages from history on app rerun
#     for message in st.session_state.messages:
#         with st.chat_message(message["role"]):
#             st.markdown(message["content"])

#     # Accept user input
#     if question := st.chat_input("What do you want to know about your products?"):
#         # Add user message to chat history
#         st.session_state.messages.append({"role": "user", "content": question})
#         print(question)
#         # Display user message in chat message container
#         with st.chat_message("user"):
#             st.markdown(question)
#         # Display assistant response in chat message container
#         with st.chat_message("assistant"):
#             message_placeholder = st.empty()

#             question = question.replace("'", "")

#             with st.spinner(f"{st.session_state.model_name} thinking..."):
#                 response, relative_paths = answer_question(question)
#                 response = response.replace("'", "")
#                 message_placeholder.markdown(response)

#                 if relative_paths != "None":
#                     with st.sidebar.expander("Related Documents"):
#                         for path in relative_paths:
#                             cmd2 = f"select GET_PRESIGNED_URL(@docs, '{path}', 360) as URL_LINK from directory(@docs)"
#                             df_url_link = session.sql(cmd2).to_pandas()
#                             url_link = df_url_link._get_value(0, 'URL_LINK')

#                             display_url = f"Doc: [{path}]({url_link})"
#                             st.sidebar.markdown(display_url)

#         st.session_state.messages.append({"role": "assistant", "content": response})


# if __name__ == "__main__":
#     main()

# Main function
def main():
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