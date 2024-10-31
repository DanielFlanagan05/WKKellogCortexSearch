from typing import Literal
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.cortex import Complete
from snowflake.core import Root
import bcrypt
import json
import pandas as pd

from auth import login_user, register_user, logout_user


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
    }
    # Creating Snowpark session
    session = Session.builder.configs(connection_parameters).create()
    return session

# Ensures only one session is created and used
if 'session' not in st.session_state:
    st.session_state['session'] = create_snowflake_session()
session = st.session_state['session']

root = Root(session)

if 'root' not in st.session_state:
    st.session_state['root'] = Root(st.session_state['session'])
    st.session_state['svc'] = st.session_state['root'].databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
    
svc = st.session_state['svc']


# Ensure the session is using the correct database and schema
session.sql("USE DATABASE CC_QUICKSTART_CORTEX_SEARCH_DOCS").collect()
session.sql("USE SCHEMA DATA").collect()

######################################################################
# Login Related 
######################################################################

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
    
# Show the login or registration page if the user is not logged in
if not st.session_state['logged_in']:
    st.title("Login or Register")
    option = st.selectbox('Choose an option', ['Login', 'Register'])

    username = st.text_input('Username')
    password = st.text_input('Password', type='password')

    if option == 'Register':
        if st.button('Register'):
            register_user(session, username, password)

    elif option == 'Login':
        if st.button('Login'):
            login_user(session, username, password)
else:
    st.write("Welcome to KAI! You are logged in.")

def run_sql_file(session, file_path):
    try:
        with open(file_path, 'r') as file:
            sql_commands = file.read()  

        sql_statements = sql_commands.strip().split(';')

        for sql in sql_statements:
            sql = sql.strip()  
            if sql: 
                session.sql(sql).collect() 
        # st.success(f"SQL file {file_path} executed successfully.")
    except FileNotFoundError:
        st.error(f"SQL file {file_path} not found.")
    except Exception as e:
        st.error(f"Error executing SQL file {file_path}: {e}")

######################################################################
 

# Load custom styles and logo
def load_custom_styles():
    try:
        with open('css/home.css') as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.write("CSS file not found.")

def add_header():
    st.markdown(
        """
        <div class='fixed-header'>
            <img src='https://i.ytimg.com/vi/X13SUD8iD-8/maxresdefault.jpg' alt='WK Kellogg Co Logo' style='max-width: 200px; margin-right: 10px;'>
            <h2 style='display: inline;'>Ask KAI!</h2>
            <button onclick="triggerLogout()" style="position: absolute; right: 20px; top: 10px; background-color: #ff4d4d; color: white; padding: 10px 20px; font-size: 16px; font-weight: bold; border-radius: 8px; cursor: pointer; border: none;">Logout</button>
        </div>
        
        <script>
        function triggerLogout() {
            // Set URL query parameter to trigger logout in Python
            const currentUrl = new URL(window.location.href);
            currentUrl.searchParams.set('logout', 'true');
            window.location.href = currentUrl;  // Redirect to the updated URL
        }
        </script>
        """,
        unsafe_allow_html=True
    )

    # Check for the logout query parameter in Python
    if 'logged_in' in st.session_state and st.query_params.get("logout") == "true":
        st.session_state['logged_in'] = False
        # Clear the logout query parameter and rerun
        st.query_params.clear()  # Clears the query parameters
        st.rerun()


# Call this function at the start of the main function or where appropriate in app.py
add_header()

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

svc_file_1 = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
svc_file_2 = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

def get_similar_chunks_search_service(query):
    # Fetching responses from both services as in Document 1
    try:
        if st.session_state.category_value == "ALL":
            response_file_1 = svc_file_1.search(query, COLUMNS, limit=NUM_CHUNKS)
            response_file_2 = svc_file_2.search(query, COLUMNS, limit=NUM_CHUNKS)
        else:
            filter_obj = {"@eq": {"category": st.session_state.category_value}}
            response_file_1 = svc_file_1.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)
            response_file_2 = svc_file_2.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)
        
        # Parse JSON responses from both services
        json_response_1 = json.loads(response_file_1.json())
        json_response_2 = json.loads(response_file_2.json())

        # Combine the 'results' key from both JSON responses
        combined_response = {
            "results": json_response_1.get('results', []) + json_response_2.get('results', [])
        }
        
        # Debugging information for both responses
        if st.session_state.debug:
            st.sidebar.write(f"Response from service 1: {json_response_1}")
            st.sidebar.write(f"Response from service 2: {json_response_2}")
        
        # Return the combined JSON response
        return json.dumps(combined_response)
    
    except Exception as e:
        st.error(f"Failed to fetch or parse the service response: {e}")
        return {}




# Summarize chat history with the current question
def summarize_question_with_history(chat_history, question):
    prompt = f"<chat_history>{chat_history}</chat_history><question>{question}</question>"
    return Complete(model = st.session_state.model_name, prompt = prompt, session=session)

def create_prompt(myquestion):
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history:
            question_summary = summarize_question_with_history(chat_history, myquestion)
            response_file_1 = svc_file_1.search(question_summary, COLUMNS, limit=NUM_CHUNKS)
            response_file_2 = svc_file_2.search(question_summary, COLUMNS, limit=NUM_CHUNKS)
        else:
            response_file_1 = svc_file_1.search(myquestion, COLUMNS, limit=NUM_CHUNKS)
            response_file_2 = svc_file_2.search(myquestion, COLUMNS, limit=NUM_CHUNKS)
    else:
        response_file_1 = svc_file_1.search(myquestion, COLUMNS, limit=NUM_CHUNKS)
        response_file_2 = svc_file_2.search(myquestion, COLUMNS, limit=NUM_CHUNKS)
 
    # Parse the response as JSON using json.loads()
    try:
        prompt_context_1 = json.loads(response_file_1.json()).get('results', [])
        prompt_context_2 = json.loads(response_file_2.json()).get('results', [])
    except Exception as e:
        st.error(f"Error parsing search response JSON: {e}")
        prompt_context_1 = []
        prompt_context_2 = []

    # Combine results with clear distinction in the prompt
    prompt = prompt = f"""
    As an expert financial analyst, provide a detailed analysis of the financial statements (10Q, 10K) of WK Kellogg Co and General Mills from 2019-2023. Focus on these aspects:
    1. Revenue Trends (Provide a table)
    2. Net Income 
    3. Cash Flow Analysis 
    4. Areas of Investments made by the company (Provide a table)
    5. Efficiency and Cost Control Strategies: Analyze how WK Kellogg Co and General Mills is working to improve operational efficiency and reduce marginal costs.
    6. Profit Margins: Break down gross, operating, and net profit margins (Display in a table).
    7. Key Risk Factors

    **Important**: Even if specific data is not available, leverage pre-trained financial knowledge to provide the most accurate analysis possible based on typical industry standards and practices. Do not state that you lack the context; instead, offer insights and trends based on relevant industry data.  
    **Important**:Do not cover all aspects at once; address them only when specifically requested.
    Answer:
    <context 1>{prompt_context_1}</context 1>
    <context 2>{prompt_context_2}</context 2>
    <question>{myquestion}</question>
    Answer:
    """
    return prompt, [prompt_context_1, prompt_context_2]

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
    # Load custom styles and logo
    if st.session_state['logged_in']:
        # load_custom_styles()
        # add_logo()

        # Configure sidebar options and initialize messages
        config_options()
        init_messages()

        # Predefined questions for the user to select
        button_texts = [
            "What was WK Kellogg Co's revenue for 2023?",
            "How did WK Kellogg Co compete with General Mills?",
            "What are the top product categories in the cereal industry?",
            "What are the health trends affecting cereal sales?",
            "How is the cereal industry adapting to consumer preferences?"
        ]

        # Show recommendations only when the page is first loaded or when "Start Over" is clicked
        if 'show_recommendations' not in st.session_state:
            st.session_state.show_recommendations = True  

        # Initialize selected recommendation state
        if 'selected_recommendation' not in st.session_state:
            st.session_state.selected_recommendation = None

        # Display welcome message and recommendations if no conversation has started and recommendations are active
        if st.session_state.show_recommendations and not st.session_state.messages:
            st.markdown(
                """
                <div class='welcome-container'>
                    <h1 class='welcome-heading'>Hi! I am Kai, your Cereal Industry Analysis Tool</h1>
                    <h2 class='welcome-subheading'>Please select a question or type your own to begin.</h2>
                </div>
                """,
                unsafe_allow_html=True
            )

            # Show recommendations as buttons, and chat input below
            cols = st.columns(len(button_texts))
            for i, rec in enumerate(button_texts):
                with cols[i]:
                    if st.button(rec, key=f"recommendation_{i}"):
                        st.session_state.selected_recommendation = rec
                        st.session_state.messages.append({"role": "user", "content": rec})  # Add clicked recommendation as user input
                        answer, _ = answer_question(rec)  # Get the bot's answer to the selected question
                        st.session_state.messages.append({"role": "assistant", "content": answer})  # Add bot's response to the conversation

                        # Once a recommendation is clicked, hide the recommendations
                        st.session_state.show_recommendations = False
                        st.rerun()  # Rerun the app to hide buttons and show the conversation

        # If recommendations have been clicked, display conversation history
        if not st.session_state.show_recommendations:
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

        # Handling user input from chat box
        # if not st.session_state.show_recommendations:
        if prompt := st.chat_input("Ask a question:"):
            st.session_state.messages.append({"role": "user", "content": prompt})

            answer, _ = answer_question(prompt)

            with st.chat_message("user"):
                st.markdown(prompt)

            with st.chat_message("assistant"):
                st.markdown(answer)
            st.session_state.messages.append({"role": "assistant", "content": answer})

            # Hides the recommendations after a user submits a prompt
            st.session_state.show_recommendations = False
            st.rerun()

        # Add a "Start Over" button to reset the app state
        if st.button("Start Over"):
            st.session_state.show_recommendations = True  # Show recommendations again
            st.session_state.messages = []  # Clear conversation history
            st.rerun()  # Refresh the app
    else:
        st.warning("Please login to access the app.")

# Run the app
if __name__ == "__main__":
    load_custom_styles()
    add_header()
    run_sql_file(session, 'sql/login.sql')
    main()