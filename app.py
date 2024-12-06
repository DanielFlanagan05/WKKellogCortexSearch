import random
from typing import Literal
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.cortex import Complete
from snowflake.core import Root
import base64
import io
import bcrypt
import json
import pandas as pd
from fpdf import FPDF

import re

from auth import login_user, register_user



pd.set_option("max_colwidth", None)

### Default Values
NUM_CHUNKS = 3  
SLIDE_WINDOW = 7  

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

# Full pool of potential button texts
BUTTON_TEXTS = [
    "What was WK Kellogg Co's revenue for 2023?",
    "How did WK Kellogg Co compete with General Mills?",
    "What are the top product categories in the cereal industry?",
    "What are the health trends affecting cereal sales?",
    "How is the cereal industry adapting to consumer preferences?",
    "How has WK Kellogg Co's market share evolved from 2019 to 2023?",
    "What are the key sustainability initiatives WK Kellogg Co has implemented?",
    "What are the major trends in consumer preferences affecting cereal sales?",
    "What are the top-performing products for General Mills in recent years?",
    "How has the COVID-19 pandemic impacted the cereal industry?",
    "What are the key risks facing the cereal industry?",
    "How have health-conscious trends influenced cereal product development?",
    "What are the major marketing strategies used by WK Kellogg Co?",
    "How has General Mills invested in product innovation?",
    "What are the revenue growth projections for the cereal industry through 2025?"
]

MODEL_DESCRIPTIONS = {
    'mixtral-8x7b': "Mixtral-8x7b is an open-source transformer model, ideal for general text processing tasks such as summarization, classification, and answering questions. It performs well on medium-sized to large datasets.",
    'snowflake-arctic': "Snowflake-Arctic is a model optimized for financial analysis and forecasting. It excels in analyzing large datasets with particular optimization for Snowflake's data lake and warehouse ecosystems.",
    'mistral-large': "Mistral-Large is a high-performance model tailored for language generation tasks. It's great for content creation, including generating emails, reports, and marketing copy.",
    'llama3-8b': "Llama3-8b is a powerful, general-purpose language model.It is highly optimized for maintaining coherence and structure across complex and nuanced tasks generating structured content like formal reports and conversational agents requiring adherence to format",
    'llama3-70b': "Llama3-70b is a massive model capable of handling highly complex tasks with a deep understanding of nuanced language patterns. Ideal for advanced research or industry-specific NLP tasks.",
    'reka-flash': "Reka-Flash is a lightweight model designed for fast inference and quick responses. It's suitable for low-latency applications like real-time chatbots or recommendation systems.",
    'mistral-7b': "Mistral-7b is a balanced model that offers a good trade-off between performance and computational efficiency. It's useful for general NLP tasks without requiring massive compute resources.",
    'llama2-70b-chat': "Llama2-70b-Chat is optimized for conversational AI, providing human-like dialogue capabilities. It works best in chat applications, support bots, and virtual assistants.",
    'gemma-7b': "Gemma-7b is fine-tuned for creative tasks like writing, generating ideas, and crafting compelling stories or articles. It's ideal for content marketers and creative professionals."
}


# --- Snowflake connection setup ---
def create_snowflake_session():
    debug_log("Creating Snowflake session...")
    try:
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
        session = Session.builder.configs(connection_parameters).create()
        debug_log("Snowflake session created successfully.")
        return session
    except Exception as e:
        debug_log(f"Failed to create Snowflake session: {e}")
        st.error(f"Error: Could not create Snowflake session. {e}")
        raise

# Ensures only one session is created and used
if 'session' not in st.session_state:
    st.session_state['session'] = create_snowflake_session()
session = st.session_state['session']

root = Root(session)

if 'root' not in st.session_state:
    st.session_state['root'] = Root(st.session_state['session'])
    st.session_state['svc'] = st.session_state['root'].databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
    
svc = st.session_state['svc']


# Ensures the session is using the correct database and schema
session.sql("USE DATABASE CC_QUICKSTART_CORTEX_SEARCH_DOCS").collect()
session.sql("USE SCHEMA DATA").collect()

######################################################################
# Login Related 
######################################################################

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'show_welcome' not in st.session_state:
    st.session_state['show_welcome'] = False

# Function to display login/register interface
def display_login_register():
    st.title("Login or Register")
    option = st.selectbox('Choose an option', ['Login', 'Register'])

    username = st.text_input('Username')
    password = st.text_input('Password', type='password')

    if option == 'Register':
        if st.button('Register'):
            user_id = register_user(session, username, password)
            if user_id:
                st.session_state['logged_in'] = True
                st.session_state['show_welcome'] = True
                st.rerun()

    elif option == 'Login':
        if st.button('Login'):
            user_id = login_user(session, username, password)
            if user_id:
                st.session_state['logged_in'] = True
                st.session_state['show_welcome'] = True  
                st.rerun()  

def run_sql_file(session, file_path):
    try:
        with open(file_path, 'r') as file:
            sql_commands = file.read()  

        sql_statements = sql_commands.strip().split(';')

        for sql in sql_statements:
            sql = sql.strip()  
            if sql: 
                session.sql(sql).collect() 
    except FileNotFoundError:
        st.error(f"SQL file {file_path} not found.")
    except Exception as e:
        st.error(f"Error executing SQL file {file_path}: {e}")

######################################################################
# HEADER & STYLE SHEET LOADING
######################################################################

# Loads custom styles and logo
def load_custom_styles():
    try:
        with open('css/home.css') as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.write("CSS file not found.")

# Adds in our custom header with the logo, app name/title and logout button
def add_header():
    if st.session_state['logged_in']:
        existing_user_row = session.sql(f"SELECT username FROM users WHERE id = '{st.session_state.get('user_id')}'").collect()
        existing_user = existing_user_row[0]['USERNAME'] 

        username = st.session_state.get('user_id', 'User')
        st.markdown(
            f"""
            <div class='fixed-header'>
            <img src='https://i.ytimg.com/vi/X13SUD8iD-8/maxresdefault.jpg' alt='WK Kellogg Co Logo'>
            <h2 id='ask-kai'>Ask KAI!</h2>
            <p id='username-display'>Logged in as {existing_user}</p>
            <a href="?logout=true" target="_self" id="logout_button">Logout</a>
            </div>
            """,
            unsafe_allow_html=True
        )

        if st.query_params.get("logout") == "true":
            st.session_state['logged_in'] = False
            st.query_params.from_dict({})  
            st.rerun()
    else:
        st.markdown(
            """
            <div class='fixed-header'>
                <img src='https://i.ytimg.com/vi/X13SUD8iD-8/maxresdefault.jpg' alt='WK Kellogg Co Logo'>
                <h2 id='ask-kai'>Ask KAI!</h2>
            </div>
            """,
            unsafe_allow_html=True
        )

######################################################################
# NOTE TAKING FUNCTIONS & EXPORTING NOTES, SUMMARY, AND CHAT TO PDF
######################################################################

# Adds in a text area for users to write notes and a button to save them
def notes_section():
    st.sidebar.markdown("## 📝 Note-Taking")
    
    if "notes" not in st.session_state:
        st.session_state.notes = []

    new_note = st.sidebar.text_area("Add a new note:", key="note_input")
    
    if st.sidebar.button("Save Note"):
        if new_note:
            st.session_state.notes.append(new_note)
            st.sidebar.success("Note saved!")
        else:
            st.sidebar.warning("Please enter a note before saving.")
    
    if st.sidebar.button("Export Notes as PDF"):
        if st.session_state.notes:
            export_notes_to_pdf()
        else:
            st.sidebar.warning("No notes to export.")

# Handles exporting any note the user saved to a PDF
def export_notes_to_pdf():
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.cell(200, 10, txt="Saved Notes", ln=True, align="C")
    pdf.ln(10)  

    for idx, note in enumerate(st.session_state.notes, start=1):
        pdf.multi_cell(0, 10, f"Note {idx}:\n{note}")
        pdf.ln(5)  

    pdf_file = "/tmp/notes.pdf"
    pdf.output(pdf_file)

    with open(pdf_file, "rb") as file:
        b64_pdf = base64.b64encode(file.read()).decode("utf-8")
        href = f'<a href="data:application/octet-stream;base64,{b64_pdf}" download="notes.pdf">Download Notes as PDF</a>'
        st.sidebar.markdown(href, unsafe_allow_html=True)

# Handles export of the summarized response to a PDF
def export_summary_to_pdf(summary):
    if not summary:
        st.sidebar.warning("No summary available to export.")
        return
    
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.cell(200, 10, txt="Response Summary", ln=True, align="C")
    pdf.ln(10)  

    pdf.multi_cell(0, 10, summary)
    pdf.ln(2)  

    pdf_file = "/tmp/response_summary.pdf"
    pdf.output(pdf_file)

    with open(pdf_file, "rb") as file:
        b64_pdf = base64.b64encode(file.read()).decode("utf-8")
        href = f'<a href="data:application/octet-stream;base64,{b64_pdf}" download="response_summary.pdf">Download Response Summary as PDF</a>'
        st.sidebar.markdown(href, unsafe_allow_html=True)

# Handles export of the active chat to a PDF 
def export_chat_to_pdf():
    if "messages" not in st.session_state or not st.session_state.messages:
        st.sidebar.warning("No chat messages to export.")
        return

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.cell(200, 10, txt="Chat Conversation", ln=True, align="C")
    pdf.ln(10)  

    for message in st.session_state.messages:
        role = "User" if message["role"] == "user" else "Assistant"
        pdf.multi_cell(0, 10, f"{role}: {message['content']}")
        pdf.ln(2)  

    if "notes" in st.session_state and st.session_state.notes:
        pdf.add_page()  
        pdf.cell(200, 10, txt="Saved Notes", ln=True, align="C")
        pdf.ln(10)  

        for idx, note in enumerate(st.session_state.notes, start=1):
            pdf.multi_cell(0, 10, f"Note {idx}: {note}")
            pdf.ln(2)  

    pdf_file = "/tmp/chat_conversation.pdf"
    pdf.output(pdf_file)

    with open(pdf_file, "rb") as file:
        b64_pdf = base64.b64encode(file.read()).decode("utf-8")
        href = f'<a href="data:application/octet-stream;base64,{b64_pdf}" download="chat_conversation.pdf">Download Chat as PDF</a>'
        st.sidebar.markdown(href, unsafe_allow_html=True)



### Functions

def debug_log(message):
    if "debug_logs" not in st.session_state:
        st.session_state["debug_logs"] = []
    st.session_state["debug_logs"].append(message)
    st.sidebar.text_area("Debug Logs", value="\n".join(st.session_state["debug_logs"]), height=200)


def display_model_documentation():
    st.markdown("## 📚 Model Documentation")
    st.markdown("Here are the models available for selection and their descriptions:")
    
    for model, description in MODEL_DESCRIPTIONS.items():
        st.markdown(f"### **{model}**")
        st.write(description)
        st.markdown("---")  

def config_options():
    # Sidebar dropdown for model selection
    selected_model = st.sidebar.selectbox(
        'Select your model:', 
        list(MODEL_DESCRIPTIONS.keys()), 
        key="model_name"
    )

    # Automatically display the description of the selected model
    st.sidebar.markdown(f"### Selected Model: **{selected_model}**")
    st.sidebar.write(MODEL_DESCRIPTIONS[selected_model])

    categories = session.table('docs_chunks_table').select('category').distinct().collect()
    cat_list = ['ALL'] + [cat.CATEGORY for cat in categories]
    st.sidebar.checkbox('Remember chat history?', key="use_chat_history", value=True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=start_over)

    if st.session_state.get('logged_in'):
        user_id = st.session_state.get('user_id')
        if user_id:
            past_prompts_df = session.table('user_prompts') \
                .filter(f"user_id = {user_id}") \
                .select('prompt_text', 'id') \
                .order_by('id', ascending=False) \
                .limit(20) \
                .collect()
            past_prompts = [row['PROMPT_TEXT'][:100] for row in past_prompts_df]

            if 'past_chats_selectbox' not in st.session_state:
                st.session_state['past_chats_selectbox'] = 'Select a prompt'

            if 'last_processed_prompt' not in st.session_state:
                st.session_state['last_processed_prompt'] = None

            if past_prompts:
                selected_past_prompt = st.sidebar.selectbox(
                    'Past Chats',
                    ['Select a prompt'] + past_prompts,
                    key='past_chats_selectbox'
                )
                if (selected_past_prompt and selected_past_prompt != 'Select a prompt' and
                    selected_past_prompt != st.session_state['last_processed_prompt']):
                    st.session_state.messages.append({"role": "user", "content": selected_past_prompt})
                    answer, summary, _ = answer_question(selected_past_prompt)
                    with st.chat_message("user"):
                        st.markdown(selected_past_prompt)
                    with st.chat_message("assistant"):
                        st.markdown(answer)
                    st.session_state.messages.append({"role": "assistant", "content": answer})
                    st.session_state.summary = summary
                    st.session_state.show_recommendations = False
                    st.session_state['last_processed_prompt'] = selected_past_prompt

                    st.rerun()



def init_messages():
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.show_welcome_message = True
    else:
        st.session_state.show_welcome_message = False

svc_file_1 = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
svc_file_2 = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

# Retrieves "similar" chunks, meaning chunks (data) that are related to the query
def get_similar_chunks_search_service(query):
    response_file_1 = svc_file_1.search(query, COLUMNS, limit=NUM_CHUNKS)
    response_file_2 = svc_file_2.search(query, COLUMNS, limit=NUM_CHUNKS)
    try:
        json_response_1 = json.loads(response_file_1.json())  
        json_response_2 = json.loads(response_file_2.json())  
    except Exception as e:
        st.error(f"Failed to parse JSON response: {e}")
        return {}
    results_1 = json_response_1.get('results', [])
    results_2 = json_response_2.get('results', [])
    combined_response = {
        "results": results_1 + results_2
    }
    st.sidebar.json(combined_response)
    return json.dumps(combined_response)


# --- Debugging Cortex Model Usage ---
def summarize_question_with_history(chat_history, question):
    debug_log(f"Summarizing question with history. Chat history: {chat_history}, Question: {question}")
    prompt = f"""
        Based on the chat history below and the question, generate a query that extends the question
        with the chat history provided. The query should be in natural language. 
        Answer with only the query.
<chat_history>{chat_history}</chat_history>
<question>{question}</question>
"""
    try:
        summary = Complete(st.session_state.model_name, prompt, session=session)
        debug_log(f"Generated summary: {summary}")
        return summary.replace("'", "")
    except Exception as e:
        debug_log(f"Error in summarizing question with history: {e}")
        st.error(f"Error: Could not summarize question with history. {e}")
        raise

def create_prompt(myquestion):
    debug_log(f"Creating prompt for question: {myquestion}")
    try:
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()
            debug_log(f"Chat history retrieved: {chat_history}")
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

        prompt_context_1 = json.loads(response_file_1.json()).get('results', [])
        prompt_context_2 = json.loads(response_file_2.json()).get('results', [])
        debug_log(f"Prompt contexts retrieved: {prompt_context_1}, {prompt_context_2}")
    except Exception as e:
        debug_log(f"Error in creating prompt: {e}")
        st.error(f"Error: Could not create prompt. {e}")
        prompt_context_1 = []
        prompt_context_2 = []
 
    prompt = prompt = f""" 
        As an expert financial analyst, provide a detailed analysis of the financial statements (10Q, 10K) of WK Kellogg Co and General Mills from 2019-2024. Focus on these aspects:
        1. Revenue Trends (Provide a table)- Talk about the sales figures, the products sold and the countries/regions the products are sold in
        2. Net Income 
        3. Cash Flow Analysis 
        4. Areas of Investments made by the company (Provide a table)
        5. Efficiency and Cost Control Strategies: Analyze how WK Kellogg Co and General Mills is working to improve operational efficiency and reduce marginal costs.
        6. Profit Margins: Break down gross, operating, and net profit margins (Display in a table).
        7. Key Risk Factors 
        8. Cereal/product prices 
        9. Exactly which product(s) example froot loops or cornflakes generated most revenue?
    
        
    
         **Important**: Even if specific data is not available, leverage pre-trained financial knowledge to provide the most accurate analysis possible based on typical industry standards and practices. Do not state that you lack the context; instead, offer insights and trends based on relevant industry data.  
         **Important**:Do not cover all aspects at once; address them only when specifically requested.
         **Important**:Anytime the user enters the word "We" or 'we' or 'WE' it is referring to WK Kellogg Co as you are part of their financial department.
         Answer:
         <context 1>{prompt_context_1}</context 1>
         <context 2>{prompt_context_2}</context 2>
         <question>{myquestion}</question>
         Answer:


    
#         Also, As a financial analyst specialized in the cereal and consumer packaged goods (CPG) industries, provide a direct, data-driven answer to the question based only on the specific area requested. Use financial insights from available SEC filings, historical financial data (10Q, 10K), and industry knowledge, but keep responses brief and focused. 
        
#         Important Details to Include in Responses:
#         1. Only address the requested topic (e.g., supply/demand changes, M&A activities, investment programs, etc.) with concise, quantified data or insights where applicable.
#         2. If relevant, summarize tables of revenue, investment, or margin metrics with recent trends rather than providing full tables unless explicitly requested.
        3. For questions about competitors, draw insights from publicly available filings, such as investment initiatives, market expansion, product shifts, or financial strategies.
        4. If industry-wide trends or macroeconomic impacts are requested, focus on significant changes, shareholder response, or sector-wide risks.
        
        **Guidelines**:
        - Even if specific data is not available, leverage pre-trained financial knowledge to provide the most accurate analysis possible based on typical industry standards and practices. Do not state that you lack the context; instead, offer insights and trends based on relevant industry data. 
        - Use context from past SEC filings, public stock data, and known macroeconomic impacts to suggest trends where specific figures may not be available.
        - Assume "We" refers to WK Kellogg Co.
        - When responding, ensure that numbers and units (e.g., '3,515 million') have proper spacing to avoid unintended styling. Do not use underscores or other characters directly following numeric values.
        
        Question:
        {myquestion}
        
        Context:
        <context 1>{prompt_context_1}</context 1>
        <context 2>{prompt_context_2}</context 2>
        
        Answer:
        """
    debug_log(f"Generated prompt: {prompt}")

    return prompt, [prompt_context_1, prompt_context_2]



def save_prompt_to_database(session, user_id, prompt_text):
    if user_id is None or not prompt_text:
        raise ValueError("User ID and prompt text must not be NULL or empty")

    sql_query = f"INSERT INTO user_prompts (user_id, prompt_text) VALUES ('{user_id}', '{prompt_text}')"

    session.sql(sql_query).collect()  

def display_welcome_message():
    st.markdown(
        """
        <div class='welcome-container'>
            <h1 class='welcome-heading'>Welcome to KAI, your Cereal Industry Analysis Tool</h1>
            <h2 class='welcome-subheading'>Please select a question or type your own to begin.</h2>
        </div>
        """,
        unsafe_allow_html=True
    )

def clean_response(response):
    
    response = re.sub(r'(\d)(million|billion)', r'\1 million', response)
    response = re.sub(r'(\d)(thousand)', r'\1 thousand', response)
    return response

def summarize_response(response):
    prompt = f"""
    Provide a concise summary of the following response, focusing on the top three key insights only. Organize the summary in three clear, actionable bullet points:

    {response}

    Key Insights (Limit to 3):
    """
    summary = Complete(st.session_state.model_name, prompt, session=session)
    return summary


# Answers the prompt using the model
def answer_question(myquestion):
    debug_log(f"Answering question: {myquestion}")
    try:
        prompt, relative_paths = create_prompt(myquestion)
        response = Complete(st.session_state.model_name, prompt, session=session)
        cleaned_response = clean_response(response)
        debug_log(f"Response: {cleaned_response}")
        summary = summarize_response(cleaned_response)
        return cleaned_response, summary, relative_paths
    except Exception as e:
        debug_log(f"Error in answering question: {e}")
        st.error(f"Error: Could not answer question. {e}")
        raise

# Gets chat history from the last 7 messages (the slide window size) to use for context
def get_chat_history():
    chat_history = []
    start_index = max(0, len(st.session_state.messages) - SLIDE_WINDOW)
    chat_history.extend(st.session_state.messages[start_index:])
    return chat_history
    
# Resets the chat, reccomendations, selected prompt from history, and reruns the app
def start_over():
    st.session_state.show_recommendations = True
    st.session_state.messages = [] 
    st.session_state.visible_recommendations = random.sample(BUTTON_TEXTS, 3) 
    st.session_state["reset_requested"] = True  
    st.session_state['last_processed_prompt'] = None
    st.rerun()

st.sidebar.markdown("### Debugging Logs")
if "debug_logs" in st.session_state:
    st.sidebar.text_area("Logs", "\n".join(st.session_state["debug_logs"]), height=200)
    
def main():
    if st.session_state['logged_in']:
        # Checks for reset flag in session state
        if st.session_state.get("reset_requested", False): 
            st.session_state["reset_requested"] = False
            st.session_state['past_chats_selectbox'] = 'Select a prompt'
            st.rerun()

        config_options()
        init_messages()
        notes_section()

        st.sidebar.markdown("## Export Chat")
        if st.sidebar.button("Export Chat as PDF"):
            export_chat_to_pdf()

        st.sidebar.markdown("## Export Summary")
        if st.sidebar.button("Export Summary as PDF"):
            if "summary" in st.session_state and st.session_state.summary:
                export_summary_to_pdf(st.session_state.summary)
            else:
                st.sidebar.warning("Generate a response summary first before exporting.")




        # Show recommendations only when the page is first loaded or when "Start Over" is clicked
        if 'show_recommendations' not in st.session_state:
            st.session_state.show_recommendations = True  

        # Initialize selected recommendation state
        if 'selected_recommendation' not in st.session_state:
            st.session_state.selected_recommendation = None

        # Display welcome message and recommendations if no conversation has started and recommendations are active
        if st.session_state.show_recommendations and not st.session_state.messages:
            display_welcome_message()

        if 'visible_recommendations' not in st.session_state:
            st.session_state.visible_recommendations = random.sample(BUTTON_TEXTS, 3)

        # Display the three selected recommendations
        if st.session_state.show_recommendations:
            cols = st.columns(3)
            for i, rec in enumerate(st.session_state.visible_recommendations):
                with cols[i]:
                    if st.button(rec, key=f"recommendation_{i}"):
                        st.session_state.selected_recommendation = rec
                        st.session_state.messages.append({"role": "user", "content": rec})
                        answer, summary, _ = answer_question(rec)
                        st.session_state.messages.append({"role": "assistant", "content": answer})
                        st.session_state.summary = summary
                        st.session_state.show_recommendations = False
                        st.rerun()

        # If recommendations have been clicked, display conversation history
        if not st.session_state.show_recommendations and st.session_state.past_chats_selectbox == 'Select a prompt':
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

        # Handling user input from chat box
        if prompt := st.chat_input("Ask a question:"):
            st.session_state.messages.append({"role": "user", "content": prompt})

            # Save prompt to database
            user_id = st.session_state.get('user_id')
            if user_id:
                save_prompt_to_database(session, user_id, prompt)

            answer, summary, _ = answer_question(prompt)

            with st.chat_message("user"):
                st.markdown(prompt)

            with st.chat_message("assistant"):
                st.markdown(answer)

            st.session_state.messages.append({"role": "assistant", "content": answer})
            st.session_state.summary = summary

            # Hides the recommendations after a user submits a prompt
            st.session_state.show_recommendations = False
            st.rerun()

        st.sidebar.markdown("## 📄 Response Summary")
        if "summary" in st.session_state:
            st.sidebar.write(st.session_state.summary)

        # Reset recommendations when "Start Over" button is clicked
        if st.button("Start Over"):
            start_over()
    else:
        display_login_register()
        st.warning("Please login to access the app.")


if __name__ == "__main__":
    load_custom_styles()
    add_header()
    run_sql_file(session, 'sql/login.sql')
    main()