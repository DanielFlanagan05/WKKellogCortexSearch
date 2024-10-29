import bcrypt
import streamlit as st

# Use bcrypt to hash passwords
def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Use bcrypt to check passwords
def check_password(hashed_password, password):
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))

# Register new user (add username and password to users table in database)
def register_user(session, username, password):
    hashed_password = hash_password(password)
    try:
        sql_query = f"INSERT INTO users (username, password_hash) VALUES ('{username}', '{hashed_password}')"
        # st.write(f"Executing query: {sql_query}")  # Debugging: Output query to Streamlit
        session.sql(sql_query).collect()  # Execute the SQL query
        st.success('User registered successfully!')
        st.session_state['logged_in'] = True
    except Exception as e:
        st.error(f"Error registering user: {e}")
        # st.write(e)  # Debugging: Output error details to Streamlit



# Login user by checking username and password against the database
def login_user(session, username, password):
    try:
        user_data = session.sql("SELECT password_hash FROM users WHERE username = ?", (username,)).collect()
        if user_data:
            hashed_password = user_data[0]['PASSWORD_HASH']
            if check_password(hashed_password, password):
                st.session_state['logged_in'] = True
                st.success('Logged in successfully!')
            else:
                st.error('Incorrect username or password.')
        else:
            st.error('User not found.')
    except Exception as e:
        st.error(f"Error logging in: {e}")