import bcrypt
import streamlit as st
import re


# Use bcrypt to hash passwords
def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Use bcrypt to check passwords
def check_password(hashed_password, password):
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))

# Function to validate password requirements
def validate_password(password):
    if len(password) < 8:
        return "Password must be at least 8 characters long."
    if not re.search("[A-Z]", password):
        return "Password must contain at least one uppercase letter."
    if not re.search("[a-z]", password):
        return "Password must contain at least one lowercase letter."
    if not re.search("[0-9]", password):
        return "Password must contain at least one digit."
    if not re.search("[!@#$%^&*(),.?\":{}|<>]", password):
        return "Password must contain at least one special character."
    return None  # If all checks pass, return None

# Register new user (add username and password to users table in the database)
def register_user(session, username, password):
    password_error = validate_password(password)
    if password_error:
        st.error(password_error)  
        return

    try:
        # Check if the username already exists in the users table
        existing_user = session.sql(f"SELECT * FROM users WHERE username = '{username}'").collect()

        if existing_user:
            st.error('Username already exists. Please choose a different username.')
        else:
            # Hash the password and insert the new user
            hashed_password = hash_password(password)
            sql_query = f"INSERT INTO users (username, password_hash) VALUES ('{username}', '{hashed_password}')"
            session.sql(sql_query).collect()  
            st.success('User registered successfully!')
            st.session_state['logged_in'] = True

    except Exception as e:
        st.error(f"Error registering user: {e}")




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