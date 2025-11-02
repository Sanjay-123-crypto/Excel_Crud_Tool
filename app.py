# app.py
import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime
import time
import sys

st.set_page_config(
    page_title="LAAD BOT PHASE-1 TOOL",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {font-size:2.5rem;color:#1f77b4;text-align:center;margin-bottom:1rem;font-weight:bold;}
    .sub-header {font-size:1.3rem;color:#2e86ab;margin-bottom:1rem;font-weight:600;}
    .success-box {background-color:#d4edda;border:1px solid #c3e6cb;border-radius:5px;padding:12px;margin:8px 0;}
    .error-box {background-color:#f8d7da;border:1px solid #f5c6cb;border-radius:5px;padding:12px;margin:8px 0;}
    .info-box {background-color:#d1ecf1;border:1px solid #bee5eb;border-radius:5px;padding:12px;margin:8px 0;}
    .connection-status {padding:8px 12px;border-radius:20px;font-weight:bold;text-align:center;}
    .online {background-color:#d4edda;color:#155724;}
    .offline {background-color:#f8d7da;color:#721c24;}
</style>
""", unsafe_allow_html=True)

API_BASE_URL = "http://localhost:8000"

# ------------------ CLIENT ------------------
class ExcelCRUDClient:
    def __init__(self):
        self.base_url = API_BASE_URL

    def test_connection(self):
        try:
            response = requests.get(f"{self.base_url}/", timeout=3)
            return response.status_code == 200
        except:
            return False

    def get_system_info(self):
        try:
            r = requests.get(f"{self.base_url}/info", timeout=5)
            if r.status_code == 200:
                return r.json()
        except:
            return None

    def list_files(self):
        try:
            r = requests.get(f"{self.base_url}/info", timeout=5)
            if r.status_code == 200:
                return {"success": True, "files": r.json()}
        except:
            return {"success": False}

    def read_data(self, column_name, column_value=None):
        payload = {"column_name": column_name}
        if column_value:
            payload["column_value"] = column_value
        try:
            r = requests.post(f"{self.base_url}/read", json=payload, timeout=10)
            return r.json()
        except Exception as e:
            return {"success": False, "message": str(e)}

    def update_data(self, column_name, column_value, update_column, update_value):
        payload = {
            "column_name": column_name,
            "column_value": column_value,
            "update_column": update_column,
            "update_value": update_value,
        }
        try:
            r = requests.post(f"{self.base_url}/update", json=payload, timeout=10)
            return r.json()
        except Exception as e:
            return {"success": False, "message": str(e)}

    def insert_data(self, data: dict):
        try:
            r = requests.post(f"{self.base_url}/insert", json={"data": data}, timeout=10)
            return r.json()
        except Exception as e:
            return {"success": False, "message": str(e)}

    def delete_data(self, column_name, column_value):
        payload = {"column_name": column_name, "column_value": column_value}
        try:
            r = requests.post(f"{self.base_url}/delete", json=payload, timeout=10)
            return r.json()
        except Exception as e:
            return {"success": False, "message": str(e)}

# ------------------ UI HELPERS ------------------
def show_connection_status(client):
    if client.test_connection():
        st.markdown('<div class="connection-status online">🟢 Connected to API</div>', unsafe_allow_html=True)
        return True
    else:
        st.markdown('<div class="connection-status offline">🔴 API Not Connected</div>', unsafe_allow_html=True)
        st.warning("Make sure the backend FastAPI server is running on port 8000.")
        if st.button("🔄 Retry"):
            st.rerun()
        return False

# ------------------ OPERATIONS ------------------
def show_read_operation(client):
    st.markdown('<div class="sub-header">🔍 Read Data</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        col_name = st.text_input("Column Name *")
    with col2:
        col_value = st.text_input("Column Value (Optional)")

    if st.button("🚀 Execute Query"):
        if not col_name:
            st.error("Column name is required")
            return
        with st.spinner("Reading data..."):
            result = client.read_data(col_name, col_value)
            if result.get("success"):
                st.success(result.get("message", "Success"))
                df = pd.DataFrame(result.get("data", []))
                if not df.empty:
                    st.dataframe(df, use_container_width=True)
            else:
                st.error(result.get("message", "Failed"))

def show_update_operation(client):
    st.markdown('<div class="sub-header">✏️ Update Data</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        find_col = st.text_input("Search Column *")
        find_val = st.text_input("Search Value *")
    with col2:
        upd_col = st.text_input("Update Column *")
        upd_val = st.text_input("New Value *")

    if st.button("🚀 Execute Update"):
        if not all([find_col, find_val, upd_col, upd_val]):
            st.error("All fields are required")
            return
        with st.spinner("Updating data..."):
            result = client.update_data(find_col, find_val, upd_col, upd_val)
            if result.get("success"):
                st.success(result["message"])
            else:
                st.error(result.get("message", "Failed"))

def show_insert_operation(client):
    st.markdown('<div class="sub-header">➕ Insert New Record</div>', unsafe_allow_html=True)
    st.info("Provide column-value pairs to insert. Example: Project_Name: NewProject")
    data_str = st.text_area("Enter JSON-like data", placeholder='{"Project_Name":"Test","Status":"In Progress"}')
    if st.button("🚀 Insert Record"):
        try:
            data = json.loads(data_str)
        except:
            st.error("Invalid JSON format")
            return
        with st.spinner("Inserting data..."):
            result = client.insert_data(data)
            if result.get("success"):
                st.success(result["message"])
            else:
                st.error(result.get("message", "Insert failed"))

def show_delete_operation(client):
    st.markdown('<div class="sub-header">🗑️ Delete Record</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        col_name = st.text_input("Column Name *")
    with col2:
        col_value = st.text_input("Value to Match *")

    if st.button("🚀 Delete Record"):
        if not all([col_name, col_value]):
            st.error("Both fields required")
            return
        with st.spinner("Deleting data..."):
            result = client.delete_data(col_name, col_value)
            if result.get("success"):
                st.success(result["message"])
            else:
                st.error(result.get("message", "Delete failed"))

# ------------------ MAIN ------------------
def main():
    client = ExcelCRUDClient()
    st.markdown('<div class="main-header">LAAD BOT PHASE-1 TOOL</div>', unsafe_allow_html=True)

    if not show_connection_status(client):
        st.stop()

    with st.sidebar:
        st.markdown("### Navigation")
        option = st.radio("Choose Operation:", [
            "📈 Dashboard", "🔍 Read Data", "✏️ Update Data", "➕ Insert Data", "🗑️ Delete Data"
        ])

    if option == "📈 Dashboard":
        info = client.get_system_info()
        if info:
            st.success("✅ System Healthy")
            st.json(info)
        else:
            st.error("⚠️ Could not fetch info - maybe backend not loaded files yet.")
    elif option == "🔍 Read Data":
        show_read_operation(client)
    elif option == "✏️ Update Data":
        show_update_operation(client)
    elif option == "➕ Insert Data":
        show_insert_operation(client)
    elif option == "🗑️ Delete Data":
        show_delete_operation(client)

if __name__ == "__main__":
    main()
