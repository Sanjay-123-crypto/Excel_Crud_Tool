# routers/data_loader.py
import os
from models.excel_predictor import ExcelPredictor

# Global predictor instance
_predictor = None

# File mapping
FILE_MAPPING = {
    'project_detail': '05-Project_Detail_Document.xlsx',
    'leave_management': '03-Leave_Management_2025.xlsx',
    'month_efforts': '02-2025_Sep_Month_efforts.xlsx', 
    'month_report': '01-LAAD September Month Report - All.xlsx',
    'team_info': '06-Team Information.xlsx'
}

def get_file_path(file_key: str) -> str:
    """Get full file path from file key"""
    filename = FILE_MAPPING.get(file_key)
    if not filename:
        raise ValueError(f"Unknown file key: {file_key}")
    return os.path.join('data', filename)

def load_all_excel_data():
    """Load all Excel files into the predictor"""
    global _predictor
    
    _predictor = ExcelPredictor()
    
    for file_key, filename in FILE_MAPPING.items():
        file_path = get_file_path(file_key)
        if os.path.exists(file_path):
            try:
                _predictor.load_excel_file(file_path, file_key)
                print(f"✅ Loaded: {filename}")
            except Exception as e:
                print(f"❌ Error loading {filename}: {e}")
        else:
            print(f"⚠️  File not found: {file_path}")

def get_excel_predictor() -> ExcelPredictor:
    """Get the global predictor instance"""
    if _predictor is None:
        raise RuntimeError("Excel predictor not initialized. Call load_all_excel_data() first.")
    return _predictor