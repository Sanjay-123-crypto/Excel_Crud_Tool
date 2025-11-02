# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
import os
from datetime import datetime

app = FastAPI(
    title="Excel ML CRUD Tool",
    description="AI-powered Excel operations without specifying file/sheet names",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data models
class ReadRequest(BaseModel):
    column_name: str
    column_value: Optional[str] = None

class UpdateRequest(BaseModel):
    column_name: str
    column_value: str
    update_column: str
    update_value: Any

class InsertRequest(BaseModel):
    data: Dict[str, Any]

class DeleteRequest(BaseModel):
    column_name: str
    column_value: str

class ResponseModel(BaseModel):
    success: bool
    message: str
    data: Optional[List[Dict]] = None
    predicted_location: Optional[Dict] = None
    total_records: Optional[int] = None

# Global data storage
excel_data = {}
file_paths = {
    'project_detail': 'data/05-Project_Detail_Document.xlsx',
    'leave_management': 'data/03-Leave_Management_2025.xlsx',
    'month_efforts': 'data/02-2025_Sep_Month_efforts.xlsx',
    'month_report': 'data/01-LAAD September Month Report - All.xlsx',
    'team_info': 'data/06-Team Information.xlsx'
}

class SimpleExcelPredictor:
    def __init__(self):
        self.excel_data = {}
        self.column_mapping = {}
        
    def load_all_data(self):
        """Load all Excel files"""
        for file_key, file_path in file_paths.items():
            if os.path.exists(file_path):
                try:
                    excel_file = pd.ExcelFile(file_path)
                    self.excel_data[file_key] = {}
                    
                    for sheet_name in excel_file.sheet_names:
                        df = excel_file.parse(sheet_name)
                        # Clean column names
                        df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]
                        self.excel_data[file_key][sheet_name] = df
                        
                        # Build column mapping
                        for col in df.columns:
                            col_lower = col.lower()
                            if col_lower not in self.column_mapping:
                                self.column_mapping[col_lower] = []
                            self.column_mapping[col_lower].append({
                                'file': file_key,
                                'sheet': sheet_name,
                                'confidence': self._calculate_confidence(col_lower, df[col])
                            })
                            
                    print(f"✅ Loaded {file_key}: {len(excel_file.sheet_names)} sheets")
                    
                except Exception as e:
                    print(f"❌ Error loading {file_key}: {e}")
    
    def _calculate_confidence(self, column_name: str, series: pd.Series) -> float:
        """Calculate confidence score for a column"""
        confidence = 0.5  # Base confidence
        
        # Boost for common column names
        common_columns = ['name', 'id', 'status', 'date', 'project', 'user', 'email']
        for common in common_columns:
            if common in column_name:
                confidence += 0.3
                break
        
        # Boost for columns with high uniqueness
        if len(series) > 0:
            uniqueness = series.nunique() / len(series)
            if uniqueness > 0.8:
                confidence += 0.2
        
        return min(confidence, 1.0)
    
    def predict_location(self, column_name: str, column_value: str = None) -> Dict:
        """Predict the best location for the column"""
        column_name_lower = column_name.strip().lower()
        
        if column_name_lower not in self.column_mapping:
            # Try partial matching
            candidates = []
            for col, locations in self.column_mapping.items():
                if column_name_lower in col or col in column_name_lower:
                    candidates.extend(locations)
            
            if not candidates:
                return {'file': 'unknown', 'sheet': 'unknown', 'confidence': 0.0}
            
            # Sort by confidence
            candidates.sort(key=lambda x: x['confidence'], reverse=True)
            best = candidates[0]
            return {
                'file': best['file'],
                'sheet': best['sheet'],
                'confidence': best['confidence'] * 0.8  # Reduce confidence for partial match
            }
        
        # Get all locations for this column
        locations = self.column_mapping[column_name_lower]
        locations.sort(key=lambda x: x['confidence'], reverse=True)
        
        best_location = locations[0]
        
        # If value is provided, try to find best match
        if column_value:
            best_location = self._refine_with_value(best_location, column_name_lower, column_value, locations)
        
        return {
            'file': best_location['file'],
            'sheet': best_location['sheet'],
            'confidence': best_location['confidence']
        }
    
    def _refine_with_value(self, best_location: Dict, column_name: str, value: str, all_locations: List[Dict]) -> Dict:
        """Refine prediction using the provided value"""
        value_str = str(value).lower()
        
        # Check if value exists in the best location
        file_key = best_location['file']
        sheet_name = best_location['sheet']
        
        if file_key in self.excel_data and sheet_name in self.excel_data[file_key]:
            df = self.excel_data[file_key][sheet_name]
            if column_name in df.columns:
                if value_str in df[column_name].astype(str).str.lower().values:
                    best_location['confidence'] += 0.2
                    return best_location
        
        # Try other locations
        for location in all_locations[1:]:
            file_key = location['file']
            sheet_name = location['sheet']
            
            if file_key in self.excel_data and sheet_name in self.excel_data[file_key]:
                df = self.excel_data[file_key][sheet_name]
                if column_name in df.columns:
                    if value_str in df[column_name].astype(str).str.lower().values:
                        location['confidence'] += 0.3
                        return location
        
        return best_location
    
    def get_dataframe(self, file_key: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """Get dataframe for file and sheet"""
        return self.excel_data.get(file_key, {}).get(sheet_name)
    
    def reload_file(self, file_key: str):
        """Reload a specific file"""
        if file_key in file_paths:
            file_path = file_paths[file_key]
            if os.path.exists(file_path):
                try:
                    excel_file = pd.ExcelFile(file_path)
                    self.excel_data[file_key] = {}
                    
                    for sheet_name in excel_file.sheet_names:
                        df = excel_file.parse(sheet_name)
                        df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]
                        self.excel_data[file_key][sheet_name] = df
                        
                    print(f"✅ Reloaded {file_key}")
                except Exception as e:
                    print(f"❌ Error reloading {file_key}: {e}")

# Initialize predictor
predictor = SimpleExcelPredictor()

@app.on_event("startup")
async def startup_event():
    """Load data on startup"""
    predictor.load_all_data()
    print("🚀 Excel ML CRUD Tool started successfully!")

@app.get("/")
async def root():
    return {
        "message": "Excel ML CRUD Tool",
        "status": "running",
        "endpoints": {
            "read": "POST /read - Read data by column name and value",
            "update": "POST /update - Update data",
            "insert": "POST /insert - Insert new data", 
            "delete": "POST /delete - Delete data",
            "info": "GET /info - System information"
        }
    }

@app.get("/info")
async def system_info():
    """Get system information"""
    total_files = len(predictor.excel_data)
    total_sheets = sum(len(sheets) for sheets in predictor.excel_data.values())
    total_columns = len(predictor.column_mapping)
    
    return {
        "status": "healthy",
        "total_files": total_files,
        "total_sheets": total_sheets,
        "total_columns": total_columns,
        "loaded_files": list(predictor.excel_data.keys())
    }

@app.post("/read", response_model=ResponseModel)
async def read_data(request: ReadRequest):
    """Read data without specifying file/sheet"""
    try:
        # Predict location
        prediction = predictor.predict_location(request.column_name, request.column_value)
        
        if prediction['confidence'] < 0.3:
            return ResponseModel(
                success=False,
                message="No confident prediction found. Try different column names.",
                predicted_location=prediction
            )
        
        # Get data from predicted location
        file_key = prediction['file']
        sheet_name = prediction['sheet']
        df = predictor.get_dataframe(file_key, sheet_name)
        
        if df is None:
            return ResponseModel(
                success=False,
                message="Predicted location not available",
                predicted_location=prediction
            )
        
        # Find matching column
        column_name_lower = request.column_name.lower()
        actual_column = None
        for col in df.columns:
            if column_name_lower == col.lower():
                actual_column = col
                break
        
        if not actual_column:
            return ResponseModel(
                success=False,
                message=f"Column '{request.column_name}' not found",
                predicted_location=prediction
            )
        
        # Filter data
        if request.column_value:
            mask = df[actual_column].astype(str).str.lower() == str(request.column_value).lower()
            results = df[mask]
        else:
            results = df
        
        # Convert to list of dictionaries
        data_list = results.replace({np.nan: None}).to_dict('records')
        
        return ResponseModel(
            success=True,
            message=f"Found {len(data_list)} matching records",
            data=data_list,
            predicted_location=prediction,
            total_records=len(data_list)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

@app.post("/update", response_model=ResponseModel)
async def update_data(request: UpdateRequest):
    """Update data without specifying file/sheet"""
    try:
        # Predict location
        prediction = predictor.predict_location(request.column_name, request.column_value)
        
        if prediction['confidence'] < 0.3:
            return ResponseModel(
                success=False,
                message="Low confidence for update operation",
                predicted_location=prediction
            )
        
        file_path = file_paths[prediction['file']]
        file_key = prediction['file']
        sheet_name = prediction['sheet']
        
        if not os.path.exists(file_path):
            return ResponseModel(
                success=False,
                message="Excel file not found",
                predicted_location=prediction
            )
        
        # Load the Excel file
        excel_file = pd.ExcelFile(file_path)
        sheets_data = {}
        
        for sheet in excel_file.sheet_names:
            sheets_data[sheet] = excel_file.parse(sheet)
        
        # Get target dataframe
        df = sheets_data[sheet_name]
        
        # Find actual columns
        actual_column = None
        actual_update_column = None
        
        for col in df.columns:
            col_str = str(col).lower()
            if request.column_name.lower() == col_str:
                actual_column = col
            if request.update_column.lower() == col_str:
                actual_update_column = col
        
        if not actual_column or not actual_update_column:
            return ResponseModel(
                success=False,
                message="Required columns not found",
                predicted_location=prediction
            )
        
        # Update data
        mask = df[actual_column].astype(str).str.lower() == str(request.column_value).lower()
        update_count = mask.sum()
        
        if update_count == 0:
            return ResponseModel(
                success=False,
                message="No matching records found to update",
                predicted_location=prediction
            )
        
        df.loc[mask, actual_update_column] = request.update_value
        sheets_data[sheet_name] = df
        
        # Save back to Excel
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet, data in sheets_data.items():
                data.to_excel(writer, sheet_name=sheet, index=False)
        
        # Reload the updated file
        predictor.reload_file(file_key)
        
        return ResponseModel(
            success=True,
            message=f"Successfully updated {update_count} record(s)",
            predicted_location=prediction
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating data: {str(e)}")

@app.post("/insert", response_model=ResponseModel)
async def insert_data(request: InsertRequest):
    """Insert new data without specifying file/sheet"""
    try:
        if not request.data:
            return ResponseModel(success=False, message="No data provided")
        
        # Use first column for prediction
        first_column = list(request.data.keys())[0]
        first_value = str(request.data[first_column])
        
        prediction = predictor.predict_location(first_column, first_value)
        
        if prediction['confidence'] < 0.2:
            return ResponseModel(
                success=False,
                message="Cannot determine where to insert data",
                predicted_location=prediction
            )
        
        file_path = file_paths[prediction['file']]
        file_key = prediction['file']
        sheet_name = prediction['sheet']
        
        # Load the file
        excel_file = pd.ExcelFile(file_path)
        sheets_data = {}
        
        for sheet in excel_file.sheet_names:
            sheets_data[sheet] = excel_file.parse(sheet)
        
        # Prepare new row
        df = sheets_data[sheet_name]
        new_row = {}
        
        # Map input data to actual columns
        for input_col, value in request.data.items():
            input_col_lower = input_col.lower()
            for actual_col in df.columns:
                if input_col_lower == str(actual_col).lower():
                    new_row[actual_col] = value
                    break
        
        # Fill missing columns
        for col in df.columns:
            if col not in new_row:
                new_row[col] = None
        
        # Append new row
        new_df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        sheets_data[sheet_name] = new_df
        
        # Save back to Excel
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet, data in sheets_data.items():
                data.to_excel(writer, sheet_name=sheet, index=False)
        
        # Reload data
        predictor.reload_file(file_key)
        
        return ResponseModel(
            success=True,
            message="Successfully inserted new record",
            predicted_location=prediction
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting data: {str(e)}")

@app.post("/delete", response_model=ResponseModel)
async def delete_data(request: DeleteRequest):
    """Delete data without specifying file/sheet"""
    try:
        prediction = predictor.predict_location(request.column_name, request.column_value)
        
        if prediction['confidence'] < 0.3:
            return ResponseModel(
                success=False,
                message="Low confidence for delete operation",
                predicted_location=prediction
            )
        
        file_path = file_paths[prediction['file']]
        file_key = prediction['file']
        sheet_name = prediction['sheet']
        
        # Load the file
        excel_file = pd.ExcelFile(file_path)
        sheets_data = {}
        
        for sheet in excel_file.sheet_names:
            sheets_data[sheet] = excel_file.parse(sheet)
        
        # Find and delete data
        df = sheets_data[sheet_name]
        actual_column = None
        
        for col in df.columns:
            if request.column_name.lower() == str(col).lower():
                actual_column = col
                break
        
        if not actual_column:
            return ResponseModel(
                success=False,
                message="Column not found",
                predicted_location=prediction
            )
        
        # Delete matching rows
        mask = df[actual_column].astype(str).str.lower() == str(request.column_value).lower()
        delete_count = mask.sum()
        
        if delete_count == 0:
            return ResponseModel(
                success=False,
                message="No matching records found to delete",
                predicted_location=prediction
            )
        
        df = df[~mask]
        sheets_data[sheet_name] = df
        
        # Save back to Excel
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet, data in sheets_data.items():
                data.to_excel(writer, sheet_name=sheet, index=False)
        
        # Reload data
        predictor.reload_file(file_key)
        
        return ResponseModel(
            success=True,
            message=f"Successfully deleted {delete_count} record(s)",
            predicted_location=prediction
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting data: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)