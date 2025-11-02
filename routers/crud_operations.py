# routers/crud_operations.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional, Any, Union
import pandas as pd
import os
import numpy as np

from routers.data_loader import get_excel_predictor, get_file_path

router = APIRouter()

class ReadRequest(BaseModel):
    column_name: str
    column_value: Optional[str] = None
    operation: str = "read"

class UpdateRequest(BaseModel):
    column_name: str
    column_value: str
    update_column: str
    update_value: Any
    operation: str = "update"

class InsertRequest(BaseModel):
    data: Dict[str, Any]
    operation: str = "insert"

class DeleteRequest(BaseModel):
    column_name: str
    column_value: str
    operation: str = "delete"

class SearchRequest(BaseModel):
    search_text: str
    max_results: int = 10

class ResponseModel(BaseModel):
    success: bool
    message: str
    data: Optional[List[Dict]] = None
    predicted_location: Optional[Dict] = None
    total_records: Optional[int] = None

@router.post("/read", response_model=ResponseModel)
async def read_data(request: ReadRequest):
    """Read data - no file/sheet name needed!"""
    try:
        predictor = get_excel_predictor()
        
        # Predict location using ML
        prediction = predictor.predict_location(request.column_name, request.column_value)
        
        if prediction['confidence'] < 0.3:
            return ResponseModel(
                success=False,
                message="❌ No confident prediction found. Try different column names or values.",
                predicted_location=prediction
            )
        
        # Get data from predicted location
        file_key = prediction['file']
        sheet_name = prediction['sheet']
        df = predictor.get_dataframe(file_key, sheet_name)
        
        if df is None:
            return ResponseModel(
                success=False,
                message="❌ Predicted location not available",
                predicted_location=prediction
            )
        
        # Find matching column (case insensitive)
        actual_column = None
        for col in df.columns:
            if request.column_name.lower() == col.lower():
                actual_column = col
                break
        
        if not actual_column:
            return ResponseModel(
                success=False,
                message=f"❌ Column '{request.column_name}' not found",
                predicted_location=prediction
            )
        
        # Filter data
        if request.column_value:
            mask = df[actual_column].astype(str).str.lower() == str(request.column_value).lower()
            results = df[mask]
        else:
            results = df
        
        # Convert to list of dictionaries
        data_list = results.replace({pd.NA: None, np.nan: None}).to_dict('records')
        
        return ResponseModel(
            success=True,
            message=f"✅ Found {len(data_list)} matching records",
            data=data_list,
            predicted_location=prediction,
            total_records=len(data_list)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

@router.post("/update", response_model=ResponseModel)
async def update_data(request: UpdateRequest):
    """Update data - system automatically finds where to update"""
    try:
        predictor = get_excel_predictor()
        
        # Predict location
        prediction = predictor.predict_location(request.column_name, request.column_value)
        
        if prediction['confidence'] < 0.4:
            return ResponseModel(
                success=False,
                message="❌ Low confidence for update operation",
                predicted_location=prediction
            )
        
        file_path = get_file_path(prediction['file'])
        file_key = prediction['file']
        sheet_name = prediction['sheet']
        
        if not os.path.exists(file_path):
            return ResponseModel(
                success=False,
                message="❌ Excel file not found",
                predicted_location=prediction
            )
        
        # Load the entire Excel file
        excel_file = pd.ExcelFile(file_path)
        sheets_data = {}
        
        for sheet in excel_file.sheet_names:
            sheets_data[sheet] = excel_file.parse(sheet)
        
        # Get the target dataframe
        df = sheets_data[sheet_name]
        
        # Find actual column names
        actual_column = None
        actual_update_column = None
        
        for col in df.columns:
            col_lower = str(col).lower()
            if request.column_name.lower() == col_lower:
                actual_column = col
            if request.update_column.lower() == col_lower:
                actual_update_column = col
        
        if not actual_column or not actual_update_column:
            return ResponseModel(
                success=False,
                message="❌ Required columns not found",
                predicted_location=prediction
            )
        
        # Update the data
        mask = df[actual_column].astype(str).str.lower() == str(request.column_value).lower()
        update_count = mask.sum()
        
        if update_count == 0:
            return ResponseModel(
                success=False,
                message="❌ No matching records found",
                predicted_location=prediction
            )
        
        # Perform update
        df.loc[mask, actual_update_column] = request.update_value
        sheets_data[sheet_name] = df
        
        # Save back to Excel
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet, data in sheets_data.items():
                data.to_excel(writer, sheet_name=sheet, index=False)
        
        # Reload the updated data
        predictor.load_excel_file(file_path, file_key)
        
        return ResponseModel(
            success=True,
            message=f"✅ Successfully updated {update_count} record(s)",
            predicted_location=prediction
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating data: {str(e)}")

@router.post("/insert", response_model=ResponseModel)
async def insert_data(request: InsertRequest):
    """Insert new data - system finds the right location"""
    try:
        if not request.data:
            return ResponseModel(success=False, message="❌ No data provided")
        
        predictor = get_excel_predictor()
        
        # Use first column for prediction
        first_column = list(request.data.keys())[0]
        first_value = str(request.data[first_column])
        
        prediction = predictor.predict_location(first_column, first_value)
        
        if prediction['confidence'] < 0.2:
            return ResponseModel(
                success=False,
                message="❌ Cannot determine where to insert data",
                predicted_location=prediction
            )
        
        file_path = get_file_path(prediction['file'])
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
        
        # Fill missing columns with None
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
        predictor.load_excel_file(file_path, file_key)
        
        return ResponseModel(
            success=True,
            message="✅ Successfully inserted new record",
            predicted_location=prediction
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting data: {str(e)}")

@router.post("/delete", response_model=ResponseModel)
async def delete_data(request: DeleteRequest):
    """Delete data - system finds where to delete"""
    try:
        predictor = get_excel_predictor()
        
        prediction = predictor.predict_location(request.column_name, request.column_value)
        
        if prediction['confidence'] < 0.4:
            return ResponseModel(
                success=False,
                message="❌ Low confidence for delete operation",
                predicted_location=prediction
            )
        
        file_path = get_file_path(prediction['file'])
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
                message="❌ Column not found",
                predicted_location=prediction
            )
        
        # Delete matching rows
        mask = df[actual_column].astype(str).str.lower() == str(request.column_value).lower()
        delete_count = mask.sum()
        
        if delete_count == 0:
            return ResponseModel(
                success=False,
                message="❌ No matching records found",
                predicted_location=prediction
            )
        
        df = df[~mask]
        sheets_data[sheet_name] = df
        
        # Save back to Excel
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet, data in sheets_data.items():
                data.to_excel(writer, sheet_name=sheet, index=False)
        
        # Reload data
        predictor.load_excel_file(file_path, file_key)
        
        return ResponseModel(
            success=True,
            message=f"✅ Successfully deleted {delete_count} record(s)",
            predicted_location=prediction
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting data: {str(e)}")

@router.post("/search")
async def search_data(request: SearchRequest):
    """Search across all Excel files and sheets"""
    predictor = get_excel_predictor()
    results = []
    
    for file_key, sheets in predictor.excel_data.items():
        for sheet_name, df in sheets.items():
            # Search in all columns
            for col in df.columns:
                if df[col].dtype == 'object':  # Text columns
                    mask = df[col].astype(str).str.lower().str.contains(
                        request.search_text.lower(), na=False
                    )
                    matching_rows = df[mask]
                    
                    for _, row in matching_rows.iterrows():
                        results.append({
                            'file': file_key,
                            'sheet': sheet_name,
                            'column': col,
                            'value': row[col],
                            'full_record': row.replace({pd.NA: None, np.nan: None}).to_dict()
                        })
                        
                        if len(results) >= request.max_results:
                            break
                
                if len(results) >= request.max_results:
                    break
            if len(results) >= request.max_results:
                break
    
    return {
        "success": True,
        "search_text": request.search_text,
        "results_found": len(results),
        "results": results[:request.max_results]
    }