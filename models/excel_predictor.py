# models/excel_predictor.py
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import os

class ExcelPredictor:
    def __init__(self):
        self.excel_data = {}
        self.vectorizer = TfidfVectorizer(stop_words='english', max_features=1000)
        self.column_index = defaultdict(list)  # column_name -> [(file, sheet, confidence)]
        self.value_patterns = defaultdict(dict)
        self.semantic_models = {}
        
    def load_excel_file(self, file_path: str, file_key: str):
        """Load a single Excel file and extract patterns"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        excel_file = pd.ExcelFile(file_path)
        self.excel_data[file_key] = {}
        
        for sheet_name in excel_file.sheet_names:
            try:
                df = excel_file.parse(sheet_name)
                # Clean the dataframe
                df = self._clean_dataframe(df)
                self.excel_data[file_key][sheet_name] = df
                
                # Index columns and values
                self._index_sheet_data(file_key, sheet_name, df)
                
            except Exception as e:
                print(f"Error loading sheet {sheet_name} from {file_key}: {e}")
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize dataframe"""
        # Clean column names
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]
        
        # Remove completely empty rows and columns
        df = df.dropna(how='all')
        df = df.loc[:, ~df.columns.str.contains('^unnamed')]
        
        return df
    
    def _index_sheet_data(self, file_key: str, sheet_name: str, df: pd.DataFrame):
        """Index column names and value patterns for a sheet"""
        for column in df.columns:
            column_lower = column.lower()
            
            # Calculate confidence based on column properties
            confidence = self._calculate_column_confidence(column_lower, df[column])
            
            # Add to column index
            self.column_index[column_lower].append({
                'file': file_key,
                'sheet': sheet_name,
                'confidence': confidence,
                'data_type': self._infer_data_type(df[column]),
                'unique_values': df[column].nunique(),
                'sample_values': df[column].dropna().astype(str).str.lower().tolist()[:5]
            })
            
            # Index value patterns
            self._index_value_patterns(file_key, sheet_name, column, df[column])
    
    def _calculate_column_confidence(self, column_name: str, series: pd.Series) -> float:
        """Calculate confidence score for a column"""
        confidence = 0.0
        
        # Base confidence for common column patterns
        common_patterns = {
            'name': 0.9, 'id': 0.9, 'date': 0.9, 'status': 0.9, 'project': 0.8,
            'email': 0.9, 'phone': 0.8, 'address': 0.7, 'city': 0.7, 'country': 0.7
        }
        
        for pattern, score in common_patterns.items():
            if pattern in column_name:
                confidence = max(confidence, score)
        
        # Boost confidence if column has high uniqueness (likely primary key)
        uniqueness = series.nunique() / len(series) if len(series) > 0 else 0
        if uniqueness > 0.8:
            confidence += 0.2
        
        # Boost confidence if column has mixed data types (likely important)
        if series.dtype == 'object' and len(series) > 0:
            sample = series.dropna().astype(str)
            if any(len(str(x)) > 10 for x in sample.head(10)):
                confidence += 0.1
        
        return min(confidence, 1.0) if confidence > 0 else 0.5
    
    def _infer_data_type(self, series: pd.Series) -> str:
        """Infer the data type of a series"""
        if series.dtype in ['int64', 'float64']:
            return 'numeric'
        
        sample = series.dropna().astype(str)
        if len(sample) == 0:
            return 'unknown'
        
        # Check for date patterns
        date_pattern = r'\d{4}-\d{2}-\d{2}'
        if any(re.match(date_pattern, str(x)) for x in sample.head(5)):
            return 'date'
        
        # Check for boolean patterns
        bool_pattern = r'^(true|false|yes|no|1|0)$'
        if all(re.match(bool_pattern, str(x).lower()) for x in sample.head(5)):
            return 'boolean'
        
        return 'text'
    
    def _index_value_patterns(self, file_key: str, sheet_name: str, column: str, series: pd.Series):
        """Index patterns in column values"""
        key = f"{file_key}::{sheet_name}::{column}"
        self.value_patterns[key] = {
            'common_patterns': self._extract_common_patterns(series),
            'data_distribution': self._get_data_distribution(series),
            'avg_length': series.astype(str).str.len().mean() if len(series) > 0 else 0
        }
    
    def _extract_common_patterns(self, series: pd.Series) -> List[str]:
        """Extract common patterns from series values"""
        patterns = []
        sample = series.dropna().astype(str).head(20)
        
        for value in sample:
            # Extract alphanumeric patterns
            alphanum = re.findall(r'[A-Za-z0-9]+', value)
            patterns.extend(alphanum)
            
            # Extract special patterns (dates, codes, etc.)
            if re.match(r'^[A-Z]{2,}\d+$', value):
                patterns.append('CODE_PATTERN')
            elif re.match(r'\d{4}-\d{2}-\d{2}', value):
                patterns.append('DATE_PATTERN')
            elif re.match(r'^\d+$', value):
                patterns.append('NUMERIC_PATTERN')
        
        return list(set(patterns))
    
    def _get_data_distribution(self, series: pd.Series) -> Dict:
        """Get basic data distribution"""
        return {
            'total_count': len(series),
            'null_count': series.isnull().sum(),
            'unique_count': series.nunique(),
            'most_frequent': series.mode().iloc[0] if len(series.mode()) > 0 else None
        }
    
    def predict_location(self, column_name: str, column_value: str = None) -> Dict:
        """Predict the best file and sheet for the given column and value"""
        column_name_lower = column_name.strip().lower()
        column_value_str = str(column_value).lower() if column_value else None
        
        # Get candidate locations for this column
        candidates = self.column_index.get(column_name_lower, [])
        
        if not candidates:
            # Try fuzzy matching for column names
            candidates = self._fuzzy_match_columns(column_name_lower)
        
        if not candidates:
            return {
                'file': 'unknown',
                'sheet': 'unknown',
                'confidence': 0.0,
                'message': 'No matching column found in any file'
            }
        
        # Score candidates based on value matching
        scored_candidates = []
        for candidate in candidates:
            score = candidate['confidence']
            
            # Boost score if value matches patterns in this column
            if column_value_str:
                value_score = self._calculate_value_match_score(candidate, column_value_str)
                score = score * 0.6 + value_score * 0.4
            
            scored_candidates.append((candidate, score))
        
        # Sort by score and get best match
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        best_candidate, best_score = scored_candidates[0]
        
        return {
            'file': best_candidate['file'],
            'sheet': best_candidate['sheet'],
            'confidence': round(best_score, 3),
            'data_type': best_candidate['data_type'],
            'message': f"Found in {best_candidate['file']} - {best_candidate['sheet']}"
        }
    
    def _fuzzy_match_columns(self, target_column: str) -> List[Dict]:
        """Find columns with similar names using fuzzy matching"""
        candidates = []
        target_words = set(target_column.split('_'))
        
        for column_name, locations in self.column_index.items():
            column_words = set(column_name.split('_'))
            
            # Calculate word overlap
            overlap = len(target_words & column_words)
            similarity = overlap / max(len(target_words), len(column_words))
            
            if similarity > 0.3:  # Threshold for fuzzy matching
                for location in locations:
                    candidate = location.copy()
                    candidate['confidence'] = candidate['confidence'] * similarity
                    candidates.append(candidate)
        
        return candidates
    
    def _calculate_value_match_score(self, candidate: Dict, value: str) -> float:
        """Calculate how well the value matches patterns in the candidate column"""
        key = f"{candidate['file']}::{candidate['sheet']}::{candidate.get('column_name', '')}"
        patterns = self.value_patterns.get(key, {})
        
        score = 0.0
        
        # Check if value exists in sample values
        if value in candidate.get('sample_values', []):
            score += 0.8
        
        # Check pattern matching
        common_patterns = patterns.get('common_patterns', [])
        for pattern in common_patterns:
            if pattern in value.upper():
                score += 0.2
                break
        
        # Check data type consistency
        inferred_type = self._infer_data_type(pd.Series([value]))
        if inferred_type == candidate['data_type']:
            score += 0.3
        
        return min(score, 1.0)
    
    def get_dataframe(self, file_key: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """Get dataframe for a specific file and sheet"""
        return self.excel_data.get(file_key, {}).get(sheet_name)
    
    def update_dataframe(self, file_key: str, sheet_name: str, df: pd.DataFrame):
        """Update dataframe and reindex"""
        if file_key in self.excel_data and sheet_name in self.excel_data[file_key]:
            self.excel_data[file_key][sheet_name] = df
            # Reindex the updated sheet
            self._index_sheet_data(file_key, sheet_name, df)