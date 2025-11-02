<!-- 1. Read Data
bash
curl -X POST "http://localhost:8000/api/v1/read" \
-H "Content-Type: application/json" \
-d '{
  "column_name": "Project_Name",
  "column_value": "Tarsus Opthalmology TestIn"
}'

2. Update Status
bash
curl -X POST "http://localhost:8000/api/v1/update" \
-H "Content-Type: application/json" \
-d '{
  "column_name": "Project_Name",
  "column_value": "Tarsus Opthalmology TestIn", 
  "update_column": "Status",
  "update_value": "Completed"
}'


3. Insert New Record
bash
curl -X POST "http://localhost:8000/api/v1/insert" \
-H "Content-Type: application/json" \
-d '{
  "data": {
    "Name": "John Doe",
    "UserID": "U1234567",
    "Location": "Bangalore",
    "Status": "Active"
  }
}'


4. Delete Record
bash
curl -X POST "http://localhost:8000/api/v1/delete" \
-H "Content-Type: application/json" \
-d '{
  "column_name": "Name", 
  "column_value": "John Doe"
}'

5. Search Across All Files
bash
curl -X POST "http://localhost:8000/api/v1/search" \
-H "Content-Type: application/json" \
-d '{
  "search_text": "Complete",
  "max_results": 5
}' -->