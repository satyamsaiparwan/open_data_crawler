import requests
import io

csv_data = """id,category,amount,date
1,Budget,50000,2023-01-01
2,Population,120000,2023-01-02
3,Maintenance,450.50,2023-01-03
4,EmptyRow,,
5,Invalid,notanumber,2023-01-04
"""

files = {'file': ('test.csv', io.StringIO(csv_data), 'text/csv')}
response = requests.post('http://127.0.0.1:5000/api/upload_data', files=files)
print("Upload Response:", response.status_code, response.json())

response_get = requests.get('http://127.0.0.1:5000/api/data')
print("Get Data Response:", response_get.status_code)
for item in response_get.json():
    print(item)
