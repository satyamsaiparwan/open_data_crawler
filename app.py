from flask import Flask, request, jsonify, render_template, Response
from database import init_db, db_session, GovernmentData
import datetime
import pandas as pd
import requests
import io
app = Flask(__name__)

init_db()

@app.route('/')
def dashboard():
    try:
        GovernmentData.query.delete()
        db_session.commit()
    except Exception:
        db_session.rollback()
    return render_template('index.html')

@app.route('/api/data', methods=['GET'])
def get_data():
    sort_by = request.args.get('sort_by', 'timestamp')
    order = request.args.get('order', 'desc')
    
    valid_sorts = {
        'category': GovernmentData.category,
        'identifier': GovernmentData.identifier,
        'value': GovernmentData.value,
        'timestamp': GovernmentData.timestamp
    }
    
    sort_column = valid_sorts.get(sort_by, GovernmentData.timestamp)
    
    if order == 'asc':
        data = GovernmentData.query.order_by(sort_column.asc()).all()
    else:
        data = GovernmentData.query.order_by(sort_column.desc()).all()
        
    return jsonify([d.to_dict() for d in data])

@app.route('/api/data', methods=['POST'])
def add_data():
    try:
        req_data = request.get_json()
        category = str(req_data.get('category', '')).strip()
        identifier = str(req_data.get('identifier', '')).strip()
        
        if not category or not identifier:
            return jsonify({"status": "error", "message": "Category and Identifier are required."}), 400
            
        value = float(req_data.get('value', 0))
        
        # Remove old data so nothing is mixed
        GovernmentData.query.delete()
        
        new_entry = GovernmentData(
            category=category,
            identifier=identifier,
            value=value
        )
        
        db_session.add(new_entry)
        db_session.commit()
        
        return jsonify({"status": "success", "message": "Data structured!"}), 201
        
    except ValueError:
        return jsonify({"status": "error", "message": "Invalid number format."}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/upload_data', methods=['POST'])
def upload_data():
    try:
        df = None
        if 'file' in request.files and request.files['file'].filename != '':
            file = request.files['file']
            filename = file.filename.lower()
            if filename.endswith('.csv'):
                df = pd.read_csv(file)
            elif filename.endswith('.json'):
                df = pd.read_json(file)
            else:
                return jsonify({"status": "error", "message": "Unsupported file format. Please upload CSV or JSON."}), 400
        else:
            url = request.form.get('url', '').strip()
            if not url and request.is_json:
                url = request.get_json().get('url', '').strip()
                
            if url:
                response = requests.get(url)
                response.raise_for_status()
                content_type = response.headers.get('content-type', '').lower()
                
                # Crawler Logic
                if 'html' in content_type:
                    from bs4 import BeautifulSoup
                    from urllib.parse import urljoin
                    soup = BeautifulSoup(response.text, 'html.parser')
                    link = soup.find('a', href=lambda href: href and (href.endswith('.csv') or href.endswith('.json')))
                    if link:
                        crawler_url = urljoin(url, link['href'])
                        response = requests.get(crawler_url)
                        response.raise_for_status()
                        content_type = response.headers.get('content-type', '').lower()
                        url = crawler_url
                    else:
                        return jsonify({"status": "error", "message": "Crawler could not find any CSV or JSON data links on the provided webpage."}), 400

                if 'csv' in content_type or url.endswith('.csv'):
                    df = pd.read_csv(io.StringIO(response.text))
                elif 'json' in content_type or url.endswith('.json'):
                    df = pd.read_json(io.StringIO(response.text))
                else:
                    try:
                        df = pd.read_csv(io.StringIO(response.text))
                    except:
                        try:
                            df = pd.read_json(io.StringIO(response.text))
                        except:
                            return jsonify({"status": "error", "message": "Could not parse data from URL. Ensure it is CSV or JSON."}), 400
            else:
                return jsonify({"status": "error", "message": "No file or URL provided."}), 400

        if df is None or df.empty:
            return jsonify({"status": "error", "message": "No data found or file is empty."}), 400
            
        # Automatic cleaning
        df = df.dropna(how='all') # Drop fully empty rows
        
        columns = df.columns.tolist()
        val_col = None
        
        # Identify potential value column early to coerce
        for col in columns:
            col_lower = str(col).lower()
            if any(k in col_lower for k in ['val', 'amount', 'price', 'count', 'total', 'population', 'num', 'metric']):
                val_col = col
                df[val_col] = pd.to_numeric(df[val_col], errors='coerce')
                break

        # If val_col not found by keyword, just coerce anything that looks numeric
        if not val_col:
            for col in columns:
                coerced = pd.to_numeric(df[col], errors='coerce')
                # If more than 50% are numbers, consider it numeric
                if coerced.notna().mean() > 0.5:
                    df[col] = coerced

        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        string_cols = df.select_dtypes(exclude=['number']).columns.tolist()
        
        if not numeric_cols:
            return jsonify({"status": "error", "message": "No numeric 'value' column found in data."}), 400
            
        if not val_col:
            val_col = numeric_cols[0]
            
        cat_col = string_cols[0] if len(string_cols) > 0 else numeric_cols[0]
        id_col = string_cols[1] if len(string_cols) > 1 else (string_cols[0] if len(string_cols) > 0 else numeric_cols[0])
        
        # Refine mapping with keyword matching for category/id
        for col in columns:
            col_lower = str(col).lower()
            if any(k in col_lower for k in ['cat', 'type', 'dept', 'group', 'class']):
                cat_col = col
            if any(k in col_lower for k in ['id', 'name', 'title', 'city', 'district', 'state', 'region']):
                id_col = col
                
        # Drop rows where the value is missing
        df = df.dropna(subset=[val_col])
        
        # Remove old data before bulk inserting new dataset
        GovernmentData.query.delete()
        
        added_count = 0
        for index, row in df.iterrows():
            try:
                new_entry = GovernmentData(
                    category=str(row[cat_col])[:100],
                    identifier=str(row[id_col])[:100],
                    value=float(row[val_col])
                )
                db_session.add(new_entry)
                added_count += 1
            except Exception as e:
                continue # Skip invalid rows
                
        db_session.commit()
        return jsonify({"status": "success", "message": f"Successfully processed and added {added_count} rows!"}), 201

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/export', methods=['GET'])
def export_data():
    try:
        data = GovernmentData.query.all()
        if not data:
            return "No data available to export.", 404
            
        df = pd.DataFrame([d.to_dict() for d in data])
        if not df.empty and 'id' in df.columns:
            df = df.drop(columns=['id'])
            
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Municipal Data')
        output.seek(0)
        
        return Response(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-disposition": "attachment; filename=municipal_data.xlsx"}
        )
    except Exception as e:
        return str(e), 500

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

if __name__ == '__main__':
    print("Government Sorting Dashboard Running!")
    app.run(debug=True, port=5000)
