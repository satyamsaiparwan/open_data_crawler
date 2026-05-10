from flask import Flask, request, jsonify, render_template, Response
import datetime
import pandas as pd
import requests
import io
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base

engine = create_engine('sqlite:///government_data.db')
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

class GovernmentData(Base):
    __tablename__ = 'government_data'
    id = Column(Integer, primary_key=True)
    category = Column(String(100), nullable=False)
    identifier = Column(String(100), nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'category': self.category,
            'identifier': self.identifier,
            'value': self.value,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }

def init_db():
    Base.metadata.create_all(bind=engine)

app = Flask(__name__, template_folder='.')

init_db()

@app.route('/')
def dashboard():
    return render_template('frontend.html')

@app.route('/api/data', methods=['GET'])
def get_data():
    sort_by = request.args.get('sort_by', 'timestamp')
    order = request.args.get('order', 'desc')
    search = request.args.get('search', '').strip()
    category_filter = request.args.get('category', '').strip()
    
    valid_sorts = {
        'category': GovernmentData.category,
        'identifier': GovernmentData.identifier,
        'value': GovernmentData.value,
        'timestamp': GovernmentData.timestamp
    }
    
    sort_column = valid_sorts.get(sort_by, GovernmentData.timestamp)
    
    query = GovernmentData.query

    if search:
        search_term = f"%{search}%"
        query = query.filter(
            GovernmentData.category.ilike(search_term) |
            GovernmentData.identifier.ilike(search_term)
        )
        
    if category_filter and category_filter.lower() != 'all':
        query = query.filter(GovernmentData.category.ilike(f"%{category_filter}%"))
    
    if order == 'asc':
        data = query.order_by(sort_column.asc()).all()
    else:
        data = query.order_by(sort_column.desc()).all()
        
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

@app.route('/api/health_metrics', methods=['GET'])
def health_metrics():
    total_records = GovernmentData.query.count()
    accuracy = 98.5 # Mock metric for demo
    last_record = GovernmentData.query.order_by(GovernmentData.timestamp.desc()).first()
    last_update = last_record.timestamp.strftime("%Y-%m-%d %H:%M:%S") if last_record else "Never"
    
    return jsonify({
        "total_records": total_records,
        "accuracy": accuracy,
        "last_update": last_update
    })

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
                    try:
                        tables = pd.read_html(response.text)
                        if tables:
                            df = tables[0]
                    except ValueError:
                        pass
                    if df is None:
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
                            return jsonify({"status": "error", "message": "Crawler could not find any tables or CSV/JSON data links on the provided webpage."}), 400
                
                if ('pdf' in content_type or url.endswith('.pdf')) and df is None:
                    import pdfplumber
                    import tempfile
                    import os
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_pdf:
                        temp_pdf.write(response.content)
                        temp_pdf_path = temp_pdf.name
                    with pdfplumber.open(temp_pdf_path) as pdf:
                        all_tables = []
                        for page in pdf.pages:
                            table = page.extract_table()
                            if table:
                                all_tables.extend(table)
                    os.remove(temp_pdf_path)
                    if all_tables:
                        df = pd.DataFrame(all_tables[1:], columns=all_tables[0])
                    else:
                        return jsonify({"status": "error", "message": "No tables found in PDF."}), 400

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
            
        initial_rows = len(df)
            
        # Get User Rules from form
        remove_empty = request.form.get('remove_empty', 'true') == 'true'
        fill_zero = request.form.get('fill_zero', 'false') == 'true'
        convert_currency = request.form.get('convert_currency', 'true') == 'true'
            
        empty_rows_removed = 0
        if remove_empty:
            df = df.dropna(how='all') # Drop fully empty rows
            empty_rows_removed = initial_rows - len(df)
        
        columns = df.columns.tolist()
        val_col = None
        
        # Identify potential value column early to coerce
        for col in columns:
            col_lower = str(col).lower()
            if any(k in col_lower for k in ['val', 'amount', 'price', 'count', 'total', 'population', 'num', 'metric']):
                val_col = col
                if convert_currency:
                    df[val_col] = df[val_col].replace('[\$,]', '', regex=True)
                df[val_col] = pd.to_numeric(df[val_col], errors='coerce')
                break

        # If val_col not found by keyword, just coerce anything that looks numeric
        if not val_col:
            for col in columns:
                if convert_currency:
                    if df[col].dtype == object:
                        df[col] = df[col].replace('[\$,]', '', regex=True)
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
                
        # Handle Missing Values
        len_before_val_drop = len(df)
        if fill_zero:
            df[val_col] = df[val_col].fillna(0.0)
            df[cat_col] = df[cat_col].fillna('Unknown')
            df[id_col] = df[id_col].fillna('Unknown')
            invalid_rows_removed = 0
        else:
            df = df.dropna(subset=[val_col])
            invalid_rows_removed = len_before_val_drop - len(df)
            
        # Automated Data Categorization (Domain mapping)
        def assign_domain(row, cat_col_name, val_col_name):
            text = str(row[cat_col_name]).lower() + " " + str(val_col_name).lower()
            if any(k in text for k in ['budget', 'amount', 'finance', 'cost', 'revenue', 'price', 'tax']):
                return 'Finance'
            if any(k in text for k in ['population', 'demographic', 'age', 'citizen']):
                return 'Demographics'
            if any(k in text for k in ['road', 'bridge', 'water', 'maintenance', 'project', 'infrastructure']):
                return 'Public Works'
            return str(row[cat_col_name])[:100]
        
        added_count = 0
        for index, row in df.iterrows():
            try:
                assigned_category = assign_domain(row, cat_col, val_col)
                new_entry = GovernmentData(
                    category=assigned_category,
                    identifier=str(row[id_col])[:100],
                    value=float(row[val_col])
                )
                db_session.add(new_entry)
                added_count += 1
            except Exception as e:
                invalid_rows_removed += 1
                continue # Skip invalid rows
                
        db_session.commit()
        
        # Email Notification Trigger (Dummy)
        print(f"--- EMAIL NOTIFICATION TRIGGERED ---")
        print(f"To: admin@municipality.gov")
        print(f"Subject: New Data Crawl Completed")
        print(f"Body: Successfully processed {initial_rows} rows. Added {added_count} records.")
        print(f"------------------------------------")
        
        return jsonify({
            "status": "success", 
            "message": f"Successfully processed {initial_rows} rows. Added {added_count} records. Removed {empty_rows_removed} empty rows and fixed/dropped {invalid_rows_removed} invalid entries.",
            "report": {
                "initial_rows": initial_rows,
                "added_count": added_count,
                "empty_removed": empty_rows_removed,
                "invalid_removed": invalid_rows_removed
            }
        }), 201

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
