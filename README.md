# LSEP-coding-challenge

## Overview
This project is 2 tier application namely; an ETL (Extract, Transform, Load) pipeline designed to process and load tweet data into a PostgreSQL database. The pipeline includes modules for extracting data from a JSON file, transforming it, and loading it into the database. Additionally, there is a Flask-based web tier to interact with the data.

## Project Structure
```
.
├── etl-pipeline
│   ├── config.py
│   ├── db_utils.py
│   ├── etl_extract.py
│   ├── etl_transform.py
│   ├── etl_load.py
│   └── app.py
├── web-tier
│   ├── app
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── routes.py
│   │   ├── .gitignore
│   ├── run.py
├── .gitignore
├── README.md
└── requirements.txt
```

## Prerequisites
- Python 3.8+
- PostgreSQL
- Virtual Environment (optional but recommended)

## Setup

### Step 1: Clone the Repository
```bash
git clone https://github.com/mugemanebertin2001/LSEP-coding-challenge.git
cd LSEP-coding-challenge
```

### Step 2: Set Up Virtual Environment
#### On Windows
```bash
python -m venv .venv
.venv\Scripts\activate
```

#### On macOS/Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Configure Database
Ensure your PostgreSQL server is running and accessible. Modify `etl-pipeline/config.py` and `web-tier/config.py`with your database credentials. For example:
```python
# config.py
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'yourdbname',
    'user': 'yourdbuser',
    'password': 'yourdbpassword'
}
```

### Step 5: Run the ETL Pipeline
```bash
python etl-pipeline/app.py
```

### Step 6: Run the Web Application
```bash
cd web-tier
python app.py
```

### Accessing the Web Application
The web application will be accessible at `http://127.0.0.1:5000/`.

## API Endpoints

## initial route
```
GET /
```
welcome message

## Trigger etl to load data
```
GET /run_etl

```
This end point will return msg after reloading data into db

### Query Tweets
```
GET /q2?user_id=<user_id>&type=<type>&phrase=<phrase>&hashtag=<hashtag>
```
Queries tweets based on user_id, type, phrase, and hashtag. Any of these parameters can be omitted.

## Additional Notes
- Ensure PostgreSQL is properly installed and running on your machine.
- Modify the JSON file path in `etl-pipeline/app.py` if necessary:
  ```python
  file_path = os.path.join('D:', 'query2_ref.json')  # Modify as needed
  ```

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Contact
For any inquiries or support, please contact [bertin.m2001@gmail.com].
