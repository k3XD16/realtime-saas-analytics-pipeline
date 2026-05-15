FROM apache/airflow:3.2.1

# Copy requirements file
COPY requirements.txt .

# Install dependencies as the airflow user
USER airflow
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --upgrade "sqlalchemy>=2.1.0" "flask-sqlalchemy>=3.1.0" && \
    pip install --no-cache-dir -r requirements.txt
