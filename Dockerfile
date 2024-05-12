FROM python:3.11

# set working directory
WORKDIR /app

# Install requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application to the server
COPY src/main.py .
COPY src/edit_db.py .
COPY src/template_resource.txt .
COPY src/lookup_table_columns.csv .
COPY src/process_data.py .
COPY src/edit_config.py .
COPY src/load_data.py .

CMD python main.py
