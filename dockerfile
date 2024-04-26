FROM python:3.11

# set working directory
WORKDIR /app

# Install requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application to the server
COPY scripts/main.py .
COPY scripts/edit_db.py .
COPY scripts/template_resource.txt .
COPY scripts/lookup_table_columns.csv .
COPY scripts/process_data.py .
COPY scripts/edit_config.py .
COPY pygeoapi-config.yml .

CMD python main.py