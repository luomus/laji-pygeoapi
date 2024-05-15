FROM python:3.11

# set working directory
WORKDIR /app
RUN chown -Rh python:python /app
USER python

# Install requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application to the server
COPY --chown=python:python src/main.py .
COPY --chown=python:python src/edit_db.py .
COPY --chown=python:python src/template_resource.txt .
COPY --chown=python:python src/lookup_table_columns.csv .
COPY --chown=python:python src/process_data.py .
COPY --chown=python:python src/edit_config.py .
COPY --chown=python:python src/load_data.py .

# Copy test data
COPY --chown=python:python test_data/10000_virva_data.json .
COPY --chown=python:python test_data/taxon-export.csv .

ENTRYPOINT ["python", "main.py"]