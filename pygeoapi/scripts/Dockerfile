FROM python:3.11

# set working directory
WORKDIR /app

# Change group ownership and set permissions
RUN chgrp -R 0 /app && chmod -R g=u /app

# Install requirements
COPY ../requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire pygeoapi package
COPY pygeoapi/scripts ./pygeoapi/scripts

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-u", "pygeoapi/scripts/main.py"]
