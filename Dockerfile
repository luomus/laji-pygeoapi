FROM python:3.11

# set working directory
WORKDIR /app

RUN chgrp -R 0 /app && chmod -R g=u /app

# Install requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application to the server
COPY src/ .

ENTRYPOINT ["python", "-u", "main.py"]
