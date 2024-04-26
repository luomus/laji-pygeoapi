FROM python:3.11

# set working directory
WORKDIR /app

# Install Cron
RUN apt-get update
RUN apt-get -y install cron

# Install requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application to the server
COPY scripts/create_datadump_from_url.py .
COPY scripts/template_resource.txt .
COPY scripts/lookup_table_columns.csv .
COPY scripts/test.py .

# Copy hello-cron file to the cron.d directory
COPY hello-cron /etc/cron.d/hello-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/hello-cron

# Apply cron job
RUN crontab /etc/cron.d/hello-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run the job
CMD /etc/init.d/cron start && tail -f /var/log/cron.log
