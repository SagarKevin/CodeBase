# Base Image
FROM python:3.8

# Select working directory to place files
WORKDIR /opt/files

# Copy script and requirements files in directory
COPY requirements.txt /opt/files/requirements.txt
COPY main.py /opt/files/main.py

# Install Cron for running scheduled job
RUN apt-get update && apt-get -y install cron vim

RUN apt-get update \
 && apt-get install -y apt-transport-https \
 && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
 && curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install msodbcsql17 unixodbc-dev -y 

# Create Crontab and save
RUN echo "* * * * * root /usr/local/bin/python /opt/files/main.py >> /var/log/cron.log 2>&1" >> /etc/cron.d/cronschedule

# Set permissions on Crontab file
RUN chmod 0644 /etc/cron.d/cronschedule

# Install all required packages
RUN pip install -r /opt/files/requirements.txt

# Create empty log file
RUN touch /var/log/cron.log
CMD cron && tail -f /var/log/cron.log