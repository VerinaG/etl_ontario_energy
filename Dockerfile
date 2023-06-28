# Set base image (host OS)
FROM apache/airflow:2.5.3

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any dependencies
RUN pip install -r requirements.txt

# Append Scripts folder to PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/scripts"