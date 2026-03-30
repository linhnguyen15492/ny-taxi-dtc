FROM python:3.12-slim

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Set the working directory inside the container
# WORKDIR /opt

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip install uv

# Copy the requirements file and install any dependencies
COPY requirements.txt .

# RUN pip install --no-cache-dir -r requirements.txt
RUN uv pip install --system -r requirements.txt

CMD ["airflow", "standalone"]

