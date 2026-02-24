# Use an official Python runtime as the base image
FROM python:3.12-slim


# Set the working directory in the container
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && \
    rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

# Copy the requirements file into the container
COPY pyproject.toml .

# Install the required packages
RUN uv sync

# Copy the rest of the application code
COPY main.py .

# Set the command to run your application
CMD ["uv", "run", "main.py"]
