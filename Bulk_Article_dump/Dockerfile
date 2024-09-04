# 1. Use an official Python runtime as a parent image
FROM python:3.9-slim

# 2. Set the working directory in the container
WORKDIR /app

# 3. Copy the current directory contents into the container at /app
COPY . /app

# 4. Install any necessary dependencies
RUN pip install --no-cache-dir mysql-connector-python fastapi uvicorn tqdm

# 5. Run your script when the container launches
CMD ["python", "FastAPI ArticleDump.py"]