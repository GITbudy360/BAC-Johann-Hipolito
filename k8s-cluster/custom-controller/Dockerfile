FROM python:3.9-slim
WORKDIR /app
RUN pip install --no-cache-dir kubernetes requests
COPY controller.py .
# The -u flag prevents Python from buffering the stdout logs
CMD ["python", "-u", "controller.py"]