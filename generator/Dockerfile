FROM python:3.9-slim
WORKDIR /app
COPY cdr_generator.py .
RUN pip install requests
CMD ["python", "cdr_generator.py"]
