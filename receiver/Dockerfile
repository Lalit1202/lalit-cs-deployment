FROM python:3.9-slim
WORKDIR /app
COPY cdr_receiver.py .
RUN pip install flask
CMD ["python", "-u", "cdr_receiver.py"]
