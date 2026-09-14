FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .
ENV NARAZ_HOST=0.0.0.0
ENV NARAZ_PORT=8080
ENV NARAZ_WS_PORT=8081
EXPOSE 8080 8081
CMD ["python","server.py"]
