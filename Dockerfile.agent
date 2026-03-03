FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pipenv

COPY Pipfile Pipfile.lock* ./
RUN pipenv install --system --deploy --ignore-pipfile 2>/dev/null || pipenv install --system

COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
