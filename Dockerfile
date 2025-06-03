# Используем Python 3.12
FROM python:3.12.8

# Открываем порт для приложения
EXPOSE 80

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Enables env file
ENV APP_ENV=development
#add pyppi mirror to config
COPY pip.conf /etc/xdg/pip/pip.conf
# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /app
COPY . /app

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["gunicorn", "--bind", "0.0.0.0:80", "-k", "uvicorn.workers.UvicornWorker", "landuse_app.fastapi_init:app"]
