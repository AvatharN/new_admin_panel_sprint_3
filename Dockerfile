FROM python:3.11-slim

# Выберите папку, в которой будут размещаться файлы проекта внутри контейнера
WORKDIR /opt/app

# Заведите необходимые переменные окружения
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Скопируйте в контейнер файлы, которые редко меняются
COPY /etl/etl.py .
COPY requirements.txt .

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt


# Укажите, как запускать ваш сервис
CMD ["python", "etl.py"]