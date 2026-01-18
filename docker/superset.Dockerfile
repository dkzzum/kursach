FROM apache/superset:latest

USER root

# 1. Ставим системные либы
RUN apt-get update && \
    apt-get install -y libsasl2-dev libsasl2-modules-gssapi-mit gcc curl && \
    apt-get clean

# 2. ФРАНКЕНШТЕЙН: Скачиваем установщик pip и запускаем его
# Это насильно вставит pip в текущий (даже поломанный) python
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py && \
    rm get-pip.py

# 3. Теперь pip точно есть. Ставим библиотеки
RUN pip install --no-cache-dir pyhive thrift thrift_sasl

# 4. Даем полные права на все пакеты (чтобы superset не ныл)
RUN chmod -R 777 /app/.venv/lib/python3.9/site-packages || \
    chmod -R 777 /usr/local/lib/python3.9/site-packages || \
    echo "Пути могут отличаться, но pip отработал"

USER superset