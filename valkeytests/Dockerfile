FROM python:3.13.1-bookworm

RUN apt-get install tcl
RUN git clone https://github.com/valkey-io/valkey

RUN curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /code
COPY . /code
RUN /root/.local/bin/poetry install

WORKDIR /
COPY valkeytests/entrypoint.sh /
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/bin/bash"]