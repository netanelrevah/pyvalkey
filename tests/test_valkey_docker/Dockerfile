FROM debian:bookworm

RUN apt update && apt install git tcl -y
RUN git clone https://github.com/valkey-io/valkey

WORKDIR /valkey

ENTRYPOINT ["./runtest"]