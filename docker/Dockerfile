FROM 3.7.3-stretch

RUN apt-get update && \
    apt-get install \\
        libpq-dev python3-dev \
        git \
        tidy

RUN git clone //github.com/humlab-sead/sead_clearinghouse.git && \
    mv sead_clearinghouse/import/* . && \
    rm -rf sead_clearinghouse import && \
    mkdir ./input && \
    mkdir ./output && \
    pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "/import.sh" ]
CMD []

# docker run -rm -ti ch/import:latest options...
