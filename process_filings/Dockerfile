# set base image (host OS)
FROM python:3.8


# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory
COPY pyproject.toml poetry.lock /

# install dependencies
RUN pip3 install poetry
ENV PATH="${PATH}:/root/.local/bin"
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

# Set the locale
RUN apt-get update && \
    apt-get install -y locales && \
    sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales

ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# copy the content of the local src directory to the working directory
COPY src/ .

# command to run on container start
CMD [ "python", "./process_filings.py" ]