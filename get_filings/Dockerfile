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

# copy the content of the local src directory to the working directory
COPY src/ .

# command to run on container start
CMD [ "python", "./get_filings.py" ]