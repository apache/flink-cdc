FROM ruby:3.3-slim

WORKDIR /src
RUN apt-get update && apt-get install -y wget
RUN gem install tty-prompt
COPY src /src
RUN chmod +x /src/app.rb
ENTRYPOINT ["/src/app.rb"]
