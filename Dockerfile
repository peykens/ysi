FROM node:10.9

# Create app directory
#RUN mkdir /app
WORKDIR /
RUN apt-get update && apt-get install dnsutils -y && apt-get install iputils-ping -y

COPY intake-service/ /

RUN npm install

CMD ["node","index.js"]
