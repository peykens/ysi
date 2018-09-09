FROM node:10.9

# Create app directory
#RUN mkdir /app
WORKDIR /

COPY intake-service/ /

RUN npm install

CMD ["node","index.js"]