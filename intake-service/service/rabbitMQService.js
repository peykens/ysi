"use strict";
//v1.0: Exchange supported
const debug = require('debug')('rabbitMQService');
const amqlib = require('amqplib');

const EXCHANGE_TYPE_TOPIC = 'topic';

class rabbitMQService{
    constructor(host){
        debug('host',host);
        this._rabbitMQHost = host;
        this._rabbitMQConnected = false;
    }

    async open(){
        debug('open','create rabbitmq connection');
        try {
            this._rabbitMQConnection = await amqlib.connect(`amqp://${this._rabbitMQHost}/`);
            debug('open','rabbitmq connected');
            this._rabbitMQConnected = true;
            this._registerListenerOnExit();
            return true;
        } catch (error) {
            debug('open',`error connecting to amqp://${this._rabbitMQHost}/`, error);
            throw error;
        }
    }

    _registerListenerOnExit(){
        process.on('SIGTERM', () => {
            debug('SIGTERM signal received', 'close connection to broker');
            this.close();
        });
    }

    async close(){
        if (!this._rabbitMQConnected) {return;}
        debug('close','close rabbitmq connection');
        return this._rabbitMQConnection.close();
    }

    async listenOnQueue(queue,callback){
        if (!this._rabbitMQConnected){ await this.open();}
        debug('listenOnQueue',queue,'creating channel');
        const channel = await this._rabbitMQConnection.createChannel();
        debug('listenOnQueue',queue,'asserting to queue');
        await channel.assertQueue(queue);
        debug('listenOnQueue',queue,'start listening');
        channel.consume(queue,(msg)=> {
            if (msg !== null){
                debug('listenOnQueue',queue,'handle message','invoking callback');
                callback(JSON.parse(msg.content.toString())).then((rsp) => {
                    debug('listenOnQueue',queue,'handle message','processed succesfully, acknowledging message on broker');
                    channel.ack(msg);
                }).catch((ex)=>{
                    debug('listenOnQueue',queue,'handle message','error returned from callback, acknowledging message on broker',ex);
                    //channel.nack(msg); 
                    channel.ack(msg);
                });
                
            }
        });
        return;
    }

    async listenOnExchange(exchange,topics,callback){
        if (!Array.isArray(topics)) { topics = [topics]; }
        if (!this._rabbitMQConnected){ await this.open();}
        debug('listenOnExchange',exchange,'creating channel');
        const channel = await this._rabbitMQConnection.createChannel();
        debug('listenOnExchange',exchange,'asserting to exchange');
        await channel.assertExchange(exchange,EXCHANGE_TYPE_TOPIC, {durable: false});
        debug('listenOnExchange','create queue');
        const queue = await channel.assertQueue('',{exclusive: true});
        debug('listenOnExchange','Queue created', queue.queue);
        await topics.forEach((t) => {
            debug('listenOnExchange',`Bind queue ${queue} to exchange ${exchange} and register topic ${t}`);
            channel.bindQueue(queue.queue,exchange,t);
        });
        debug('listenOnExchange',queue.queue,'start listening on queue');
        channel.consume(queue.queue,(msg)=> {
            if (msg !== null){
                debug('listenOnExchange',queue.queue,'handle message',msg.fields.exchange ,msg.fields.routingKey,'invoking callback');
                callback(JSON.parse(msg.content.toString()),msg.fields.exchange ,msg.fields.routingKey).then((rsp) => {
                    debug('listenOnExchange',queue,'handle message','processed succesfully');
                }).catch((ex)=>{
                    debug('listenOnExchange',queue,'handle message','error returned from callback',ex);
                });
            }
        }, {noAck: true});
        return;
    }

    async sendToQueue(queue,obj){
        if (!this._rabbitMQConnected){ await this.open();}
        debug('sendToQueue','creating channel');
        const channel = await this._rabbitMQConnection.createChannel();
        debug('sendToQueue',queue,'asserting to queue');
        await channel.assertQueue(queue);
        debug('sendToQueue',queue,'sending message', obj);
        await channel.sendToQueue(queue,Buffer.from(JSON.stringify(obj)));
        return;
    }


    async sentToTopicExchange(exchange,topic,obj){
        if (!this._rabbitMQConnected){ await this.open();}
        debug('sentToTopicExchange','creating channel');
        const channel = await this._rabbitMQConnection.createChannel();
        debug('sentToTopicExchange',exchange,'asserting to exchange');
        await channel.assertExchange(exchange,EXCHANGE_TYPE_TOPIC, {durable: false});
        debug('sentToTopicExchange',exchange,'publish message on topic', topic, obj);
        await channel.publish(exchange,topic,Buffer.from(JSON.stringify(obj)));
        return;
    }

}

module.exports = rabbitMQService;