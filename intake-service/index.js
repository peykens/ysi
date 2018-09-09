//Load .env
require('dotenv').config();

const debug = require('debug')('intake-service');
const package = require('./package.json');
const rabbitBuilder = require('./service/rabbitMQService');
const uuidv4 = require('uuid/v4');

const envalid = require('envalid')
const { str } = envalid

const env = envalid.cleanEnv(process.env, {
    BROKER_HOST:            str(),
    BROKER_EXCHANGE:     str(),
    BROKER_TOPIC_INPUT:  str(),
    BROKER_TOPIC_MANAGER: str()
});

const workflowConfig = require('./config/workflow');

debug(`Application: ${package.name}`,`Version: ${package.version}`);
debug(`BROKER_HOST: ${env.BROKER_HOST}`);
debug(`BROKER_EXCHANGE: ${env.BROKER_EXCHANGE}`);
debug(`BROKER_TOPIC_INPUT: ${env.BROKER_TOPIC_INPUT}`);
debug(`BROKER_TOPIC_MANAGER: ${env.BROKER_TOPIC_MANAGER}`);

debug(`Initialize rabbit connection`);
const broker = new rabbitBuilder(env.BROKER_HOST);
debug(`Start listening on exchange ${env.BROKER_EXCHANGE} topic ${env.BROKER_TOPIC_INPUT}`);

broker.listenOnExchange(env.BROKER_EXCHANGE,env.BROKER_TOPIC_INPUT,handleIncomingMessage);

function handleIncomingMessage(obj,exchange,topic){
    return new Promise((resolve,reject)=>{
        //Handle incoming message
        debug(`Handle incoming message on exchange ${exchange} and topic ${topic}`);
        //  Is message valid
        if (!isMessageValid(obj)){ reject('Invallid message structure'); }
        if (!obj.id){ obj.id = uuidv4(); }
        debug(`Message ID ${obj.id}`,obj.id);
        //  Is the workflow known?
        if (!workflowConfig[obj.workflow.name]) {
            debug(`Workflow [${obj.workflow.name}] is not known in config`);
            reject('Workflow is not known');
        }
        //  Sanitize the data
        obj.workflow.execution = obj.workflow.name;
        obj.data = {}
        //  Publish on exchange queue
        debug(`Publish for execution to exchange ${env.BROKER_EXCHANGE} on topic ${env.BROKER_TOPIC_MANAGER}`,obj.id);
        broker.sentToTopicExchange(env.BROKER_EXCHANGE,env.BROKER_TOPIC_MANAGER,obj);
    });
}

function isMessageValid(obj){
    debug(`Validate incoming message`);
    if (!obj.workflow) { 
        debug(`Workflow data missing in request`);
        return false; 
    }
    if (!obj.workflow.name) { 
        debug(`Workflow name missing in request`);
        return false; 
    }
    return true;
}