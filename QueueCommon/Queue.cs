using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace QueueCommon
{
    public class Queue
    {
        Exchange thisExch = null;
        QueueDeclareOk dok = null;
        RabbitMQ.Client.Events.EventingBasicConsumer consumer = null;

        string queueName = "";
        public string name { get { return queueName; } }
        List<string> routingKeys;
        int messagesSent = 0;
        ReadQueueHandler clientCallback = null;

        public delegate void ReadQueueHandler(byte[] result, string routeKey);
        public event ReadQueueHandler SubscribedMessageReceived;

        public Queue(Exchange exch, string qName, string route)
        {
            BaseInit();
            queueName = qName;
            routingKeys.Add(route);
            thisExch = exch;    // the assumption is that this is pretty fully initialized by now...
            InitQueue();
        }
        public Queue(Exchange exch, string qName, List<string> routes)
        {
            BaseInit();
            queueName = qName;
            foreach (string s in routes)
                routingKeys.Add(s);
            thisExch = exch;    // the assumption is that this is pretty fully initialized by now...
            InitQueue();
        }

        private void BaseInit()
        {
            queueName = System.Guid.NewGuid().ToString();
            routingKeys = new List<string>();
            messagesSent = 0;
            consumer = null;
            clientCallback = null;
        }

        public bool IsOpen { get { return thisExch != null && thisExch.IsOpen; } }
        public bool IsClosed { get { return !IsOpen; } }

        private void InitQueue()
        {
            dok = thisExch.channel.QueueDeclare(queueName, false, false, false, null);
            foreach (string rk in routingKeys)
                thisExch.channel.QueueBind(queueName, thisExch.name, rk, null);
            // the routing keys determine what this queue wants to LISTEN to - can be multiple!!
        }

        public void SetListenerCallback(ReadQueueHandler callback)
        {
            consumer = thisExch.CreateConsumer();
            consumer.Received += LocalCallback;
            SubscribedMessageReceived += callback;
            thisExch.channel.BasicConsume(queueName, true, consumer);
        }
        private void LocalCallback(Object o, RabbitMQ.Client.Events.BasicDeliverEventArgs e)
        {
            SubscribedMessageReceived(e.Body, e.RoutingKey);
        }

        public bool IsEmpty { get { return MessageCount() == 0; } }

        public uint MessageCount()
        {
            dok = thisExch.channel.QueueDeclarePassive(queueName);
            return dok.MessageCount;
        }
        public void PostMessage(string someMessage)
        {
            thisExch.PostMessage(someMessage, routingKeys[0]);
        }
        public void PostMessage(string someMessage, string thisRK)
        {
            thisExch.PostMessage(someMessage, thisRK);
        }

        public void PostTestMessages()
        {
            for (int i = 0; i < 5; i++)
            {
                string outStr = "Hello, world! " + (++messagesSent).ToString();
                PostMessage(outStr);
                Console.WriteLine(outStr);
            }
        }
        public byte[] ReadMessage(bool noAck)
        {
            byte[] body = null;
            BasicGetResult result = thisExch.channel.BasicGet(queueName, noAck);
            if (result != null)
            {
                IBasicProperties props = result.BasicProperties;
                body = result.Body;
                string outStr = System.Text.Encoding.Default.GetString(body);

                if (!noAck)
                {
                    // acknowledge receipt of the message
                    thisExch.channel.BasicAck(result.DeliveryTag, false);
                }
            }
            return body;
        }
        public string ReadMessageAsString()
        {
            return System.Text.Encoding.Default.GetString(ReadMessage(true));
        }
        public byte[] ReadMessage()
        {
            return ReadMessage(true);
        }
        public byte[] ReadMessageAndAck()
        {
            return ReadMessage(false);
        }
        public void Close()
        {
            foreach (string rk in routingKeys )
                if( thisExch.IsOpen && thisExch.channel.IsOpen )
                    thisExch.channel.QueueUnbind(queueName, thisExch.name, rk, null);
        }
    }
}

