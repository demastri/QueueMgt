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
        ConnectionDetail thisConnDetail;

        Exchange thisExch = null;
        QueueDeclareOk dok = null;
        RabbitMQ.Client.Events.EventingBasicConsumer consumer = null;

        public string name { get { return thisConnDetail.queueName; } }
        int messagesSent = 0;
        ReadQueueHandler clientCallback = null;

        public delegate void ReadQueueHandler(byte[] result, string routeKey);
        public event ReadQueueHandler SubscribedMessageReceived;


        public Queue(Exchange exch, ConnectionDetail conn)
        {
            BaseInit(conn);
            thisExch = exch;    // the assumption is that this is pretty fully initialized by now...
            InitQueue();
        }
        private void BaseInit()
        {
            BaseInit( new ConnectionDetail() );
        }
        private void BaseInit(ConnectionDetail cd)
        {
            thisConnDetail = cd.Copy();
            if (thisConnDetail.queueName == "")
                thisConnDetail = thisConnDetail.UpdateQueueDetail(System.Guid.NewGuid().ToString(), null);
            
            messagesSent = 0;
            consumer = null;
            clientCallback = null;
        }

        public bool IsOpen { get { return thisExch != null && thisExch.IsOpen; } }
        public bool IsClosed { get { return !IsOpen; } }

        private void InitQueue()
        {
            dok = thisExch.channel.QueueDeclare(thisConnDetail.queueName, false, false, false, null);
            foreach (string rk in thisConnDetail.routeKeys)
                thisExch.channel.QueueBind(thisConnDetail.queueName, thisExch.name, rk, null);
            // the routing keys determine what this queue wants to LISTEN to - can be multiple!!
        }

        public void SetListenerCallback(ReadQueueHandler callback)
        {
            consumer = thisExch.CreateConsumer();
            consumer.Received += LocalCallback;
            SubscribedMessageReceived += callback;
            thisExch.channel.BasicConsume(thisConnDetail.queueName, true, consumer);
        }
        private void LocalCallback(Object o, RabbitMQ.Client.Events.BasicDeliverEventArgs e)
        {
            SubscribedMessageReceived(e.Body, e.RoutingKey);
        }

        public bool IsEmpty { get { return MessageCount() == 0; } }

        public uint MessageCount()
        {
            dok = thisExch.channel.QueueDeclarePassive(thisConnDetail.queueName);
            return dok.MessageCount;
        }
        public void PostMessage(string someMessage)
        {
            thisExch.PostMessage(someMessage, thisConnDetail.routeKeys[0]);
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
            BasicGetResult result = thisExch.channel.BasicGet(thisConnDetail.queueName, noAck);
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
            foreach (string rk in thisConnDetail.routeKeys )
                if( thisExch.IsOpen && thisExch.channel.IsOpen )
                    thisExch.channel.QueueUnbind(thisConnDetail.queueName, thisExch.name, rk, null);
        }
    }
}

