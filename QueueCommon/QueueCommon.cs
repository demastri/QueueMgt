using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace QueueCommon
{
    // deprecated class - Queueing model supports different exch types and breaks exch/queues apart
    class RabbitMQWrapper    
    {
        ConnectionFactory factory = null;
        IConnection conn = null;
        IModel channel = null;
        QueueDeclareOk dok = null;
        RabbitMQ.Client.Events.EventingBasicConsumer consumer = null;
        string hostName = "localhost";
        string uid = "guest";
        string pwd = "guest";
        int port = 5672;
        string exchangeName = "refExch";
        string queueName = "refQueue";
        string routingKey = "";
        int messagesSent = 0;
        ReadQueueHandler clientCallback = null;

        public delegate void ReadQueueHandler(byte[] result);
        public event ReadQueueHandler SubscribedMessageReceived;

        public RabbitMQWrapper(string exchName, string qName, string routeKey, string host, string user, string pass, int qPort)
        {
            BaseInit();
            exchangeName = exchName;
            queueName = qName;
            routingKey = routeKey;
            hostName = host;
            uid = user;
            pwd = pass;
            port = qPort;
            InitQueue();
        }
        public RabbitMQWrapper(string qName, string host, int qPort)
        {
            BaseInit();
            queueName = qName;
            hostName = host;
            port = qPort;
            InitQueue();
        }
        public RabbitMQWrapper(string exchName, string qName, string route, string host)
        {
            BaseInit();
            exchangeName = exchName;
            queueName = qName;
            hostName = host;
            routingKey = route;
            InitQueue();
        }
        public RabbitMQWrapper(string exchName, string qName, string host)
        {
            BaseInit();
            exchangeName = exchName;
            queueName = qName;
            hostName = host;
            InitQueue();
        }
        public RabbitMQWrapper(string qName, string host)
        {
            BaseInit();
            queueName = qName;
            hostName = host;
            InitQueue();
        }
        public RabbitMQWrapper(string qName)
        {
            BaseInit();
            queueName = qName;
            InitQueue();
        }
        public RabbitMQWrapper()
        {
            BaseInit();
            InitQueue();
        }

        private void BaseInit()
        {
            hostName = "localhost";
            uid = "guest";
            pwd = "guest";
            port = 5672;
            exchangeName = System.Guid.NewGuid().ToString();
            queueName = System.Guid.NewGuid().ToString();
            routingKey = "";
            messagesSent = 0;
            consumer = null;
            clientCallback = null;
        }

        public bool IsOpen
        {
            get
            {
                return conn != null && conn.IsOpen && channel != null && channel.IsOpen;
            }
        }

        private void InitQueue()
        {
            factory = new ConnectionFactory();
            factory.Uri = "amqp://" + uid + ":" + pwd + "@" + hostName + ":" + port.ToString();//        amqp://user:pass@hostName:port/vhost";
            try
            {
                conn = factory.CreateConnection();
            }
            catch (Exception e)
            {
                conn = null;
            }
            channel = null;
            dok = null;
            if (conn != null)
            {
                channel = conn.CreateModel();

                channel.ExchangeDeclare(exchangeName, ExchangeType.Direct);
                dok = channel.QueueDeclare(queueName, false, false, false, null);
                channel.QueueBind(queueName, exchangeName, routingKey, null);
            }
        }

        public void SetListenerCallback(ReadQueueHandler callback)
        {
            consumer = new RabbitMQ.Client.Events.EventingBasicConsumer(channel);
            consumer.Received += LocalCallback;
            SubscribedMessageReceived += callback;
            channel.BasicConsume(queueName, true, consumer);
        }
        private void LocalCallback(Object o, RabbitMQ.Client.Events.BasicDeliverEventArgs e)
        {
            SubscribedMessageReceived(e.Body);
        }

        public bool QueueEmpty()
        {
            return MessageCount() == 0;
        }
        public uint MessageCount()
        {
            dok = channel.QueueDeclarePassive(queueName);
            return dok.MessageCount;
        }
        public bool IsClosed()
        {
            return channel.IsClosed;
        }
        public void PostMessage(string someMessage)
        {
            try
            {
                byte[] messageBodyBytes = System.Text.Encoding.UTF8.GetBytes(someMessage);
                channel.BasicPublish(exchangeName, routingKey, null, messageBodyBytes);
                //Console.WriteLine("Posting message " + (++messagesSent).ToString());
            }
            catch (Exception e)
            {
            }
        }

        public void PostTestMessages()
        {
            for (int i = 0; i < 5; i++)
            {
                byte[] messageBodyBytes = System.Text.Encoding.UTF8.GetBytes("Hello, world! " + (++messagesSent).ToString());
                channel.BasicPublish(exchangeName, routingKey, null, messageBodyBytes);
                Console.WriteLine("Posting message " + messagesSent.ToString());
            }
        }
        public byte[] ReadMessage()
        {
            BasicGetResult result = channel.BasicGet(queueName, true);
            if (result != null)
                return result.Body;
            return null;
        }
        public void PullMessages()
        {
            bool noAck = false;
            BasicGetResult result = channel.BasicGet(queueName, noAck);
            if (result != null)
            {
                IBasicProperties props = result.BasicProperties;
                byte[] body = result.Body;
                string outStr = System.Text.Encoding.Default.GetString(body);

                // acknowledge receipt of the message
                channel.BasicAck(result.DeliveryTag, false);
                Console.WriteLine(outStr);
            }
        }
        public void CloseConnections()
        {
            channel.QueueUnbind(queueName, exchangeName, routingKey, null);
            channel.Close(200, "Goodbye");
            conn.Close();
        }
    }
}
