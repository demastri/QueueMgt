using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace QueueCommon
{
    public class Exchange
    {
        ConnectionFactory thisFactory = null;
        IConnection thisConnection = null;
        IModel thisModel;

        string hostName;
        string uid;
        string pwd;
        int port;
        string exchName;
        string exchType;

        public string name { get { return exchName; } }
        public IModel channel { get { return thisModel; } }
        public bool IsOpen { get { return thisModel != null && thisModel.IsOpen; } }
        public bool IsClosed { get { return !IsOpen; } }

        public Exchange(string exchangeName, string exchangeType, string host, string user, string pass, int qPort)
        {
            BaseInit();
            exchName = exchangeName;
            hostName = host;
            uid = user;
            pwd = pass;
            port = qPort;
            exchType = exchangeType;
            InitExchange();
        }
        public Exchange()
        {
            BaseInit();
            InitExchange();
        }
        private void BaseInit()
        {
            hostName = "localhost";
            uid = "guest";
            pwd = "guest";
            port = 5672;
            exchName = System.Guid.NewGuid().ToString();
            exchType = ExchangeType.Direct;
        }
        private void InitExchange()
        {
            thisFactory = new ConnectionFactory();
            thisFactory.Uri = "amqp://" + uid + ":" + pwd + "@" + hostName + ":" + port.ToString();//        amqp://user:pass@hostName:port/vhost";
            try
            {
                thisConnection = thisFactory.CreateConnection();
            }
            catch (Exception e)
            {
                thisConnection = null;
            }
            thisModel = null;
            if (thisConnection != null)
            {
                thisModel = thisConnection.CreateModel();
                channel.ExchangeDeclare(exchName, exchType);
            }
        }

        public QueueDeclareOk CreateQueue(string qName, string routeKey)
        {
            QueueDeclareOk outOk = thisModel.QueueDeclare(qName, false, false, false, null);
            thisModel.QueueBind(qName, exchName, routeKey, null);
            return outOk;
        }

        public RabbitMQ.Client.Events.EventingBasicConsumer CreateConsumer()
        {
            return new RabbitMQ.Client.Events.EventingBasicConsumer(thisModel);
        }
        public void PostMessage(string someMessage, string routeKey)
        {
            try
            {
                channel.BasicPublish(name, routeKey, null, System.Text.Encoding.UTF8.GetBytes(someMessage));
            }
            catch (Exception e)
            {
            }

        }
        public void Close()
        {
            channel.Close(200, "Goodbye");
            if( thisConnection.IsOpen )
                thisConnection.Close();
        }

    }
}
