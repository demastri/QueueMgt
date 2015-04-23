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
        ConnectionDetail thisConnDetail;

        ConnectionFactory thisFactory = null;
        IConnection thisConnection = null;
        IModel thisModel;

        public string name { get { return thisConnDetail.exchName; } }
        public IModel channel { get { return thisModel; } }
        public bool IsOpen { get { return thisModel != null && thisModel.IsOpen; } }
        public bool IsClosed { get { return !IsOpen; } }

        public Exchange(ConnectionDetail connDetail)
        {
            BaseInit(connDetail);
            InitExchange();
        }
        private void BaseInit()
        {
            BaseInit(new ConnectionDetail());
        }
        private void BaseInit(ConnectionDetail connDetail)
        {
            thisConnDetail = connDetail.Copy();
        }
        private void InitExchange()
        {
            thisFactory = new ConnectionFactory();
            thisFactory.Uri = thisConnDetail.Uri;
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
                channel.ExchangeDeclare(thisConnDetail.exchName, thisConnDetail.exchType);
            }
        }

        public QueueDeclareOk CreateQueue(string qName, string routeKey)
        {
            QueueDeclareOk outOk = thisModel.QueueDeclare(qName, false, false, false, null);
            thisModel.QueueBind(qName, thisConnDetail.exchName, routeKey, null);
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
