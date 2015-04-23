using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueueCommon
{
    public class QueueingModel
    {
        ConnectionDetail myConn;
        Exchange myExchange;
        Queue myQueue;

        public QueueingModel(ConnectionDetail conn)
        {
            myConn = conn;
            myExchange = new Exchange(conn);

            myQueue = new Queue(myExchange, conn);
        }
        public QueueingModel(string exchName, string exchType, string qName, List<string> routeKeys, string host, string user, string pass, int qPort)
        {
            myConn = new ConnectionDetail(host, qPort, exchName, exchType, qName, routeKeys, user, pass);
            myExchange = new Exchange(myConn);

            myQueue = new Queue(myExchange, myConn);
        }
        public QueueingModel(string exchName, string exchType, string qName, string routeKey, string host, string user, string pass, int qPort)
        {
            myConn = new ConnectionDetail(host, qPort, exchName, exchType, qName, routeKey, user, pass);
            myExchange = new Exchange(myConn);

            myQueue = new Queue(myExchange, myConn);
        }
        public bool IsOpen { get { return myExchange.IsOpen && myQueue.IsOpen; } }
        public bool IsClosed { get { return myExchange.IsClosed || myQueue.IsClosed; } }

        public void SetListenerCallback(Queue.ReadQueueHandler callback)
        {
            myQueue.SetListenerCallback(callback);
        }

        public bool QueueEmpty()
        {
            return myQueue.MessageCount() == 0;
        }
        public uint MessageCount()
        {
            return myQueue.MessageCount();
        }
        public void PostMessage(string someMessage)
        {
            myQueue.PostMessage(someMessage);
        }
        public void PostMessage(string someMessage, string thisRK)
        {
            myQueue.PostMessage(someMessage, thisRK);
        }

        public void PostTestMessages()
        {
            for (int i = 0; i < 5; i++)
            {
                string outStr = "Hello, world! " + (++i).ToString();
                PostMessage(outStr);
                Console.WriteLine(outStr);
            }
        }
        public string ReadMessageAsString()
        {
            return System.Text.Encoding.Default.GetString(ReadMessage());
        }
        public byte[] ReadMessage()
        {
            return myQueue.ReadMessage();
        }
        public void CloseConnections()
        {
            myQueue.Close();
            myExchange.Close();
        }

    }
}
