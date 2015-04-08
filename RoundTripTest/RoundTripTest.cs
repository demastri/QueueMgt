#undef UseQWrapper

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using QueueCommon;

namespace RoundTripTest
{
    class RoundTripTest
    {
        static string hostName = "localhost";
        static string uid = "guest";
        static string pwd = "guest";
        static int port = 5672;
        static string exchName = "TestRTExch";
        static string exchType = "direct";

        static List<string> WrapRequests = new List<string>();
        static List<string> ModelRequests = new List<string>();
        static List<string> SeparateRequests = new List<string>();

#if UseQWrapper
        static RabbitMQWrapper asyncWrap = null;
#endif
        static QueueingModel asyncModel = null;
        static Exchange asyncExch = null;
        static Queue asyncQueue = null;

        static void Main(string[] args)
        {
            BuildPublishers();
            BuildConsumers();
            ValidateResults();

            BuildPublishers();
            BuildAsyncConsumers();
            System.Threading.Thread.Sleep(1000);
            ValidateResults();
            CloseAsync();
        }
        static void BuildPublishers()
        {
            TestQWrapperPub();
            TestQModelPub();
            TestSeparatePub();
        }
        static void BuildConsumers()
        {
            TestQWrapperRead();
            TestQModelRead();
            TestSeparateRead();
        }
        static void BuildAsyncConsumers()
        {
            TestQWrapperReadAsync();
            TestQModelReadAsync();
            TestSeparateReadAsync();
        }
        static void ValidateResults()
        {
            if (WrapRequests.Count == 0 && ModelRequests.Count == 0 && SeparateRequests.Count == 0)
                Console.WriteLine(" Looks good...");
            else
                Console.WriteLine("Wrap had: " + WrapRequests.Count.ToString() + " Models had: " + ModelRequests.Count.ToString() + " Separates had: " + SeparateRequests.Count.ToString());
        }

        #region publishers

        static public void TestQWrapperPub()
        {
#if UseQWrapper
            RabbitMQWrapper pubQueue = new RabbitMQWrapper(
                exchName, "QWrap", "QWrap", hostName, uid, pwd, port);
            for (int i=0; i < 5; i++)
            {
                string outMsg = "Hello World " + i.ToString();
                WrapRequests.Add(outMsg);
                pubQueue.PostMessage(outMsg);
            }
            pubQueue.CloseConnections();
#endif
        }
        static public void TestQModelPub()
        {
            QueueingModel pubQueue = new QueueingModel(
                exchName, exchType, "QModel", "QModel", hostName, uid, pwd, port);
            for (int i = 0; i < 5; i++)
            {
                string outMsg = "Hello World " + i.ToString();
                ModelRequests.Add(outMsg);
                pubQueue.PostMessage(outMsg);
            }
            pubQueue.CloseConnections();
        }
        static public void TestSeparatePub()
        {
            Exchange pubExch = new Exchange(exchName, exchType, hostName, uid, pwd, port);
            Queue pubQueue = new Queue(pubExch, "Separate", "Separate");

            for (int i = 0; i < 5; i++)
            {
                string outMsg = "Hello World " + i.ToString();
                SeparateRequests.Add(outMsg);
                pubQueue.PostMessage(outMsg);
            }

            pubQueue.Close();
            pubExch.Close();
        }
        #endregion

        #region readers

        static void TestQWrapperRead()
        {
#if UseQWrapper
            RabbitMQWrapper subQueue = new RabbitMQWrapper(
                exchName, "QWrap", "QWrap", hostName, uid, pwd, port);

            while (!subQueue.QueueEmpty())
            {
                string gotOne = subQueue.ReadMessageAsString();
                if (WrapRequests.Contains(gotOne))
                    WrapRequests.Remove(gotOne);
            }
            subQueue.CloseConnections();
#endif
        }
        static void TestQModelRead()
        {
            QueueingModel subQueue = new QueueingModel(
                exchName, exchType, "QModel", "QModel", hostName, uid, pwd, port);

            while (!subQueue.QueueEmpty())
            {
                string gotOne = subQueue.ReadMessageAsString();
                if (ModelRequests.Contains(gotOne))
                    ModelRequests.Remove(gotOne);
            }
            subQueue.CloseConnections();
        }
        static void TestSeparateRead()
        {
            Exchange subExch = new Exchange(exchName, exchType, hostName, uid, pwd, port);
            Queue subQueue = new Queue(subExch, "Separate", "Separate");

            while (!subQueue.IsEmpty)
            {
                string gotOne = subQueue.ReadMessageAsString();
                if (SeparateRequests.Contains(gotOne))
                    SeparateRequests.Remove(gotOne);
            }
            subQueue.Close();
            subExch.Close();
        }
        #endregion

        #region async readers

        static void TestQWrapperReadAsync()
        {
#if UseQWrapper
            asyncWrap = new RabbitMQWrapper(
                exchName, "QWrap", "QWrap", hostName, uid, pwd, port);

            asyncWrap.SetListenerCallback(PullWrapper);
#endif
        }
        static void PullWrapper(byte[] msg)
        {
            string gotOne = System.Text.Encoding.Default.GetString(msg);
            if (WrapRequests.Contains(gotOne))
                WrapRequests.Remove(gotOne);
        }

        static void TestQModelReadAsync()
        {
            asyncModel = new QueueingModel(
                exchName, exchType, "QModel", "QModel", hostName, uid, pwd, port);
            asyncModel.SetListenerCallback(PullModel);
        }
        static void PullModel(byte[] msg, string routeKey)
        {
            string gotOne = System.Text.Encoding.Default.GetString(msg);
            if (ModelRequests.Contains(gotOne))
                ModelRequests.Remove(gotOne);
        }
        static void TestSeparateReadAsync()
        {
            asyncExch = new Exchange(exchName, exchType, hostName, uid, pwd, port);
            asyncQueue = new Queue(asyncExch, "Separate", "Separate");
            asyncQueue.SetListenerCallback(PullSeparate);
        }
        static void PullSeparate(byte[] msg, string routeKey)
        {
            string gotOne = System.Text.Encoding.Default.GetString(msg);
            if (SeparateRequests.Contains(gotOne))
                SeparateRequests.Remove(gotOne);
        }

        static void CloseAsync()
        {
#if UseQWrapper
            asyncWrap.CloseConnections();
#endif
            asyncModel.CloseConnections();
            asyncQueue.Close();
            asyncExch.Close();
        }

        #endregion

    }
}
