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

        static QueueingModel asyncModel = null;
        static Exchange asyncExch = null;
        static Queue asyncQueue = null;

        static void Main(string[] args)
        {
            BuildPublishers();
            Console.WriteLine("Publishers live, press Enter to consume");
            Console.ReadLine();

            BuildConsumers();
            ValidateResults();

            BuildPublishers();
            Console.WriteLine("Async Publishers live, press Enter to consume");
            Console.ReadLine();
            BuildAsyncConsumers();
            System.Threading.Thread.Sleep(1000);
            ValidateResults();
            CloseAsync();
        }
        static void BuildPublishers()
        {
            TestQModelPub();
            TestSeparatePub();
        }
        static void BuildConsumers()
        {
            TestQModelRead();
            TestSeparateRead();
        }
        static void BuildAsyncConsumers()
        {
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
            ConnectionDetail conn = new ConnectionDetail(hostName, port,exchName, exchType, "Separate", "Separate", uid, pwd);
            Exchange pubExch = new Exchange(conn);
            Queue pubQueue = new Queue(pubExch, conn);

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
            ConnectionDetail conn = new ConnectionDetail(hostName, port, exchName, exchType, "Separate", "Separate", uid, pwd);
            Exchange subExch = new Exchange(conn);
            Queue subQueue = new Queue(subExch, conn);

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
            ConnectionDetail conn = new ConnectionDetail(hostName, port, exchName, exchType, "Separate", "Separate", uid, pwd);
            asyncExch = new Exchange(conn);

            asyncQueue = new Queue(asyncExch, conn);
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
            asyncModel.CloseConnections();
            asyncQueue.Close();
            asyncExch.Close();
        }

        #endregion

    }
}
