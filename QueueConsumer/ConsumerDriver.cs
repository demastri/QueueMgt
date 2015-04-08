using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using QueueCommon;

namespace QueuePublish
{
    class ConsumerDriver
    {
        static void Main(string[] args)
        {
            string hostName = "localhost";
            string uid = "guest";
            string pwd = "guest";
            int port = 5672;
            string exchangeName = "refExch";
            int messagesSent = 0;

            QueueingModel subQueue = new QueueingModel(exchangeName, "direct", "AnalysisFarm", "AnalysisRequest", "localhost", uid, pwd, port);

            while (!Console.KeyAvailable)
            {
                if (!subQueue.QueueEmpty())
                {
                    string gotOne = subQueue.ReadMessageAsString();
                    Console.WriteLine(gotOne);
                }
            }
            subQueue.CloseConnections();
        }
    }
}
