using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using QueueCommon;

namespace QueuePublish
{
    class PubDriver
    {
        static void Main(string[] args)
        {
            TestQWrapper();
            TestQModel();
            TestSeparate();
        }

        static public void TestQWrapper()
        {
#if false
            RabbitMQWrapper pubQueue = new RabbitMQWrapper("AnalysisFarm", "AnalysisRequest", "localhost");

            while (!Console.KeyAvailable)
            {
                if (pubQueue.QueueEmpty())
                    pubQueue.PostTestMessages();
            }
            pubQueue.CloseConnections();
#endif
        }
        static public void TestQModel()
        {
            QueueingModel pubQueue = new QueueingModel("AnalysisFarm", "AnalysisRequest", "localhost", "", "", "", "", 5);

            while (!Console.KeyAvailable)
            {
                if (pubQueue.QueueEmpty())
                    pubQueue.PostTestMessages();
            }
            pubQueue.CloseConnections();
        }
        static public void TestSeparate()
        {
            Exchange pubExch = new Exchange("AnalysisFarm", "localhost", "", "", "", 5);
            Queue pubQueue = new Queue( pubExch, "AnalysisRequest", "");

            while (!Console.KeyAvailable)
            {
                if (pubQueue.IsEmpty)
                    pubQueue.PostTestMessages();
            }
            pubQueue.Close();
            pubExch.Close();
        }
    }
}
