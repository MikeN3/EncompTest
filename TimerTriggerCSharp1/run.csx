using System;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Threading;
using System.IO;
using System.Collections;
using System.Collections.Generic;

public static void Run(TimerInfo myTimer, TraceWriter log, ICollector<string> outputSbMsg)
{
    log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

    // Queue connection string | Change to the whatever Queue you want to use |
    string connectionString = "Endpoint=sb://queuetestbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Y5C0bKQtDJ9XAcKk7v6X/RdEnUO7BUkZSRxIWd29ZBk=";

    // Queue name | Change to whatever name the Queue is |
    string queueName = "encompq";

    // This connects the namespace manager to the connection string. Its purpose is to get a count of the number of messages within the Queue
    var nsmgr = NamespaceManager.CreateFromConnectionString(connectionString);

    // Using the namespace manager connection with Q allows us to pull the total # of active messages on the queue
    long count = nsmgr.GetQueue(queueName).MessageCountDetails.ActiveMessageCount;

    // Collecting messages that were held in Queue
    HashSet<string> accrueMessages = new HashSet<string>();

    // Put in place to bypass a compilation error
    outputSbMsg = null;

    // If statment to see if there are messages in the Queue | For 2am-4am when no activity is happening
    if (count != 0)
    {
        // For loop that should pulled messages from the Queue
        for (int i = 1; i <= count; i++)
        {
            // Creates a client (factory) to connect with the Queue
            MessagingFactory factory = MessagingFactory.CreateFromConnectionString(connectionString);

            // Create a receiver connected with the Queue
            MessageReceiver messageReceiver = factory.CreateMessageReceiver(queueName);

            // Turns the returned message into a Brokered Message
            BrokeredMessage retrievedMessage = messageReceiver.Receive();

            // Puts the body of the brokered message into a stream
            Stream stream = retrievedMessage.GetBody<Stream>();
            StreamReader reader = new StreamReader(stream);
            string recievedMessage = reader.ReadToEnd();

            accrueMessages.Add(recievedMessage);

            // This is a request sent back to the Queue stating that the message was received and should be removed from the Queue
            retrievedMessage.Complete();

            // Closes the Messaging Factory client
            factory.Close();
        }

        // Loop to send messages from the HashSet to the DeDupedQ
        foreach (string i in accrueMessages)
        {
            var client = QueueClient.CreateFromConnectionString(connectionString, "encompdedupedq");
            var message = new BrokeredMessage(i);
            client.Send(message);
            client.Close();
            log.Info("Done.");
        }
    }

    if (1 == 1 )
    {
        HashSet<string> accrueMessages2 = new HashSet<string>();

        accrueMessages2.Add("C025C5CD-E8EF-4868-8F11-EA48DF63C330");
        accrueMessages2.Add("6658E6A5-3686-4F03-BD22-DAE01C3B0FB3");
        accrueMessages2.Add("FAEE4439-0CA4-48BA-B998-DE75921D8080");
        accrueMessages2.Add("B895E87F-6C87-4145-AE55-E55FCC63F433");
        accrueMessages2.Add("75AA32F4-480A-4265-B44E-DB8911E2BF51");
        accrueMessages2.Add("87A3AFCE-E833-41D8-8B57-ABD40441C084");
        accrueMessages2.Add("7E4D470F-B110-4851-871D-A4732BEAC7FF");
        accrueMessages2.Add("748999F3-00FE-4719-8ABD-783F78ADBB3A");
        accrueMessages2.Add("E4C432BF-1010-49C1-8C02-698168642769");
        accrueMessages2.Add("06C0583A-6119-40C1-B30D-BCBA7633D4F5");

        accrueMessages2.Add("D010BF68-6E16-4CBC-817A-4AFF3622689F");
        accrueMessages2.Add("B81AF406-2E52-4020-B5D3-55274357AB1B");
        accrueMessages2.Add("E22E2BBE-2EA2-471B-BA66-4DEBEF200BAD");
        accrueMessages2.Add("2A88F898-BCCE-4B6F-82CD-78B0E09C9A3E");
        accrueMessages2.Add("784F7EB3-4AC3-44BC-AF76-3772CFD382B5");
        accrueMessages2.Add("A5BE4002-8641-4A4D-8762-95032ADD0808");
        accrueMessages2.Add("9BC474AA-D64F-4304-8735-1F5789047CAC");
        accrueMessages2.Add("91AEDF3A-81CD-491B-A114-3A8CB4E19AF3");
        accrueMessages2.Add("1CB20381-E71D-40DF-A88F-EB33A7841386");
        accrueMessages2.Add("87490C6C-FE1B-42D3-AC15-2C28700D5921"); 

        accrueMessages2.Add("C5AF1FFF-E931-4794-AF11-F90F05D0602E"); 
        accrueMessages2.Add("BCC9E550-CD8C-4151-9235-C27461A453E3"); 
        accrueMessages2.Add("5C14DE44-7807-4E5D-8CAA-7AA0EEB1B93F"); 
        accrueMessages2.Add("F948AB8A-6067-4AEC-8375-71EC9063F95B"); 
        accrueMessages2.Add("0BD386CA-393B-49E1-A0FB-1ACB2842ABED"); 
        accrueMessages2.Add("5CF4AD5D-113D-4B40-AE05-4E92D7F7EFE7"); 
        accrueMessages2.Add("37B21E05-EEF0-43DD-BCE6-E32399EAC149"); 
        accrueMessages2.Add("91D910A4-5648-4F40-BAEF-FEFD5B7452B9"); 
        accrueMessages2.Add("6CE59BB1-CBAA-41EA-A37E-1A1F6926A79A"); 
        accrueMessages2.Add("13C86ED0-3B06-4971-B608-AF8525881AE3");        

        var client2 = QueueClient.CreateFromConnectionString(connectionString, "encompdedupedq2");
        
        foreach(string loanGuid in accrueMessages2)
        {
            var message2 = new BrokeredMessage(loanGuid);

            client2.Send(message2); 
        }
        client2.Close();
        log.Info("Done.");

        long count2 = nsmgr.GetQueue("encompdedupedq2").MessageCountDetails.ActiveMessageCount;

        log.Info(count2.ToString());
    }
    
}
