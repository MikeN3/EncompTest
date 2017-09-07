#r "Microsoft.ServiceBus"
#r "Newtonsoft.Json"
#r "System.Configuration"
#r "System.Data"
using System.Net;
using System.Text;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using RestSharp;

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    var data = await req.Content.ReadAsByteArrayAsync();
    var payload = System.Text.Encoding.Default.GetString(data);

    log.Info($"{payload}");

    JToken json = JObject.Parse(payload);

    string valueTime = (string)json.SelectToken("eventTime");
    string valueType = (string)json.SelectToken("eventType");
    string valueLoan = (string)json.SelectToken("meta.resourceId");
    string valueUserId = (string)json.SelectToken("meta.userId");
    string valueResourceType = (string)json.SelectToken("meta.resourceType");

    // Push the message to the 10 sec queue hold
    string connectionString = "Endpoint=sb://queuetestbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Y5C0bKQtDJ9XAcKk7v6X/RdEnUO7BUkZSRxIWd29ZBk=";
    string queueName = "encompq";

    var queueClient = QueueClient.CreateFromConnectionString(connectionString, queueName);
    var message = new BrokeredMessage(valueLoan);
    queueClient.Send(message);
    queueClient.Close();

    return req.CreateResponse(HttpStatusCode.OK);
}