namespace Rebalanser.RabbitMQTools
{
    public class QueueInventory
    {
        public string ConsumerGroup { get; set; }
        public string ExchangeName { get; set; }
        public string QueuePrefix { get; set; }
        public int QueueCount { get; set; }
        public int LeaseExpirySeconds { get; set; }
    }
}
