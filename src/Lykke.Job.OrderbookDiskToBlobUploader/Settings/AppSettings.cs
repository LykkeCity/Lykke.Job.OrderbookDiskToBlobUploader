using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Settings
{
    public class AppSettings
    {
        public OrderbookDiskToBlobUploaderSettings OrderbookDiskToBlobUploaderJob { get; set; }
        public SlackNotificationsSettings SlackNotifications { get; set; }
    }

    public class SlackNotificationsSettings
    {
        public AzureQueuePublicationSettings AzureQueue { get; set; }
    }

    public class AzureQueuePublicationSettings
    {
        public string ConnectionString { get; set; }

        public string QueueName { get; set; }
    }

    public class OrderbookDiskToBlobUploaderSettings
    {
        [AzureTableCheck]
        public string LogsConnString { get; set; }

        [AzureBlobCheck]
        public string BlobConnectionString { get; set; }

        public string DiskPath { get; set; }

        public int MaxFilesInBatch { get; set; }
    }
}
