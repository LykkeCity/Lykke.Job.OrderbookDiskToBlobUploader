using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Services
{
    public class BlobSaver : IBlobSaver
    {
        private const string _blobDateFormat = "yyyy-MM-dd-HH";

        private readonly CloudBlobClient _blobClient;
        private readonly BlobRequestOptions _blobRequestOptions = new BlobRequestOptions
        {
            MaximumExecutionTime = TimeSpan.FromMinutes(15),
        };

        private DateTime _lastBatchSave = DateTime.MinValue;

        public BlobSaver(string blobConnectionString)
        {
            _blobClient = CloudStorageAccount.Parse(blobConnectionString).CreateCloudBlobClient();
        }

        public async Task SaveToBlobAsync(
            IEnumerable<string> blocks,
            string containerName,
            string storagePath)
        {
            var blob = await InitBlobAsync(containerName, storagePath);

            using (var stream = new MemoryStream())
            {
                using (var writer = new StreamWriter(stream))
                {
                    foreach (var block in blocks)
                    {
                        writer.WriteLine(block);
                    }
                    writer.Flush();
                    stream.Position = 0;
                    await blob.AppendFromStreamAsync(stream, null, _blobRequestOptions, null);
                }
            }
        }

        private async Task<CloudAppendBlob> InitBlobAsync(string containerName, string storagePath)
        {
            var blobContainer = _blobClient.GetContainerReference(containerName.ToLower());
            var blob = blobContainer.GetAppendBlobReference(storagePath);
            if (await blob.ExistsAsync())
                return blob;

            try
            {
                await blob.CreateOrReplaceAsync(AccessCondition.GenerateIfNotExistsCondition(), null, null);
                blob.Properties.ContentType = "text/plain";
                blob.Properties.ContentEncoding = Encoding.UTF8.WebName;
                await blob.SetPropertiesAsync(null, _blobRequestOptions, null);
            }
            catch (StorageException)
            {
            }

            return blob;
        }
    }
}
