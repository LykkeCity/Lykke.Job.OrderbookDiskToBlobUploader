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
        private const int _maxBlockSize = 4 * 1024 * 1024; // 4Mb

        private readonly CloudBlobClient _blobClient;
        private readonly BlobRequestOptions _blobRequestOptions = new BlobRequestOptions
        {
            MaximumExecutionTime = TimeSpan.FromMinutes(15),
        };

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
                        if (stream.Length + block.Length * 2 >= _maxBlockSize)
                        {
                            stream.Position = 0;
                            await blob.AppendFromStreamAsync(stream, null, _blobRequestOptions, null);
                            stream.Position = 0;
                            stream.SetLength(0);
                        }

                        writer.WriteLine(block);
                        writer.Flush();
                    }

                    if (stream.Length > 0)
                    {
                        stream.Position = 0;
                        await blob.AppendFromStreamAsync(stream, null, _blobRequestOptions, null);
                    }
                }
            }
        }

        private async Task<CloudAppendBlob> InitBlobAsync(string containerName, string storagePath)
        {
            var blobContainer = _blobClient.GetContainerReference(containerName.ToLower().Replace('.', '-').Replace('_', '-'));
            if (!(await blobContainer.ExistsAsync()))
                await blobContainer.CreateAsync(BlobContainerPublicAccessType.Container, _blobRequestOptions, null);

            var blob = blobContainer.GetAppendBlobReference(storagePath);
            await blob.CreateOrReplaceAsync(null, _blobRequestOptions, null);
            blob.Properties.ContentType = "text/plain";
            blob.Properties.ContentEncoding = Encoding.UTF8.WebName;
            await blob.SetPropertiesAsync(null, _blobRequestOptions, null);

            return blob;
        }
    }
}
