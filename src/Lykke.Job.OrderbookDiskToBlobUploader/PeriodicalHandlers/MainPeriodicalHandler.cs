using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using Common;
using Common.Log;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Domain.Models;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;
using Lykke.Job.OrderbookDiskToBlobUploader.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.PeriodicalHandlers
{
    public class MainPeriodicalHandler : TimerPeriod
    {
        private readonly ILog _log;
        private readonly IBlobSaver _blobSaver;
        private readonly string _diskPath;
        private readonly int _maxFilesInBatch;

        public MainPeriodicalHandler(
            ILog log,
            IBlobSaver blobSaver,
            string diskPath,
            int maxFilesInBatch) :
            base(nameof(MainPeriodicalHandler), (int)TimeSpan.FromSeconds(10).TotalMilliseconds, log)
        {
            _log = log;
            _blobSaver = blobSaver;
            _diskPath = diskPath;
            _maxFilesInBatch = maxFilesInBatch <= 0 ? 1000 : maxFilesInBatch;
            Directory.SetCurrentDirectory(_diskPath);
        }

        public override async Task Execute()
        {
            var orderbookDataDict = await ReadFilesAsync();

            await UploadDataAsync(orderbookDataDict);
        }

        private async Task<Dictionary<string, List<(Orderbook, string)>>> ReadFilesAsync()
        {
            var files = new DirectoryInfo(_diskPath).EnumerateFiles();
            var orderbookDict = new Dictionary<string, List<(Orderbook, string)>>();
            int filesCount = 0;
            var now = DateTime.UtcNow;
            foreach (var file in files)
            {
                try
                {
                    string fileData = File.ReadAllText(file.Name);
                    if (fileData.Length == 0)
                    {
                        if (file.LastWriteTimeUtc.Subtract(now) >= TimeSpan.FromMinutes(1))
                            File.Delete(file.Name);
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(fileData))
                        continue;

                    var orderbook = OrderbookConverter.Deserialize(fileData);
                    if (orderbook == null)
                    {
                        await _log.WriteWarningAsync(nameof(MainPeriodicalHandler), nameof(ReadFilesAsync), $"Couldn't deserialize {fileData}");
                        continue;
                    }

                    string key = GetKey(orderbook, file.Name);
                    if (orderbookDict.ContainsKey(key))
                        orderbookDict[key].Add((orderbook, file.Name));
                    else
                        orderbookDict.Add(key, new List<(Orderbook, string)> { (orderbook, file.Name) });

                    ++filesCount;
                    if (filesCount >= _maxFilesInBatch)
                        break;
                }
                catch (Exception ex)
                {
                    await _log.WriteErrorAsync(nameof(MainPeriodicalHandler), nameof(ReadFilesAsync), ex);
                }
            }

            return orderbookDict;
        }

        private async Task UploadDataAsync(Dictionary<string, List<(Orderbook, string)>> orderbookDict)
        {
            foreach (var key in orderbookDict.Keys)
            {
                try
                {
                    var items = orderbookDict[key].OrderBy(i => i.Item1.Timestamp);
                    var messages = items.Select(i => OrderbookConverter.FormatMessage(i.Item1));
                    var first = items.First().Item1;
                    await _blobSaver.SaveToBlobAsync(messages, GetContainerName(first), first.Timestamp);

                    int filesCount = 0;
                    foreach (var item in items)
                    {
                        File.Delete(item.Item2);
                        ++filesCount;
                    }

                    await _log.WriteInfoAsync(
                        nameof(MainPeriodicalHandler),
                        nameof(UploadDataAsync),
                        $"Uploaded and deleted {filesCount} files for {key}");
                }
                catch (Exception ex)
                {
                    await _log.WriteErrorAsync(nameof(MainPeriodicalHandler), nameof(UploadDataAsync), ex);
                }
            }
        }

        private string GetContainerName(Orderbook item)
        {
            return item.AssetPair.ToLower() + (item.IsBuy ? "-buy" : "-sell");
        }

        private string GetKey(Orderbook item, string fileName)
        {
            return $"{item.AssetPair}_{item.IsBuy}_{fileName.Substring(0, 11)}";
        }
    }
}
