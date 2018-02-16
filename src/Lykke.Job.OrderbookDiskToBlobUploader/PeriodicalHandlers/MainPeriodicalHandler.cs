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
            base(nameof(MainPeriodicalHandler), (int)TimeSpan.FromMinutes(1).TotalMilliseconds, log)
        {
            _log = log;
            _blobSaver = blobSaver;
            _diskPath = diskPath;
            _maxFilesInBatch = maxFilesInBatch <= 0 ? 1000 : maxFilesInBatch;
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
                    string fileData = File.ReadAllText(file.FullName);
                    if (fileData.Length == 0)
                    {
                        if (file.LastWriteTimeUtc.Subtract(now) >= TimeSpan.FromMinutes(1))
                            File.Delete(file.FullName);
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

                    string key = $"{orderbook.AssetPair}_{orderbook.IsBuy}";
                    if (orderbookDict.ContainsKey(key))
                        orderbookDict[key].Add((orderbook, file.FullName));
                    else
                        orderbookDict.Add(key, new List<(Orderbook, string)> { (orderbook, file.FullName) });

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
                var byDates = orderbookDict[key].GroupBy(o => o.Item1.Timestamp.Date);
                foreach (var dateGroup in byDates)
                {
                    var byHour = dateGroup.GroupBy(i => i.Item1.Timestamp.Hour);
                    foreach (var hourGroup in byHour)
                    {
                        try
                        {
                            var messages = hourGroup.Select(i => OrderbookConverter.FormatMessage(i.Item1));
                            var first = hourGroup.First().Item1;
                            await _blobSaver.SaveToBlobAsync(messages, GetContainerName(first), first.Timestamp);

                            int filesCount = 0;
                            foreach (var item in hourGroup)
                            {
                                File.Delete(item.Item2);
                                ++filesCount;
                            }

                            await _log.WriteInfoAsync(nameof(MainPeriodicalHandler), nameof(UploadDataAsync), $"Uploaded and deleted {filesCount} files");
                        }
                        catch (Exception ex)
                        {
                            await _log.WriteErrorAsync(nameof(MainPeriodicalHandler), nameof(UploadDataAsync), ex);
                        }
                    }
                }
            }
        }

        private string GetContainerName(Orderbook item)
        {
            return item.AssetPair.ToLower() + (item.IsBuy ? "-buy" : "-sell");
        }
    }
}
