using System;
using System.Threading.Tasks;
using System.IO;
using Common;
using Common.Log;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.PeriodicalHandlers
{
    public class MainPeriodicalHandler : TimerPeriod
    {
        private readonly ILog _log;
        private readonly IDirectoryProcessor _directoryProcessor;
        private readonly string _diskPath;
        private readonly int _maxFilesInBatch;

        public MainPeriodicalHandler(
            ILog log,
            IDirectoryProcessor directoryProcessor,
            string diskPath,
            int maxFilesInBatch) :
            base(nameof(MainPeriodicalHandler), (int)TimeSpan.FromHours(1).TotalMilliseconds, log)
        {
            _log = log;
            _directoryProcessor = directoryProcessor;
            _diskPath = diskPath;
            _maxFilesInBatch = maxFilesInBatch <= 0 ? 1000 : maxFilesInBatch;
            Directory.SetCurrentDirectory(_diskPath);
        }

        public override Task Execute()
        {
            var dirs = Directory.GetDirectories(_diskPath, "*", SearchOption.TopDirectoryOnly);
            Parallel.ForEach(
                dirs,
                new ParallelOptions { MaxDegreeOfParallelism = 4 },
                dir => _directoryProcessor.ProcessDirectoryAsync(dir).GetAwaiter().GetResult());
            return Task.CompletedTask;
        }
    }
}
