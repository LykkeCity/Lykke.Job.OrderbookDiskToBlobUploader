using System;
using System.Threading.Tasks;
using System.IO;
using System.Net;
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

        private bool _apiIsReady = false;

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

        public async override Task Execute()
        {
            while(!_apiIsReady)
            {
                WebRequest request = WebRequest.Create($"http://localhost:{Program.Port}/api/isalive");
                try
                {
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                    _apiIsReady = response != null && response.StatusCode == HttpStatusCode.OK;
                    if (!_apiIsReady)
                        await Task.Delay(TimeSpan.FromSeconds(1));
                }
                catch
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }

            var dirs = Directory.GetDirectories(_diskPath, "*", SearchOption.TopDirectoryOnly);
            Parallel.ForEach(
                dirs,
                new ParallelOptions { MaxDegreeOfParallelism = 8 },
                dir => _directoryProcessor.ProcessDirectoryAsync(dir).GetAwaiter().GetResult());
        }
    }
}
