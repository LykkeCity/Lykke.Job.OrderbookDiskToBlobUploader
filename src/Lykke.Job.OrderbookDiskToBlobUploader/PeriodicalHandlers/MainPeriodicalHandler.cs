using System;
using System.Linq;
using System.Collections.Concurrent;
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
        private readonly int _workersMaxCount;

        private bool _apiIsReady = false;

        public MainPeriodicalHandler(
            ILog log,
            IDirectoryProcessor directoryProcessor,
            string diskPath,
            int workersMaxCount) :
            base(nameof(MainPeriodicalHandler), (int)TimeSpan.FromMinutes(1).TotalMilliseconds, log)
        {
            _log = log;
            _directoryProcessor = directoryProcessor;
            _diskPath = diskPath;
            _workersMaxCount = workersMaxCount <= 0 ? 8 : workersMaxCount;
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

            var directories = Directory.GetDirectories(_diskPath, "*", SearchOption.TopDirectoryOnly);
            var concurrentQueue = new ConcurrentQueue<string>(directories);
            var workerTasks = Enumerable.Range(0, _workersMaxCount).Select(i => ProcessDirectoriesAsync(i, concurrentQueue));
            await Task.WhenAll(workerTasks);

            await _log.WriteInfoAsync(nameof(MainPeriodicalHandler), nameof(Execute), "Directories are processed.");
        }

        private async Task ProcessDirectoriesAsync(int num, ConcurrentQueue<string> directories)
        {
            await Task.Delay(TimeSpan.FromMinutes(num));

            bool needToProcess = directories.TryDequeue(out string directory);
            while(needToProcess)
            {
                await _directoryProcessor.ProcessDirectoryAsync(directory);
                needToProcess = directories.TryDequeue(out directory);
            }
        }
    }
}
