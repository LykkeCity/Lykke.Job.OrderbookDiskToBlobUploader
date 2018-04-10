using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using JetBrains.Annotations;
using Common;
using Common.Log;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.PeriodicalHandlers
{
    [UsedImplicitly]
    public class MainPeriodicalHandler : TimerPeriod
    {
        private const int _workerReduceReserveInMinutes = 15;
        private const int _minProcessedDirectoriesCountForWorkersChange = 10;

        private readonly ILog _log;
        private readonly IDirectoryProcessor _directoryProcessor;
        private readonly string _diskPath;
        private readonly int _workersMaxCount;
        private readonly int _workersMinCount;

        private volatile int _processedDirectoriesCount;
        private DateTime? _idleExecutionStart;
        private int _workersCount;
        private bool _firstExecution = true;
        private bool _apiIsReady;

        public MainPeriodicalHandler(
            ILog log,
            IDirectoryProcessor directoryProcessor,
            string diskPath,
            int workersMaxCount,
            int workersMinCount) :
            base(nameof(MainPeriodicalHandler), (int)TimeSpan.FromMinutes(1).TotalMilliseconds, log)
        {
            _log = log;
            _directoryProcessor = directoryProcessor;
            _diskPath = diskPath;
            _workersMaxCount = workersMaxCount <= 0 ? 8 : workersMaxCount;
            _workersMinCount = workersMinCount <= 0 ? 2 : workersMinCount;
            _workersCount = (_workersMaxCount + _workersMinCount) / 2;
            Directory.SetCurrentDirectory(_diskPath);
        }

        public override async Task Execute()
        {
            while(!_apiIsReady)
            {
                var request = WebRequest.Create($"http://localhost:{Program.Port}/api/isalive");
                try
                {
                    var response = (HttpWebResponse)request.GetResponse();
                    _apiIsReady = response.StatusCode == HttpStatusCode.OK;
                    if (!_apiIsReady)
                        await Task.Delay(TimeSpan.FromSeconds(1));
                }
                catch
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }

            _processedDirectoriesCount = 0;
            var start = DateTime.UtcNow;
            var directories = Directory.GetDirectories(_diskPath, "*", SearchOption.TopDirectoryOnly);
            var concurrentQueue = new ConcurrentQueue<string>(directories);
            var workerTasks = Enumerable.Range(0, _workersCount).Select(i => ProcessDirectoriesAsync(i, concurrentQueue));
            await Task.WhenAll(workerTasks);

            await _log.WriteInfoAsync(nameof(MainPeriodicalHandler), nameof(Execute), $"{_processedDirectoriesCount} directories are processed.");

            if (_processedDirectoriesCount > _minProcessedDirectoriesCountForWorkersChange)
            {
                if (_idleExecutionStart.HasValue)
                {
                    _idleExecutionStart = null;
                }
                else
                {
                    if (!_firstExecution && _workersCount <= _workersMaxCount - 2)
                    {
                        _workersCount += 2;
                        await _log.WriteInfoAsync("MainPeriodicalHandler.Execute", "WorkersIncreased", $"Increased workers count to {_workersCount}.");
                    }
                    if (_firstExecution)
                        _firstExecution = false;
                }
            }
            else
            {
                if (!_idleExecutionStart.HasValue)
                {
                    _idleExecutionStart = start;
                }
                else if ((DateTime.UtcNow - _idleExecutionStart.Value).TotalMinutes >= _workerReduceReserveInMinutes)
                {
                    if (_workersCount >= _workersMinCount + 1)
                    {
                        --_workersCount;
                        await _log.WriteInfoAsync("MainPeriodicalHandler.Execute", "WorkersDecreased", $"Decreased workers count to {_workersCount}.");    
                    }
                    _idleExecutionStart = null;
                    _firstExecution = true;
                }
            }
        }

        private async Task ProcessDirectoriesAsync(int num, ConcurrentQueue<string> directories)
        {
            await Task.Delay(TimeSpan.FromMinutes(num));

            bool needToProcess = directories.TryDequeue(out string directory);
            while(needToProcess)
            {
                bool directoryProcessed = await _directoryProcessor.ProcessDirectoryAsync(directory);
                if (directoryProcessed)
                    Interlocked.Increment(ref _processedDirectoriesCount);
                needToProcess = directories.TryDequeue(out directory);
            }
        }
    }
}
