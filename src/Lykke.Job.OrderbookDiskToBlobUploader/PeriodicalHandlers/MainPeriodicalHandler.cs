﻿using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.IO;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.PeriodicalHandlers
{
    [UsedImplicitly]
    public class MainPeriodicalHandler : TimerPeriod, IStartStop
    {
        private const int _workerReduceReserveInMinutes = 15;
        private const int _minProcessedDirectoriesCountForWorkersChange = 10;

        private readonly ILog _log;
        private readonly IDirectoryProcessor _directoryProcessor;
        private readonly string _diskPath;
        private readonly int _workersMaxCount;
        private readonly int _workersMinCount;
        private readonly ConcurrentDictionary<string, bool> _processedDirectoriesDict = new ConcurrentDictionary<string, bool>();

        private DateTime? _idleExecutionStart;
        private int _workersCount;
        private bool _firstExecution = true;

        public MainPeriodicalHandler(
            ILog log,
            IDirectoryProcessor directoryProcessor,
            string diskPath,
            int workersMaxCount,
            int workersMinCount)
            : base(nameof(MainPeriodicalHandler), (int)TimeSpan.FromMinutes(1).TotalMilliseconds, log)
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
            _processedDirectoriesDict.Clear();
            var start = DateTime.UtcNow;
            var directories = Directory.GetDirectories(_diskPath, "*", SearchOption.TopDirectoryOnly);
            var concurrentQueue = new ConcurrentQueue<string>(directories);
            var workerTasks = Enumerable.Range(0, _workersCount).Select(i => ProcessDirectoriesAsync(i, concurrentQueue));
            await Task.WhenAll(workerTasks);

            if (_processedDirectoriesDict.Count > 0)
                _log.WriteInfo(
                    nameof(Execute),
                    string.Join(',', _processedDirectoriesDict.Keys),
                    $"{_processedDirectoriesDict.Count} directories are processed");

            if (_processedDirectoriesDict.Keys.Count > _minProcessedDirectoriesCountForWorkersChange)
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
                        _log.WriteInfo("MainPeriodicalHandler.Execute", "WorkersIncreased", $"Increased workers count to {_workersCount}.");
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
                        _log.WriteInfo("MainPeriodicalHandler.Execute", "WorkersDecreased", $"Decreased workers count to {_workersCount}.");
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
                string processedContainer = await _directoryProcessor.ProcessDirectoryAsync(directory);
                if (!string.IsNullOrWhiteSpace(processedContainer))
                    _processedDirectoriesDict.TryAdd(processedContainer, true);
                needToProcess = directories.TryDequeue(out directory);
            }
        }
    }
}
