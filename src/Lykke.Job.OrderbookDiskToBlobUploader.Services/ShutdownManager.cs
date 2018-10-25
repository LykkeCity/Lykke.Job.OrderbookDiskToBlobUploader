using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using JetBrains.Annotations;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Services
{
    [UsedImplicitly]
    public class ShutdownManager : IShutdownManager
    {
        private readonly List<IStopable> _stopables = new List<IStopable>();

        public ShutdownManager(IEnumerable<IStartStop> stopables)
        {
            _stopables.AddRange(stopables);
        }

        public async Task StopAsync()
        {
            Parallel.ForEach(_stopables, s => s.Stop());

            await Task.CompletedTask;
        }
    }
}
