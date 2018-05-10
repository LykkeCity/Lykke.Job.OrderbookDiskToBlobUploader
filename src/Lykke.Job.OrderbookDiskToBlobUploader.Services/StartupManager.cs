using System.Collections.Generic;
using System.Threading.Tasks;
using Autofac;
using JetBrains.Annotations;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Services
{
    [UsedImplicitly]
    public class StartupManager : IStartupManager
    {
        private readonly List<IStartable> _items = new List<IStartable>();

        public void Register(IStartable startable)
        {
            _items.Add(startable);
        }

        public Task StartAsync()
        {
            foreach (var item in _items)
            {
                item.Start();
            }

            return Task.CompletedTask;
        }
    }
}
