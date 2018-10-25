using Autofac;
using Common;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Core.Services
{
    public interface IStartStop : IStartable, IStopable
    {
    }
}
