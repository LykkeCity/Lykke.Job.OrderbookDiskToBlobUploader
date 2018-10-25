using System.Threading.Tasks;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Core.Services
{
    public interface IStartupManager
    {
        Task StartAsync();
    }
}
