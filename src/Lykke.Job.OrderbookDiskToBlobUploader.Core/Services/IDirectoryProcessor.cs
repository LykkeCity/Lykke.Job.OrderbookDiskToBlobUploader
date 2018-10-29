using System.Threading.Tasks;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Core.Services
{
    public interface IDirectoryProcessor
    {
        Task<string> ProcessDirectoryAsync(string directoryPath);
    }
}
