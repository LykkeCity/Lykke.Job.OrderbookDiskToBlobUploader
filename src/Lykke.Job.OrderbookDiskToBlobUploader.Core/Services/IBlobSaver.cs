using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Core.Services
{
    public interface IBlobSaver
    {
        Task SaveToBlobAsync(
            IEnumerable<string> blocks,
            string containerName,
            string storagePath);
    }
}
