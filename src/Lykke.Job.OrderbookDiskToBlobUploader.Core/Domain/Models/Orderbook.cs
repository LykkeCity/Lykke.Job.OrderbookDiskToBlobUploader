using System;
using System.Collections.Generic;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Core.Domain.Models
{
    public class Orderbook
    {
        public string AssetPair { get; set; }

        public bool IsBuy { get; set; }

        public DateTime Timestamp { get; set; }

        public List<VolumePrice> Prices { get; set; }
    }
}
