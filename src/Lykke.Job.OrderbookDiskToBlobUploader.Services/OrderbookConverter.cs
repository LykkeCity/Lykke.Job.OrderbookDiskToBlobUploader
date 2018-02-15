using System.Text;
using Common;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Domain.Models;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Services
{
    public static class OrderbookConverter
    {
        public static Orderbook Deserialize(string data)
        {
            return data.DeserializeJson<Orderbook>();
        }

        public static string FormatMessage(Orderbook item)
        {
            var strBuilder = new StringBuilder("{\"t\":\"");
            strBuilder.Append(item.Timestamp.ToString("mm:ss.fff"));
            strBuilder.Append("\",\"p\":[");
            for (int i = 0; i < item.Prices.Count; ++i)
            {
                var price = item.Prices[i];
                if (i > 0)
                    strBuilder.Append(",");
                strBuilder.Append("{\"v\":");
                strBuilder.Append(price.Volume);
                strBuilder.Append(",\"p\":");
                strBuilder.Append(price.Price);
                strBuilder.Append("}");
            }
            strBuilder.Append("]}");

            return strBuilder.ToString();
        }
    }
}
