using System;
using System.Linq;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Services
{
    public class DirectoryProcessor : IDirectoryProcessor
    {
        private readonly IBlobSaver _blobSaver;
        private readonly ILog _log;

        public DirectoryProcessor(IBlobSaver blobSaver, ILog log)
        {
            _blobSaver = blobSaver;
            _log = log;
        }

        public async Task ProcessDirectoryAsync(string directoryPath)
        {
            var dirs = Directory.GetDirectories(directoryPath, "*", SearchOption.TopDirectoryOnly);
            if (dirs.Length <= 1)
                return;

            int maxSize = 1024 * 1024;
            var buffer = new byte[maxSize];
            var dirsToProcess = dirs.OrderBy(i => i).ToList();
            var watch = new Stopwatch();
            try
            {
                for (int i = 0; i < dirsToProcess.Count - 1; ++i)
                {
                    string dir = dirsToProcess[i];

                    var files = Directory.EnumerateFiles(dir, "*", SearchOption.TopDirectoryOnly);

                    watch.Restart();
                    var messages = new List<string>();
                    int filesCount = 0;
                    foreach (var file in files)
                    {
                        messages.Add(File.ReadAllText(file));
                        ++filesCount;
                    }
                    watch.Stop();
                    await _log.WriteInfoAsync(nameof(DirectoryProcessor), nameof(ProcessDirectoryAsync), $"1 - {watch.ElapsedMilliseconds}");

                    watch.Restart();
                    var messages2 = new List<string>();
                    foreach (var file in files)
                    {
                        using (var sr = File.OpenText(file))
                        {
                            messages2.Add(sr.ReadToEnd());
                        }
                    }
                    watch.Stop();
                    await _log.WriteInfoAsync(nameof(DirectoryProcessor), nameof(ProcessDirectoryAsync), $"2 - {watch.ElapsedMilliseconds}");

                    watch.Restart();
                    var messages3 = new List<string>();
                    foreach (var file in files)
                    {
                        using (var sr = File.OpenText(file))
                        {
                            do
                            {
                                var str = sr.ReadLine();
                                if (str == null)
                                    break;
                                messages3.Add(str);
                            } while (true);
                        }
                    }
                    watch.Stop();
                    await _log.WriteInfoAsync(nameof(DirectoryProcessor), nameof(ProcessDirectoryAsync), $"3 - {watch.ElapsedMilliseconds}");

                    watch.Restart();
                    var messages4 = new List<string>();
                    foreach (var file in files)
                    {
                        using (var fs = File.OpenRead(file))
                        {
                            int read = fs.Read(buffer, 0, maxSize);
                            string str = System.Text.Encoding.Default.GetString(buffer, 0, read);
                        }
                    }
                    watch.Stop();
                    await _log.WriteInfoAsync(nameof(DirectoryProcessor), nameof(ProcessDirectoryAsync), $"4 - {watch.ElapsedMilliseconds}");

                    string container = Path.GetFileName(directoryPath);
                    string storagePath = Path.GetFileName(dir);
                    await _blobSaver.SaveToBlobAsync(messages, container, storagePath);

                    foreach (var file in files)
                    {
                        File.Delete(file);
                    }
                    Directory.Delete(dir);

                    await _log.WriteInfoAsync(
                        nameof(DirectoryProcessor),
                        nameof(ProcessDirectoryAsync),
                        $"Uploaded and deleted {filesCount} files for {container}/{storagePath}");
                }
            }
            catch (Exception ex)
            {
                await _log.WriteErrorAsync(nameof(DirectoryProcessor), nameof(ProcessDirectoryAsync), ex);
            }
        }
    }
}
