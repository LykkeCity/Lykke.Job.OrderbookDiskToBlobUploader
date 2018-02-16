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

        private const int _maxSize = 1024 * 1024;
        private readonly byte[] _buffer = new byte[_maxSize];

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
                    int ind = i % 4;
                    foreach (var file in files)
                    {
                        if (ind == 0)
                            Read1(file, messages);
                        else if (ind == 1)
                            Read2(file, messages);
                        else if (ind == 2)
                            Read3(file, messages);
                        else if (ind == 3)
                            Read4(file, messages);
                        //messages.Add();
                        ++filesCount;
                    }
                    watch.Stop();
                    await _log.WriteInfoAsync(nameof(DirectoryProcessor), nameof(ProcessDirectoryAsync), $"{ind} - {watch.ElapsedMilliseconds}");

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

        private void Read1(string file, List<string> messages)
        {
            messages.Add(File.ReadAllText(file));
        }

        private void Read2(string file, List<string> messages)
        {
            using (var sr = File.OpenText(file))
            {
                messages.Add(sr.ReadToEnd());
            }
        }

        private void Read3(string file, List<string> messages)
        {
            using (var sr = File.OpenText(file))
            {
                do
                {
                    var str = sr.ReadLine();
                    if (str == null)
                        break;
                    messages.Add(str);
                } while (true);
            }
        }

        private void Read4(string file, List<string> messages)
        {
            using (var fs = File.OpenRead(file))
            {
                int read = fs.Read(_buffer, 0, _maxSize);
                string str = System.Text.Encoding.Default.GetString(_buffer, 0, read);
                messages.Add(str);
            }
        }
    }
}
