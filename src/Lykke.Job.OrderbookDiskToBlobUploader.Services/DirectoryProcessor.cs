using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Services
{
    public class DirectoryProcessor : IDirectoryProcessor
    {
        private const string _inProgressMarkFile = "InProgress.txt";

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

            string inProgressFilePath = Path.Combine(directoryPath, _inProgressMarkFile);
            if (File.Exists(inProgressFilePath))
            {
                var creationDate = File.GetCreationTimeUtc(inProgressFilePath);
                if (DateTime.UtcNow.Subtract(creationDate).TotalDays < 1)
                    return;
            }
            else
            {
                try
                {
                    File.Create(inProgressFilePath).Dispose();
                }
                catch (Exception ex)
                {
                    if (!File.Exists(inProgressFilePath))
                        await _log.WriteWarningAsync(
                            nameof(DirectoryProcessor),
                            nameof(ProcessDirectoryAsync),
                            "Couldn't create in progress mark file",
                            ex);
                    return;
                }
            }

            var dirsToProcess = dirs.OrderBy(i => i).ToList();
            try
            {
                for (int i = 0; i < dirsToProcess.Count - 1; ++i)
                {
                    string dir = dirsToProcess[i];

                    var files = Directory.EnumerateFiles(dir, "*", SearchOption.TopDirectoryOnly);

                    var messages = new List<string>();
                    int filesCount = 0;
                    foreach (var file in files)
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
                        ++filesCount;
                    }

                    string container = Path.GetFileName(directoryPath);
                    string storagePath = Path.GetFileName(dir);
                    try
                    {
                        await _blobSaver.SaveToBlobAsync(messages, container, storagePath);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Couldn't save on {container} into {storagePath}", ex);
                    }

                    foreach (var file in files)
                    {
                        File.Delete(file);
                    }
                    File.Delete(inProgressFilePath);
                    try
                    {
                        Directory.Delete(dir);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Failed to delete {dir} folder", ex);
                    }

                    await _log.WriteInfoAsync(
                        "DirectoryProcessor.ProcessDirectoryAsync",
                        container,
                        $"Uploaded and deleted {filesCount} files for {storagePath}");
                }
            }
            catch (Exception ex)
            {
                await _log.WriteErrorAsync("DirectoryProcessor.ProcessDirectoryAsync", directoryPath, ex);
            }
        }
    }
}
