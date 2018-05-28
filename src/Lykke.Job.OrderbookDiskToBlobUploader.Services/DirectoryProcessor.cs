using System;
using System.Linq;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.OrderbookDiskToBlobUploader.Core.Services;

namespace Lykke.Job.OrderbookDiskToBlobUploader.Services
{
    public class DirectoryProcessor : IDirectoryProcessor
    {
        private const string _inProgressMarkFile = "InProgress.txt";
        private const string _timeFormat = "yyyy-MM-dd-HH";

        private readonly IBlobSaver _blobSaver;
        private readonly ILog _log;

        public DirectoryProcessor(IBlobSaver blobSaver, ILog log)
        {
            _blobSaver = blobSaver;
            _log = log;
        }

        public async Task<bool> ProcessDirectoryAsync(string directoryPath)
        {
            var dirs = Directory.GetDirectories(directoryPath, "*", SearchOption.TopDirectoryOnly);

            _log.WriteInfo("DirectoryProcessor.ProcessDirectoryAsync", directoryPath, $"Found {dirs.Length} directories");

            if (dirs.Length == 0)
                return false;

            string inProgressFilePath = Path.Combine(directoryPath, _inProgressMarkFile);
            if (File.Exists(inProgressFilePath))
            {
                var creationDate = File.GetCreationTimeUtc(inProgressFilePath);
                if (DateTime.UtcNow.Subtract(creationDate).TotalDays < 1)
                    return false;
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
                        _log.WriteWarning(nameof(DirectoryProcessor), nameof(ProcessDirectoryAsync), "Couldn't create in progress mark file", ex);
                    return false;
                }
            }

            var dirsToProcess = dirs.OrderBy(i => i).ToList();
            int processedDirsCount = 0;
            int dirsToPocessCount =
                DateTime.TryParseExact(dirsToProcess[dirsToProcess.Count - 1], _timeFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out DateTime dirCreationTime)
                && DateTime.UtcNow.Subtract(dirCreationTime).TotalDays >= 1
                ? dirsToProcess.Count
                : dirsToProcess.Count - 1;
            try
            {
                for (int i = 0; i < dirsToPocessCount; ++i)
                {
                    string dir = dirsToProcess[i];

                    _log.WriteInfo("DirectoryProcessor.ProcessDirectoryAsync", directoryPath, $"Processing {dir}");

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
                    try
                    {
                        Directory.Delete(dir);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Failed to delete {dir} folder", ex);
                    }

                    _log.WriteInfo(
                        "DirectoryProcessor.ProcessDirectoryAsync",
                        container,
                        $"Uploaded and deleted {filesCount} files for {storagePath}");

                    ++processedDirsCount;
                }
                File.Delete(inProgressFilePath);
            }
            catch (Exception ex)
            {
                _log.WriteError("DirectoryProcessor.ProcessDirectoryAsync", directoryPath, ex);
            }
            return processedDirsCount > 0;
        }
    }
}
