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

            string container = Path.GetFileName(directoryPath);

            _log.WriteInfo("DirectoryProcessor.ProcessDirectoryAsync", container, $"Found {dirs.Length} directories");

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
                    _log.WriteWarning(nameof(DirectoryProcessor), nameof(ProcessDirectoryAsync), "Error on in progress file creation", ex);
                    return false;
                }
            }

            var dirsToProcess = dirs.OrderBy(i => i).ToList();
            int processedDirsCount = 0;
            int dirsToPocessCount = dirsToProcess.Count - 1;
            string lastDir = dirsToProcess[dirsToProcess.Count - 1];
            if (DateTime.TryParseExact(lastDir, _timeFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out DateTime dirCreationTime))
            {
                var dateDiff = DateTime.UtcNow.Subtract(dirCreationTime).TotalDays;
                _log.WriteInfo("DirectoryProcessor.ProcessDirectoryAsync", container, $"Parsed {dirCreationTime} for {lastDir}. Diff = {dateDiff}");
                if (dateDiff >= 1)
                    dirsToPocessCount = dirsToProcess.Count;
            }

            _log.WriteInfo("DirectoryProcessor.ProcessDirectoryAsync", container, $"Will process {dirsToPocessCount} directories");

            try
            {
                for (int i = 0; i < dirsToPocessCount; ++i)
                {
                    string dir = dirsToProcess[i];

                    _log.WriteInfo("DirectoryProcessor.ProcessDirectoryAsync", container, $"Processing {dir}");

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

                    string storagePath = Path.GetFileName(dir);
                    try
                    {
                        await _blobSaver.SaveToBlobAsync(messages, container, storagePath);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Couldn't save on {container} into {storagePath}", ex);
                    }

                    _log.WriteInfo("DirectoryProcessor.ProcessDirectoryAsync", container, $"Processed {dir}");

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
                _log.WriteError("DirectoryProcessor.ProcessDirectoryAsync", container, ex);
            }
            return processedDirsCount > 0;
        }
    }
}
