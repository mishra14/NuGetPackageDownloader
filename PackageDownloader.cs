using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NuGet.CatalogVisitor;

namespace NugetPackageDownloader
{
    internal class PackageDownloader
    {
        private static Dictionary<string, Dictionary<string, PackageMetadata>> packageMap;
        private static string packageUrlPrefix;
        private static string packageSavePath;
        private static string logSavePath;
        private static string processedLogSavePath;
        private static string errorLogPath;

        private static void Main(string[] args)
        {
            packageMap = new Dictionary<string, Dictionary<string, PackageMetadata>>();
            packageSavePath = @"F:\MirrorPackages";
            logSavePath = @"F:\PackageLogs";
            processedLogSavePath = @"F:\PackageLogsProcessed";
            errorLogPath = @"F:\ErrorLogs";
            packageUrlPrefix = @"https://api.nuget.org/v3-flatcontainer/";
            while (true)
            {
                if (Directory.GetFileSystemEntries(logSavePath).Length == 0)
                {
                    parallelFetchPackages();
                    packageMap.Clear();
                    Thread.Sleep(1000);
                }
                else
                {
                    var processedFiles = new List<string>();
                    foreach (string file in Directory.EnumerateFiles(logSavePath, "*.txt"))
                    {
                        using (StreamReader sr = File.OpenText(file))
                        {
                            string line = string.Empty;
                            while ((line = sr.ReadLine()) != null)
                            {
                                var metadata = processLine(line);
                                if (packageMap.ContainsKey(metadata.Id))
                                {
                                    if (!packageMap[metadata.Id].ContainsKey(metadata.Version.ToString()))
                                    {
                                        packageMap[metadata.Id][metadata.Version.ToString()] = metadata;
                                    }
                                    else if (packageMap[metadata.Id].ContainsKey(metadata.Version.ToString()) && metadata.CommitTimeStamp > packageMap[metadata.Id][metadata.Version.ToString()].CommitTimeStamp)
                                    {
                                        packageMap[metadata.Id][metadata.Version.ToString()] = metadata;
                                    }
                                }
                                else
                                {
                                    packageMap[metadata.Id] = new Dictionary<string, PackageMetadata> { { metadata.Version.ToString(), metadata } };
                                }
                            }
                        }
                        processedFiles.Add(file);
                    }
                    foreach (var filename in processedFiles)
                    {
                        var sourceFile = Path.Combine(logSavePath, filename);
                        var destFile = Path.Combine(processedLogSavePath, Path.GetFileName(filename));
                        if (!Directory.Exists(processedLogSavePath))
                        {
                            Directory.CreateDirectory(processedLogSavePath);
                        }
                        File.Move(sourceFile, destFile);
                    }
                }
            }
        }

        private static PackageMetadata processLine(string line)
        {
            var metadataStrings = line.Split(null, 3);
            return new PackageMetadata(new NuGet.Versioning.NuGetVersion(metadataStrings[1]), metadataStrings[0], DateTimeOffset.Parse(metadataStrings[2]));
        }

        private static void parallelFetchPackages()
        {
            long count = 0;
            // parallel fetch
            try
            {
                Console.WriteLine("Starting downloads...");

                Parallel.ForEach(packageMap, package =>
                {
                    count++;
                    if (count % 1000 == 0)
                    {
                        Console.WriteLine(string.Format("Downloading - {0}. Packages Downloaded - {1}.", package.Key, count));
                    }
                    downloadPackage(package.Key, package.Value);
                });
            }
            catch (AggregateException ae)
            {
                foreach (var ex in ae.InnerExceptions)
                {
                    Console.WriteLine(ex);
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Done with " + count + " packages");
        }

        private static void downloadPackage(string packageId, Dictionary<string, PackageMetadata> packageVersions)
        {
            foreach (var packageVersion in packageVersions)
            {
                var packageMetadata = packageVersion.Value;
                // {id}/{version}/{id}.{version}.nupkg
                var packageSaveName = string.Format("{0}.{1}.nupkg", packageMetadata.Id, packageMetadata.Version);
                var urlPostfix = string.Format("{0}/{1}/{2}", packageMetadata.Id, packageMetadata.Version, packageSaveName);
                var urlComplete = string.Concat(packageUrlPrefix, urlPostfix);

                if (!File.Exists(Path.Combine(packageSavePath, packageSaveName)))
                {
                    try
                    {
                        Console.Write(".");
                        using (var webClient = new WebClient())
                        {
                            webClient.DownloadFile(new Uri(urlComplete), Path.Combine(packageSavePath, packageSaveName));
                        }
                    }
                    catch (WebException wex)
                    {
                        var errorResponse = wex.Response as HttpWebResponse;
                        if (errorResponse.StatusCode != HttpStatusCode.NotFound)
                        {
                            Console.WriteLine(string.Concat("Exception while downloading from ", urlComplete, " to ", Path.Combine(packageSavePath, packageSaveName)));
                            Console.WriteLine(wex);
                            logError(packageSaveName + " " + urlComplete + " " + wex.Message, Path.Combine(errorLogPath, @"error_" + Thread.CurrentThread.ManagedThreadId + ".txt"));
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(string.Concat("Exception while downloading from ", urlComplete, " to ", Path.Combine(packageSavePath, packageSaveName)));
                        Console.WriteLine(ex);
                    }
                }
            }
        }

        private static void logError(string line, string errorFilePath)
        {
            if (!File.Exists(errorFilePath))
            {
                File.Create(errorFilePath).Close();
            }
            using (StreamWriter w = File.AppendText(errorFilePath))
            {
                w.WriteLine(line);
            }
        }
    }
}