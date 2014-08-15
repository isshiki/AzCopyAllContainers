using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace AzCopyAllContainers
{
    class Program
    {
        static int Main(string[] args)
        {
            try
            {
                string usage = String.Format("{0}\n\nAzCopyAllContainers sourceAccount sourcKey destAccount destKey\n", GetHeaderInfo());

                if (args.Length != 4)
                    throw new ApplicationException(usage);

                CopyAllContainers(args[0], args[1], args[2], args[3]);
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.BackgroundColor = ConsoleColor.Red;
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine("Error: {0}", ex.Message);
                Console.ResetColor();
                Debug.Assert(false);
                return 1; // error
            }

            return 0;
        }

        private static string GetHeaderInfo()
        {
            Assembly entryAsm = Assembly.GetEntryAssembly();
            Assembly execAsm = Assembly.GetExecutingAssembly();
            var asmName = execAsm.GetName();
            var name = asmName.Name;
            var version = asmName.Version.ToString();
            var copyright = ((AssemblyCopyrightAttribute)Attribute.GetCustomAttribute(execAsm, typeof(AssemblyCopyrightAttribute))).Copyright;
            var description = ((AssemblyDescriptionAttribute)Attribute.GetCustomAttribute(execAsm, typeof(AssemblyDescriptionAttribute))).Description;
            return String.Format("{0} {1}\n{2}\n\n{3}", name, version, copyright, description);
        }


        private static CloudStorageAccount _storageAccountSrc;
        private static CloudBlobClient _blobClientSrc;
        private static CloudStorageAccount _storageAccountDst;
        private static CloudBlobClient _blobClientDst;

        private static void CopyAllContainers(string srcAccountName, string srcAccountKey, string destAccountName, string destAccountKey)
        {
            Console.WriteLine("Accessing Azure Storages ...");
            InitAzureStorage(srcAccountName, srcAccountKey, destAccountName, destAccountKey);

            Console.WriteLine("Creating BLOB Storage's containers ...");
            var commandArgsListOfAzCopy = new List<string>();
            var allContainersSrc = _blobClientSrc.ListContainers(null, ContainerListingDetails.All, null, null);
            foreach (var containerSrc in allContainersSrc)
            {
                var permissionsContainerSrc = containerSrc.GetPermissions();
                var metadataContainerSrc = containerSrc.Metadata;
                //var propertiesContainerSrc = containerSrc.Properties; // cannot set container's propeties...
                CloudBlobContainer containerDst = _blobClientDst.GetContainerReference(containerSrc.Name);
                containerDst.CreateIfNotExists(permissionsContainerSrc.PublicAccess);
                containerDst.SetPermissions(permissionsContainerSrc);
                if (metadataContainerSrc != null)
                {
                    containerDst.Metadata.Clear();
                    foreach (var metadataItem in metadataContainerSrc)
                    {
                        containerDst.Metadata.Add(metadataItem);
                    }
                    containerDst.SetMetadata();
                }
                Console.WriteLine("\nCONTAINER \"{0}\" was created. Copying BLOBs ...", containerSrc.Name);

                var allBlobsSrc = containerSrc.ListBlobs(null, true, BlobListingDetails.All, null, null);
                int i = 0;
                int count = allBlobsSrc.Count();
                int maxLengthName = 0;
                Parallel.ForEach(allBlobsSrc, blobSrc =>
                {
                    var blobRefSrc = _blobClientSrc.GetBlobReferenceFromServer(blobSrc.StorageUri);
                    var blobRefDst = (blobRefSrc.IsSnapshot) ? containerDst.GetBlockBlobReference(blobRefSrc.Name, blobRefSrc.SnapshotTime) : containerDst.GetBlockBlobReference(blobRefSrc.Name);

                    if (maxLengthName < blobRefSrc.Name.Length) maxLengthName = blobRefSrc.Name.Length;
                    Console.SetCursorPosition(0, Console.CursorTop);
                    Console.Write("{0}/{1} ({2}%) {3}                {4}", i + 1, count, (i + 1) * 100 / count, blobRefSrc.Name,
                        (maxLengthName > blobRefSrc.Name.Length) ? "" : new string(' ', maxLengthName - blobRefSrc.Name.Length));
                    Console.SetCursorPosition(0, Console.CursorTop);

                    var metadataBlobSrc = blobRefDst.Metadata;
                    var propetiesBlobSrc = blobRefSrc.Properties;

                    using (var streamSrc = blobRefSrc.OpenRead())
                    {
                        blobRefDst.UploadFromStream(streamSrc);
                        if (propetiesBlobSrc != null)
                        {
                            blobRefDst.Properties.CacheControl = propetiesBlobSrc.CacheControl;
                            blobRefDst.Properties.ContentDisposition = propetiesBlobSrc.ContentDisposition;
                            blobRefDst.Properties.ContentEncoding = propetiesBlobSrc.ContentEncoding;
                            blobRefDst.Properties.ContentLanguage = propetiesBlobSrc.ContentLanguage;
                            blobRefDst.Properties.ContentMD5 = propetiesBlobSrc.ContentMD5;
                            blobRefDst.Properties.ContentType = propetiesBlobSrc.ContentType;
                            blobRefDst.SetProperties();
                        }
                        if (metadataBlobSrc != null)
                        {
                            blobRefDst.Metadata.Clear();
                            foreach (var metadataItem in metadataBlobSrc)
                            {
                                blobRefDst.Metadata.Add(metadataItem);
                            }
                            blobRefDst.SetMetadata();
                        }
                    }
                    i++;
                });
                Console.SetCursorPosition(0, Console.CursorTop);
                Console.Write("{0}/{0} (100%)                 {1}", count, new string(' ', maxLengthName));
            }

            Console.WriteLine("\n\nAll Suceeded.\ntype any key to finish.");
            Console.ReadKey();
        }

        private static bool InitAzureStorage(string srcAccountName, string srcAccountKey, string destAccountName, string destAccountKey)
        {
            if (_storageAccountSrc == null)
            {
                _storageAccountSrc = CloudStorageAccount.Parse(String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", srcAccountName, srcAccountKey));
            }
            if (_blobClientSrc == null)
            {
                _blobClientSrc = _storageAccountSrc.CreateCloudBlobClient();
            }
            if (_storageAccountDst == null)
            {
                _storageAccountDst = CloudStorageAccount.Parse(String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", destAccountName, destAccountKey));
            }
            if (_blobClientDst == null)
            {
                _blobClientDst = _storageAccountDst.CreateCloudBlobClient();
            }
            return true;
        }



    }
}
