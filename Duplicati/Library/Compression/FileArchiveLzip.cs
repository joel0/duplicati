using Duplicati.Library.Interface;
using SharpCompress.Archives;
using SharpCompress.Common;
using SharpCompress.Writers;
using SharpCompress.Writers.Tar;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Duplicati.Library.Compression
{
    /// <summary>
    /// An abstraction of an lzip archive as a FileArchive, based on SharpCompress.
    /// Like FileArchiveZip, read &amp; write access at the same time has not been implemented.
    /// </summary>
    class FileArchiveLzip : ICompression {
        /// <summary>
        /// The tag used for logging
        /// </summary>
        private static readonly string LOGTAG = Logging.Log.LogTagFromType<FileArchiveLzip>();

        private const string CannotReadWhileWriting = "Cannot read while writing";
        private const string CannotWriteWhileReading = "Cannot write while reading";

        /// <summary>
        /// The commandline option for setting the compression level
        /// </summary>
        private const string COMPRESSION_LEVEL_OPTION = "lzip-compression-level";

        // TODO: default compression level

        /// <summary>
        /// This property indicates reading or writing access mode of the file archive
        /// </summary>
        readonly ArchiveMode m_mode;

        // TODO: this doc isn't right. There's no ZipArchive or LzipArchive.
        /// <summary>
        /// The LzipArchive instance used when reading archives
        /// </summary>
        private IArchive m_archive;

        /// <summary>
        /// The stream used to either read or write the archive
        /// </summary>
        private Stream m_stream;

        /// <summary>
        /// Lookup table for faster access to entries based on their names
        /// </summary>
        private Dictionary<string, IEntry> m_entryDict;

        /// <summary>
        /// The writer instance used when creating archives
        /// </summary>
        private IWriter m_writer;

        // TODO: default compression level

        /// <summary>
        /// Default constructor, used to read file extension and supported commands
        /// </summary>
        public FileArchiveLzip() { }

        private IArchive Archive
        {
            get
            {
                if (m_archive == null)
                {
                    m_stream.Position = 0;
                    m_archive = ArchiveFactory.Open(m_stream);
                }
                return m_archive;
            }
        }

        public Stream GetStreamFromReader(IEntry entry)
        {
            SharpCompress.Readers.Tar.TarReader rd = null;

            try
            {
                rd = SharpCompress.Readers.Tar.TarReader.Open(m_stream);

                while (rd.MoveToNextEntry())
                {
                    if (entry.Key == rd.Entry.Key)
                    {
                        return new StreamWrapper(rd.OpenEntryStream(), stream =>
                        {
                            rd.Dispose();
                        });
                    }
                }

                throw new Exception(string.Format("Stream not found: {0}", entry.Key));
            }
            catch
            {
                if (rd != null)
                {
                    rd.Dispose();
                }

                throw;
            }
        }

        /// <summary>
        /// Constructs a new lzip instance.
        /// </summary>
        /// <param name="stream">The stream to read or write depending on access mode</param>
        /// <param name="mode">The archive access mode</param>
        /// <param name="options">The options passed on the commandline</param>
        public FileArchiveLzip(Stream stream, ArchiveMode mode, IDictionary<string, string> options)
        {
            m_stream = stream;
            m_mode = mode;
            if (mode == ArchiveMode.Write)
            {
                var compression = new TarWriterOptions(CompressionType.LZip, true);

                // TODO: can the compression level be set?

                string cplevel;
                int tmplevel;
                if (options.TryGetValue(COMPRESSION_LEVEL_OPTION, out cplevel) && int.TryParse(cplevel, out tmplevel))
                {
                    // TODO: set compression level to tmplevel
                }

                m_writer = WriterFactory.Open(m_stream, ArchiveType.Tar, compression);

                // TODO: set m_flushBufferSize for the size of endheader?
            }
        }

        #region IFileArchive Members
        /// <summary>
        /// Gets the filename extension used by the compression module
        /// </summary>
        public string FilenameExtension { get { return "tar.lz"; } }
        /// <summary>
        /// Gets a friendly name for the compression module
        /// </summary>
        public string DisplayName { get { return Strings.FileArchiveLzip.DisplayName; } }
        /// <summary>
        /// Gets a description of the compression module
        /// </summary>
        public string Description { get { return Strings.FileArchiveLzip.Description; } }

        /// <summary>
        /// Gets a list of commands supported by the compression module
        /// </summary>
        public IList<ICommandLineArgument> SupportedCommands
        {
            get
            {
                // TODO: allow compression options
                return new List<ICommandLineArgument>(new ICommandLineArgument[] {
                    //new CommandLineArgument(COMPRESSION_LEVEL_OPTION, CommandLineArgument.ArgumentType.Enumeration, Strings.FileArchiveLzip.CompressionlevelShort, Strings.FileArchiveLzip.CompressionlevelLong, DEFAULT_COMPRESSION_LEVEL.ToString(), new string[] {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"})
                });
            }
        }

        /// <summary>
        /// Returns a list of files matching the given prefix
        /// </summary>
        /// <param name="prefix">The prefix to match</param>
        /// <returns>A list of files matching the prefix</returns>
        public string[] ListFiles(string prefix)
        {
            return ListFilesWithSize(prefix).Select(x => x.Key).ToArray();
        }

        /// <summary>
        /// Returns a list of files matching the given prefix
        /// </summary>
        /// <param name="prefix">The prefix to match</param>
        /// <returns>A list of files matching the prefix</returns>
        public IEnumerable<KeyValuePair<string, long>> ListFilesWithSize(string prefix)
        {
            LoadEntryTable();
            var q = m_entryDict.Values.AsEnumerable();
            if (!string.IsNullOrEmpty(prefix))
            {
                q = q.Where(x =>
                            x.Key.StartsWith(prefix, Utility.Utility.ClientFilenameStringComparison)
                            ||
                            x.Key.Replace('\\', '/').StartsWith(prefix, Utility.Utility.ClientFilenameStringComparison)
                            );
            }

            return q.Select(x => new KeyValuePair<string, long>(x.Key, x.Size)).ToArray();
        }

        /// <summary>
        /// Opens a file for reading
        /// </summary>
        /// <param name="file">The name of the file to open</param>
        /// <returns>A stream with the file contents</returns>
        public Stream OpenRead(string file)
        {
            if (m_mode != ArchiveMode.Read)
            {
                throw new InvalidOperationException(CannotReadWhileWriting);
            }

            var ze = GetEntry(file);
            if (ze == null)
            {
                return null;
            }

            if (ze is IArchiveEntry entry)
            {
                return entry.OpenEntryStream();
            }
            else if (ze is SharpCompress.Common.Tar.TarEntry)
            {
                return GetStreamFromReader(ze);
            }

            throw new Exception(string.Format("Unexpected result: {0}", ze.GetType().FullName));
        }

        /// <summary>
        /// Helper method to load the entry table
        /// </summary>
        private void LoadEntryTable()
        {
            if (m_entryDict == null)
            {
                var d = new Dictionary<string, IEntry>(Utility.Utility.ClientFilenameStringComparer);
                try
                {
                    foreach (var en in Archive.Entries)
                    {
                        d[en.Key] = en;
                    }
                }
                catch (Exception ex)
                {
                    // If we have zero files, or just a manifest, don't bother
                    if (d.Count < 2)
                    {
                        throw;
                    }

                    Logging.Log.WriteWarningMessage(LOGTAG, "BrokenArchive", ex, "Lzip archive appears to have broken records; returning the {0} records that could be recovered", d.Count);
                }

                m_entryDict = d;
            }
        }

        /// <summary>
        /// Internal function that returns a TarEntry for a filename, or null if no such file exists
        /// </summary>
        /// <param name="file">The name of the file to find</param>
        /// <returns>The TarEntry for the file or null if no such file was found</returns>
        private IEntry GetEntry(string file)
        {
            if (m_mode != ArchiveMode.Read)
            {
                throw new InvalidOperationException(CannotReadWhileWriting);
            }

            LoadEntryTable();

            IEntry e;
            if (m_entryDict.TryGetValue(file, out e))
            {
                return e;
            }
            if (m_entryDict.TryGetValue(file.Replace('/', '\\'), out e))
            {
                return e;
            }

            return null;
        }

        /// <summary>
        /// Creates a file in the archive and returns a writable stream.
        /// </summary>
        /// <param name="file">The name of the file to create</param>
        /// <param name="hint">A hint to the compressor as to how compressible the file data is</param>
        /// <param name="lastWrite">The time the file was last written</param>
        /// <returns>A writable stream for the file contents</returns>
        public virtual Stream CreateFile(string file, CompressionHint hint, DateTime lastWrite)
        {
            if (m_mode != ArchiveMode.Write)
            {
                throw new InvalidOperationException(CannotWriteWhileReading);
            }

            // TODO: update m_flushBufferSize

            return new FileBufferStream(file, (TarWriter)m_writer, lastWrite);
        }

        /// <summary>
        /// An in-memory buffer that will take a file that will be written to a Tar archive when the stream is disposed.
        /// </summary>
        private class FileBufferStream : MemoryStream
        {
            /// <summary>
            /// The file name within the archive that this stream will be written to.
            /// </summary>
            private string m_fileName;
            /// <summary>
            /// The TarWriter where this file will be written when this stream is disposed.
            /// </summary>
            private TarWriter m_archiveWriter;
            /// <summary>
            /// The file modification time of the file inside the archive.
            /// </summary>
            private DateTime m_lastWriteTime;

            public FileBufferStream(string name, TarWriter archiveWriter, DateTime lastWrite)
            {
                m_fileName = name;
                m_archiveWriter = archiveWriter;
                m_lastWriteTime = lastWrite;
            }

            /// <summary>
            /// Writes the file to the archive writer provided in the constructor, then disposes the memory stream.
            /// </summary>
            /// <param name="disposing">
            /// true to release both managed and unmanaged resources; false to release only unmanaged resources.
            /// </param>
            protected override void Dispose(bool disposing)
            {
                m_archiveWriter.Write(m_fileName, this, m_lastWriteTime, Length);

                base.Dispose(disposing);
            }
        }

        /// <summary>
        /// Returns a value that indiciates whether the file exists
        /// </summary>
        /// <param name="file">The name of the file to test existence for</param>
        /// <returns>True if the file exists, false otherwise</returns>
        public bool FileExists(string file)
        {
            if (m_mode != ArchiveMode.Read)
            {
                throw new InvalidOperationException(CannotReadWhileWriting);
            }

            return GetEntry(file) != null;
        }

        /// <summary>
        /// Gets the current size of the archive
        /// </summary>
        public long Size
        {
            get
            {
                return m_mode == ArchiveMode.Write ? m_stream.Length : Archive.TotalSize;
            }
        }

        /// <summary>
        /// The size of the current unflushed buffer
        /// </summary>
        public long FlushBufferSize
        {
            get
            {
                return 0; // TODO: return actuall m_flushBufferSize
            }
        }

        public DateTime GetLastWriteTime(string file)
        {
            IEntry entry = GetEntry(file);
            if (entry != null)
            {
                if (entry.LastModifiedTime.HasValue)
                {
                    return entry.LastModifiedTime.Value;
                }
                else
                {
                    return DateTime.MinValue;
                }
            }

            throw new FileNotFoundException(Strings.FileArchiveLzip.FileNotFoundError(file));
        }
        #endregion

        #region IDispose Members
        public void Dispose()
        {
            if (m_archive != null)
            {
                m_archive.Dispose();
            }
            m_archive = null;

            if (m_writer != null)
            {
                m_writer.Dispose();
            }
            m_writer = null;

            m_stream = null;
        }
        #endregion
    }
}
