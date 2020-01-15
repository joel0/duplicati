#region Disclaimer / License
// Copyright (C) 2015, The Duplicati Team
// http://www.duplicati.com, info@duplicati.com
// 
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
// 
#endregion
using System;
using System.Collections.Generic;
using System.IO;
using Duplicati.Library.Interface;
using SharpCompress.Common;

using SharpCompress.Archives;
using SharpCompress.Writers;
using SharpCompress.Writers.Zip;
using SharpCompress.Readers;
using System.Linq;
using SharpCompress.Writers.Tar;

namespace Duplicati.Library.Compression
{
    /// <summary>
    /// An abstraction of an LZip archive as a FileArchive, based on SharpCompress.
    /// Please note, duplicati does not require both Read &amp; Write access at the same time so this has not been implemented.
    /// </summary>
    public class FileArchiveLZip : ICompression
    {
        /// <summary>
        /// The tag used for logging
        /// </summary>
        private static readonly string LOGTAG = Logging.Log.LogTagFromType<FileArchiveLZip>();

        private const string CannotReadWhileWriting = "Cannot read while writing";
        private const string CannotWriteWhileReading = "Cannot write while reading";

        // TODO update this
        /// <summary>
        /// Taken from SharpCompress ZipCentralDirectorEntry.cs
        /// </summary>
        private const int CENTRAL_HEADER_ENTRY_SIZE = 8 + 2 + 2 + 4 + 4 + 4 + 4 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 4;

        /// <summary>
        /// This property indicates reading or writing access mode of the file archive.
        /// </summary>
        readonly ArchiveMode m_mode;

        /// <summary>
        /// Gets the number of bytes expected to be written after the stream is disposed
        /// </summary>
        private long m_flushBufferSize = 0;

        /// <summary>
        /// The LZipArchive instance used when reading archives
        /// </summary>
        private IArchive m_archive;
        /// <summary>
        /// The stream used to either read or write
        /// </summary>
        private Stream m_stream;

        /// <summary>
        /// Lookup table for faster access to entries based on their name.
        /// </summary>
        private Dictionary<string, IEntry> m_entryDict;

        /// <summary>
        /// The writer instance used when creating archives
        /// </summary>
        private IWriter m_writer;

        /// <summary>
        /// A flag indicating if we are using the fail-over reader interface
        /// </summary>
        public bool m_using_reader = false;

        /// <summary>
        /// Default constructor, used to read file extension and supported commands
        /// </summary>
        public FileArchiveLZip() { }

        private IArchive Archive {
            get {
                if (m_archive == null)
                {
                    m_stream.Position = 0;
                    m_archive = ArchiveFactory.Open(m_stream);
                }
                return m_archive;
            }
        }

        public void SwitchToReader()
        {
            if (!m_using_reader)
            {
                // Close what we have
                using (m_stream)
                using (m_archive)
                { }

                m_using_reader = true;
            }
        }

        public Stream GetStreamFromReader(IEntry entry)
        {
            SharpCompress.Readers.Tar.TarReader rd = null;

            try
            {
                rd = SharpCompress.Readers.Tar.TarReader.Open(m_stream);

                while (rd.MoveToNextEntry())
                    if (entry.Key == rd.Entry.Key)
                        return new StreamWrapper(rd.OpenEntryStream(), stream => {
                            rd.Dispose();
                        });

                throw new Exception(string.Format("Stream not found: {0}", entry.Key));
            }
            catch
            {
                if (rd != null)
                    rd.Dispose();

                throw;
            }

        }

        /// <summary>
        /// Constructs a new LZip instance.
        /// Access mode is specified by mode parameter.
        /// Note that stream would not be disposed by FileArchiveLZip instance so
        /// you may reuse it and have to dispose it yourself.
        /// </summary>
        /// <param name="stream">The stream to read or write depending access mode</param>
        /// <param name="mode">The archive acces mode</param>
        /// <param name="options">The options passed on the commandline</param>
        public FileArchiveLZip(Stream stream, ArchiveMode mode, IDictionary<string, string> options)
        {
            m_stream = stream;
            m_mode = mode;
            if (mode == ArchiveMode.Write)
            {
                //var compression = new ZipWriterOptions(CompressionType.Deflate);

                //compression.CompressionType = DEFAULT_COMPRESSION_METHOD;
                //compression.DeflateCompressionLevel = DEFAULT_COMPRESSION_LEVEL;

                //m_usingZip64 = compression.UseZip64 =
                //    options.ContainsKey(COMPRESSION_ZIP64_OPTION)
                //    ? Duplicati.Library.Utility.Utility.ParseBoolOption(options, COMPRESSION_ZIP64_OPTION)
                //    : DEFAULT_ZIP64;

                //string cpmethod;
                //CompressionType tmptype;
                //if (options.TryGetValue(COMPRESSION_METHOD_OPTION, out cpmethod) && Enum.TryParse<SharpCompress.Common.CompressionType>(cpmethod, true, out tmptype))
                //    compression.CompressionType = tmptype;

                //string cplvl;
                //int tmplvl;
                //if (options.TryGetValue(COMPRESSION_LEVEL_OPTION, out cplvl) && int.TryParse(cplvl, out tmplvl))
                //    compression.DeflateCompressionLevel = (SharpCompress.Compressors.Deflate.CompressionLevel)Math.Max(Math.Min(9, tmplvl), 0);
                //else if (options.TryGetValue(COMPRESSION_LEVEL_OPTION_ALIAS, out cplvl) && int.TryParse(cplvl, out tmplvl))
                //    compression.DeflateCompressionLevel = (SharpCompress.Compressors.Deflate.CompressionLevel)Math.Max(Math.Min(9, tmplvl), 0);

                //m_defaultCompressionLevel = compression.DeflateCompressionLevel;
                //m_compressionType = compression.CompressionType;

                //m_writer = WriterFactory.Open(m_stream, ArchiveType.Tar, compression);
                m_writer = new TarWriter(m_stream, new WriterOptions(CompressionType.LZMA));

                // TODO update the size
                //Size of endheader, taken from SharpCompress ZipWriter
                m_flushBufferSize = 8 + 2 + 2 + 4 + 4 + 2 + 0;
            }
        }

        #region IFileArchive Members
        /// <summary>
        /// Gets the filename extension used by the compression module
        /// </summary>
        public string FilenameExtension { get { return "tlz"; } }
        /// <summary>
        /// Gets a friendly name for the compression module
        /// </summary>
        public string DisplayName { get { return Strings.FileArchiveLZip.DisplayName; } }
        /// <summary>
        /// Gets a description of the compression module
        /// </summary>
        public string Description { get { return Strings.FileArchiveLZip.Description; } }

        /// <summary>
        /// Gets a list of commands supported by the compression module
        /// </summary>
        public IList<ICommandLineArgument> SupportedCommands {
            get {
                return new List<ICommandLineArgument>();
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
                q = q.Where(x =>
                            x.Key.StartsWith(prefix, Utility.Utility.ClientFilenameStringComparison)
                            ||
                            x.Key.Replace('\\', '/').StartsWith(prefix, Utility.Utility.ClientFilenameStringComparison)
                           );

            return q.Select(x => new KeyValuePair<string, long>(x.Key, x.Size)).ToArray();
        }

        /// <summary>
        /// Opens an file for reading
        /// </summary>
        /// <param name="file">The name of the file to open</param>
        /// <returns>A stream with the file contents</returns>
        public Stream OpenRead(string file)
        {
            if (m_mode != ArchiveMode.Read)
                throw new InvalidOperationException(CannotReadWhileWriting);

            var ze = GetEntry(file);
            if (ze == null)
                return null;

            if (ze is IArchiveEntry)
                return ((IArchiveEntry)ze).OpenEntryStream();
            else if (ze is SharpCompress.Common.Zip.ZipEntry)
                return GetStreamFromReader(ze);

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
                foreach (var en in Archive.Entries)
                    d[en.Key] = en;
                m_entryDict = d;
            }
        }

        /// <summary>
        /// Internal function that returns a ZipEntry for a filename, or null if no such file exists
        /// </summary>
        /// <param name="file">The name of the file to find</param>
        /// <returns>The ZipEntry for the file or null if no such file was found</returns>
        private IEntry GetEntry(string file)
        {
            if (m_mode != ArchiveMode.Read)
                throw new InvalidOperationException(CannotReadWhileWriting);

            LoadEntryTable();

            IEntry e;
            if (m_entryDict.TryGetValue(file, out e))
                return e;
            if (m_entryDict.TryGetValue(file.Replace('/', '\\'), out e))
                return e;

            return null;
        }

        /// <summary>
        /// Creates a file in the archive and returns a writeable stream
        /// </summary>
        /// <param name="file">The name of the file to create</param>
        /// <param name="hint">A hint to the compressor as to how compressible the file data is</param>
        /// <param name="lastWrite">The time the file was last written</param>
        /// <returns>A writeable stream for the file contents</returns>
        public virtual Stream CreateFile(string file, CompressionHint hint, DateTime lastWrite)
        {
            if (m_mode != ArchiveMode.Write)
                throw new InvalidOperationException(CannotWriteWhileReading);

            // TODO: Calculate size?
            m_flushBufferSize += CENTRAL_HEADER_ENTRY_SIZE + System.Text.Encoding.UTF8.GetByteCount(file);

            // TODO get the tar writer into a stream?
            return ((TarWriter)m_writer).WriteToStream(file, null);
        }

        /// <summary>
        /// Returns a value that indicates if the file exists
        /// </summary>
        /// <param name="file">The name of the file to test existence for</param>
        /// <returns>True if the file exists, false otherwise</returns>
        public bool FileExists(string file)
        {
            if (m_mode != ArchiveMode.Read)
                throw new InvalidOperationException(CannotReadWhileWriting);

            return GetEntry(file) != null;
        }

        /// <summary>
        /// Gets the current size of the archive
        /// </summary>
        public long Size {
            get {
                return m_mode == ArchiveMode.Write ? m_stream.Length : Archive.TotalSize;
            }
        }

        /// <summary>
        /// The size of the current unflushed buffer
        /// </summary>
        public long FlushBufferSize {
            get {
                return m_flushBufferSize;
            }
        }


        /// <summary>
        /// Gets the last write time for a file
        /// </summary>
        /// <param name="file">The name of the file to query</param>
        /// <returns>The last write time for the file</returns>
        public DateTime GetLastWriteTime(string file)
        {
            IEntry entry = GetEntry(file);
            if (entry != null)
            {
                if (entry.LastModifiedTime.HasValue)
                    return entry.LastModifiedTime.Value;
                else
                    return DateTime.MinValue;
            }

            throw new FileNotFoundException(Strings.FileArchiveLZip.FileNotFoundError(file));
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            if (m_archive != null)
                m_archive.Dispose();
            m_archive = null;

            if (m_writer != null)
                m_writer.Dispose();
            m_writer = null;

            m_stream = null;
        }

        #endregion


        private class TarWriterStream : Stream
        {
            private TarWriter m_writer;

            public TarWriterStream(TarWriter tarWriter)
            {
                m_writer = tarWriter;
            }

            public override bool CanRead => false;

            public override bool CanSeek => false;

            public override bool CanWrite => true;

            public override long Length => throw new NotImplementedException();

            public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public override void Flush()
            {
                throw new NotImplementedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }
        }
    }
}
