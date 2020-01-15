using Duplicati.Library.Interface;
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
    class FileArchiveXz : ICompression
    {
        private ArchiveMode m_mode;
        private ManagedXZ.XZCompressStream m_writer;
        private Stream m_stream;

        // TODO
        long ICompression.Size => throw new NotImplementedException();

        string ICompression.FilenameExtension { get { return "txz"; } }

        string ICompression.DisplayName { get { return Strings.FileArchiveXz.DisplayName; } }

        string ICompression.Description { get { return Strings.FileArchiveXz.Description; } }

        IList<ICommandLineArgument> ICompression.SupportedCommands {
            get {

                return new List<ICommandLineArgument>(new ICommandLineArgument[]
                {
                    // TODO?
                });
            }
        }

        // TODO
        long IArchiveWriter.FlushBufferSize => throw new NotImplementedException();

        /// <summary>
        /// Default constructor, used to read file extension and supported commands.
        /// </summary>
        public FileArchiveXz() { }

        public FileArchiveXz(Stream stream, ArchiveMode mode, IDictionary<string, string> options)
        {
            m_mode = mode;
            m_stream = stream;
            if (mode == ArchiveMode.Write)
            {
                m_writer = new ManagedXZ.XZCompressStream(m_stream);
                var m_tarWriter = new TarWriter(m_writer, new WriterOptions(CompressionType.LZip));
            }
            // TODO more args?
        }

        Stream IArchiveWriter.CreateFile(string file, CompressionHint hint, DateTime lastWrite)
        {
            if (string.IsNullOrEmpty(file))
                throw new ArgumentNullException(nameof(file));
            if (m_mode != ArchiveMode.Write)
                throw new InvalidOperationException(Strings.FileArchiveXz.NoWriteError);

            // TODO increment flush buffer size
            // TODO create file in archive
            throw new NotImplementedException();
        }

        void IDisposable.Dispose()
        {
            throw new NotImplementedException();
        }

        bool IArchiveReader.FileExists(string file)
        {
            throw new NotImplementedException();
        }

        DateTime IArchiveReader.GetLastWriteTime(string file)
        {
            throw new NotImplementedException();
        }

        string[] IArchiveReader.ListFiles(string prefix)
        {
            throw new NotImplementedException();
        }

        IEnumerable<KeyValuePair<string, long>> IArchiveReader.ListFilesWithSize(string prefix)
        {
            throw new NotImplementedException();
        }

        Stream IArchiveReader.OpenRead(string file)
        {
            throw new NotImplementedException();
        }
    }
}
