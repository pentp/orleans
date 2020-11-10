//#define TRACE_SERIALIZATION
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Orleans.Runtime;

namespace Orleans.Serialization
{
    /// <summary>
    /// Reader for Orleans binary token streams
    /// </summary>
    internal sealed class BinaryTokenStreamReader2 : IBinaryTokenStreamReader
    {
        private ReadOnlySequence<byte> input;
        private ReadOnlyMemory<byte> currentSpan;
        private SequencePosition nextSequencePosition;
        private int bufferPos;
        private long previousBuffersSize;

        public BinaryTokenStreamReader2()
        {
        }

        public BinaryTokenStreamReader2(ReadOnlySequence<byte> input)
        {
            this.input = input;
            this.currentSpan = input.First;
        }

        public long Length => this.input.Length;
        
        public long Position
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.previousBuffersSize + this.bufferPos;
        }

        public int CurrentPosition
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)this.previousBuffersSize + this.bufferPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PartialReset(ReadOnlySequence<byte> input)
        {
            this.input = input;
            this.nextSequencePosition = default;
            this.currentSpan = input.First;
            this.bufferPos = 0;
            this.previousBuffersSize = 0;
        }

        public void Skip(long count)
        {
            var end = this.Position + count;
            while (this.Position < end)
            {
                if (this.Position + this.currentSpan.Length >= end)
                {
                    this.bufferPos = (int)(end - this.previousBuffersSize);
                }
                else
                {
                    this.MoveNext();
                }
            }
        }

        /// <summary>
        /// Creates a new reader beginning at the specified position.
        /// </summary>
        public BinaryTokenStreamReader2 ForkFrom(long position)
        {
            return new BinaryTokenStreamReader2(this.input.Slice(position));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MoveNext()
        {
            // If this is the first call to MoveNext then nextSequencePosition is invalid and must be moved to the second position.
            if (nextSequencePosition.GetInteger() == 0 && nextSequencePosition.GetObject() is null)
                MoveNextFirst();

            if (!this.input.TryGet(ref this.nextSequencePosition, out var memory))
                ThrowInsufficientData();

            var bufferSize = this.currentSpan.Length;
            this.previousBuffersSize += bufferSize;
            this.currentSpan = memory;
            this.bufferPos = 0;

            void MoveNextFirst()
            {
                var pos = input.Start;
                input.TryGet(ref pos, out _);
                nextSequencePosition = pos;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte()
        {
            var buf = this.currentSpan.Span;
            var pos = this.bufferPos;
            if ((uint)pos < (uint)buf.Length)
            {
                this.bufferPos = pos + 1;
                return buf[pos];
            }

            return ReadSlower();
            byte ReadSlower()
            {
                this.MoveNext();
                this.bufferPos = 1;
                return this.currentSpan.Span[0];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte PeekByte()
        {
            var buf = this.currentSpan.Span;
            var pos = this.bufferPos;
            if ((uint)pos < (uint)buf.Length)
                return buf[pos];

            return PeekSlower();
            byte PeekSlower()
            {
                this.MoveNext();
                return this.currentSpan.Span[0];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte ReadByte(ref ReadOnlySpan<byte> span)
        {
            var pos = this.bufferPos;
            var buf = span;
            if ((uint)pos < (uint)buf.Length)
            {
                this.bufferPos = pos + 1;
                return buf[pos];
            }

            return ReadSlower(out span);
            byte ReadSlower(out ReadOnlySpan<byte> span)
            {
                this.MoveNext();
                this.bufferPos = 1;
                return (span = this.currentSpan.Span)[0];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short ReadInt16() => unchecked((short)ReadUInt16());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort ReadUInt16()
        {
            const int width = 2;
            var buf = this.currentSpan.Span;
            var pos = this.bufferPos;
            if ((ulong)(uint)pos + width <= (uint)buf.Length)
            {
                this.bufferPos = pos + width;
                return BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(pos, width));
            }

            return ReadSlower();
            ushort ReadSlower()
            {
                var span = this.currentSpan.Span;
                uint b1 = ReadByte(ref span);
                uint b2 = ReadByte(ref span);
                return (ushort)(b1 | (b2 << 8));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ushort ReadUInt16(ref ReadOnlySpan<byte> span)
        {
            const int width = 2;
            var pos = this.bufferPos;
            var buf = span;
            if ((ulong)(uint)pos + width <= (uint)buf.Length)
            {
                this.bufferPos = pos + width;
                return BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(pos, width));
            }

            return ReadSlower(ref span);
            ushort ReadSlower(ref ReadOnlySpan<byte> span)
            {
                uint b1 = ReadByte(ref span);
                uint b2 = ReadByte(ref span);
                return (ushort)(b1 | (b2 << 8));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt32() => (int)this.ReadUInt32();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadUInt32()
        {
            const int width = 4;
            var buf = this.currentSpan.Span;
            var pos = this.bufferPos;
            if ((ulong)(uint)pos + width <= (uint)buf.Length)
            {
                this.bufferPos = pos + width;
                return BinaryPrimitives.ReadUInt32LittleEndian(buf.Slice(pos, width));
            }

            return ReadSlower();
            uint ReadSlower()
            {
                var span = this.currentSpan.Span;
                uint p1 = ReadUInt16(ref span);
                uint p2 = ReadUInt16(ref span);
                return p1 | (p2 << 16);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private uint ReadUInt32(ref ReadOnlySpan<byte> span)
        {
            const int width = 4;
            var pos = this.bufferPos;
            var buf = span;
            if ((ulong)(uint)pos + width <= (uint)buf.Length)
            {
                this.bufferPos = pos + width;
                return BinaryPrimitives.ReadUInt32LittleEndian(buf.Slice(pos, width));
            }

            return ReadSlower(ref span);
            uint ReadSlower(ref ReadOnlySpan<byte> span)
            {
                uint p1 = ReadUInt16(ref span);
                uint p2 = ReadUInt16(ref span);
                return p1 | (p2 << 16);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadInt64() => (long)this.ReadUInt64();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ReadUInt64()
        {
            const int width = 8;
            var span = this.currentSpan.Span;
            var pos = this.bufferPos;
            if ((ulong)(uint)pos + width <= (uint)span.Length)
            {
                this.bufferPos = pos + width;
                return BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(pos, width));
            }

            return ReadSlower();
            ulong ReadSlower()
            {
                var span = this.currentSpan.Span;
                ulong p1 = ReadUInt32(ref span);
                ulong p2 = ReadUInt32(ref span);
                return p1 | (p2 << 32);
            }
        }

        private static void ThrowInsufficientData() => throw new InvalidOperationException("Insufficient data present in buffer.");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if NETCOREAPP
        public float ReadFloat() => BitConverter.Int32BitsToSingle(ReadInt32());
#else
        public unsafe float ReadFloat()
        {
            var i = ReadInt32();
            return *(float*)&i;
        }
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double ReadDouble() => BitConverter.Int64BitsToDouble(ReadInt64());

        public decimal ReadDecimal()
        {
            var parts = new[] { this.ReadInt32(), this.ReadInt32(), this.ReadInt32(), this.ReadInt32() };
            return new decimal(parts);
        }

        public byte[] ReadBytes(uint count)
        {
            if (count != 0)
            {
                var bytes = new byte[count];
                this.ReadBytes(bytes);
                return bytes;
            }
            return Array.Empty<byte>();
        }

        public void ReadBytes(Span<byte> destination)
        {
            var span = this.currentSpan.Span;
            var pos = this.bufferPos;
            var dest = destination;
            if ((ulong)(uint)pos + (uint)dest.Length <= (uint)span.Length)
            {
                this.bufferPos = pos + dest.Length;
                span.Slice(pos, dest.Length).CopyTo(dest);
                return;
            }

            CopySlower(in destination);

            void CopySlower(in Span<byte> d)
            {
                var dest = d;
                while (true)
                {
                    var writeSize = Math.Min(dest.Length, this.currentSpan.Length - this.bufferPos);
                    this.currentSpan.Span.Slice(this.bufferPos, writeSize).CopyTo(dest);
                    this.bufferPos += writeSize;
                    dest = dest.Slice(writeSize);

                    if (dest.Length == 0) break;

                    this.MoveNext();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryReadBytes(int length, out ReadOnlySpan<byte> bytes)
        {
            var span = this.currentSpan.Span;
            var pos = this.bufferPos;
            if ((ulong)(uint)pos + (uint)length <= (uint)span.Length)
            {
                this.bufferPos = pos + length;
                bytes = span.Slice(pos, length);
                return true;
            }

            bytes = default;
            return false;
        }

        /// <summary> Read a <c>bool</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public bool ReadBoolean()
        {
            return ReadToken() == SerializationTokenType.True;
        }

        public DateTime ReadDateTime()
        {
            return DateTime.FromBinary(ReadInt64());
        }

        /// <summary> Read an <c>string</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public string ReadString()
        {
            var span = this.currentSpan.Span;
            var n = (int)this.ReadUInt32(ref span);
            if (n <= 0)
            {
                if (n == 0) return string.Empty;

                // a length of -1 indicates that the string is null.
                if (n == -1) return null;
            }

            var pos = this.bufferPos;
            var buf = span;
            if ((ulong)(uint)pos + (uint)n <= (uint)buf.Length)
            {
                this.bufferPos = pos + n;
                buf = buf.Slice(pos, n);
#if NETCOREAPP
                return Encoding.UTF8.GetString(buf);
#else
                return buf.GetUtf8String();
#endif
            }

            return ReadSlower(n);
            string ReadSlower(int n)
            {
                if (n < 0) throw new InvalidDataException("Invalid string length");
                if (n <= 256)
                {
                    Span<byte> bytes = stackalloc byte[n];
                    this.ReadBytes(bytes);
#if NETCOREAPP
                    return Encoding.UTF8.GetString(bytes);
#else
                    return ((ReadOnlySpan<byte>)bytes).GetUtf8String();
#endif
                }
                else
                {
                    var bytes = this.ReadBytes((uint)n);
                    return Encoding.UTF8.GetString(bytes);
                }
            }
        }

        /// <summary> Read the next bytes from the stream. </summary>
        /// <param name="destination">Output array to store the returned data in.</param>
        /// <param name="offset">Offset into the destination array to write to.</param>
        /// <param name="count">Number of bytes to read.</param>
        public void ReadByteArray(byte[] destination, int offset, int count) => ReadBytes(destination.AsSpan(offset, count));

        /// <summary> Read an <c>char</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public char ReadChar()
        {
            return (char)ReadUInt16();
        }
        
        /// <summary> Read an <c>sbyte</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public sbyte ReadSByte()
        {
            return unchecked((sbyte)ReadByte());
        }

        /// <summary> Read an <c>IPAddress</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public IPAddress ReadIPAddress()
        {
            Span<byte> buff = stackalloc byte[16];
            ReadBytes(buff);

            if (MemoryMarshal.Read<long>(buff) == 0 && MemoryMarshal.Read<int>(buff.Slice(8)) == 0)
            {
                var addr = BinaryPrimitives.ReadUInt32LittleEndian(buff.Slice(12));
                return new IPAddress(addr);
            }

#if NETCOREAPP
            return new IPAddress(buff);
#else
            return new IPAddress(buff.ToArray());
#endif
        }

        public unsafe Guid ReadGuid()
        {
#if NETCOREAPP
            Guid guid;
            var bytes = new Span<byte>(&guid, sizeof(Guid));
            this.ReadBytes(bytes);
            return BitConverter.IsLittleEndian ? guid : new Guid(bytes);
#else
            byte[] bytes = ReadBytes(16);
            return new Guid(bytes);
#endif
        }

        /// <summary> Read an <c>IPEndPoint</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public IPEndPoint ReadIPEndPoint()
        {
            var addr = ReadIPAddress();
            var port = ReadInt32();
            return new IPEndPoint(addr, port);
        }

        /// <summary> Read an <c>SiloAddress</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        public SiloAddress ReadSiloAddress()
        {
            var ep = ReadIPEndPoint();
            var gen = ReadInt32();
            return SiloAddress.New(ep, gen);
        }

        public TimeSpan ReadTimeSpan()
        {
            return new TimeSpan(ReadInt64());
        }

        /// <summary>
        /// Read a block of data into the specified output <c>Array</c>.
        /// </summary>
        /// <param name="array">Array to output the data to.</param>
        /// <param name="n">Number of bytes to read.</param>
        public void ReadBlockInto(Array array, int n)
        {
            Buffer.BlockCopy(this.ReadBytes((uint)n), 0, array, 0, n);
        }

        /// <summary> Read a <c>SerializationTokenType</c> value from the stream. </summary>
        /// <returns>Data from current position in stream, converted to the appropriate output type.</returns>
        internal SerializationTokenType ReadToken()
        {
            return (SerializationTokenType)this.ReadByte();
        }

        public IBinaryTokenStreamReader Copy() => new BinaryTokenStreamReader2(this.input);

        public int ReadInt() => this.ReadInt32();

        public uint ReadUInt() => this.ReadUInt32();

        public short ReadShort() => this.ReadInt16();

        public ushort ReadUShort() => this.ReadUInt16();

        public long ReadLong() => this.ReadInt64();

        public ulong ReadULong() => this.ReadUInt64();

        public byte[] ReadBytes(int count) => this.ReadBytes((uint)count);
    }
}

public static class ext
{
    internal static unsafe string GetUtf8String(this ReadOnlySpan<byte> span)
    {
        fixed (byte* bytes = span) return Encoding.UTF8.GetString(bytes, span.Length);
    }
}