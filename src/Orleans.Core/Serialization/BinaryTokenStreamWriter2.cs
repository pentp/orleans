using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Orleans.CodeGeneration;
using Orleans.Runtime;

namespace Orleans.Serialization
{
    /// <summary>
    /// Writer for Orleans binary token streams
    /// </summary>
    internal sealed class BinaryTokenStreamWriter2<TBufferWriter> : IBinaryTokenStreamWriter where TBufferWriter : IBufferWriter<byte>
    {
        private static readonly Encoding Utf8Encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: false);

        private readonly Encoder utf8Encoder = Utf8Encoding.GetEncoder();
        private TBufferWriter output;
        private Memory<byte> currentBuffer;
        private int currentOffset;
        private int completedLength;

        private static readonly Dictionary<Type, SerializationTokenType> typeTokens = new Dictionary<Type, SerializationTokenType>
        {
            [typeof(bool)] = SerializationTokenType.Boolean,
            [typeof(int)] = SerializationTokenType.Int,
            [typeof(uint)] = SerializationTokenType.Uint,
            [typeof(short)] = SerializationTokenType.Short,
            [typeof(ushort)] = SerializationTokenType.Ushort,
            [typeof(long)] = SerializationTokenType.Long,
            [typeof(ulong)] = SerializationTokenType.Ulong,
            [typeof(byte)] = SerializationTokenType.Byte,
            [typeof(sbyte)] = SerializationTokenType.Sbyte,
            [typeof(float)] = SerializationTokenType.Float,
            [typeof(double)] = SerializationTokenType.Double,
            [typeof(decimal)] = SerializationTokenType.Decimal,
            [typeof(string)] = SerializationTokenType.String,
            [typeof(char)] = SerializationTokenType.Character,
            [typeof(Guid)] = SerializationTokenType.Guid,
            [typeof(DateTime)] = SerializationTokenType.Date,
            [typeof(TimeSpan)] = SerializationTokenType.TimeSpan,
            [typeof(GrainId)] = SerializationTokenType.GrainId,
            [typeof(ActivationId)] = SerializationTokenType.ActivationId,
            [typeof(SiloAddress)] = SerializationTokenType.SiloAddress,
            [typeof(ActivationAddress)] = SerializationTokenType.ActivationAddress,
            [typeof(IPAddress)] = SerializationTokenType.IpAddress,
            [typeof(IPEndPoint)] = SerializationTokenType.IpEndPoint,
            [typeof(CorrelationId)] = SerializationTokenType.CorrelationId,
            [typeof(InvokeMethodRequest)] = SerializationTokenType.Request,
            [typeof(Response)] = SerializationTokenType.Response,
            [typeof(Dictionary<string, object>)] = SerializationTokenType.StringObjDict,
            [typeof(Object)] = SerializationTokenType.Object,
            [typeof(List<>)] = SerializationTokenType.List,
            [typeof(SortedList<,>)] = SerializationTokenType.SortedList,
            [typeof(Dictionary<,>)] = SerializationTokenType.Dictionary,
            [typeof(HashSet<>)] = SerializationTokenType.Set,
            [typeof(SortedSet<>)] = SerializationTokenType.SortedSet,
            [typeof(KeyValuePair<,>)] = SerializationTokenType.KeyValuePair,
            [typeof(LinkedList<>)] = SerializationTokenType.LinkedList,
            [typeof(Stack<>)] = SerializationTokenType.Stack,
            [typeof(Queue<>)] = SerializationTokenType.Queue,
            [typeof(Tuple<>)] = SerializationTokenType.Tuple + 1,
            [typeof(Tuple<,>)] = SerializationTokenType.Tuple + 2,
            [typeof(Tuple<,,>)] = SerializationTokenType.Tuple + 3,
            [typeof(Tuple<,,,>)] = SerializationTokenType.Tuple + 4,
            [typeof(Tuple<,,,,>)] = SerializationTokenType.Tuple + 5,
            [typeof(Tuple<,,,,,>)] = SerializationTokenType.Tuple + 6,
            [typeof(Tuple<,,,,,,>)] = SerializationTokenType.Tuple + 7
        };

        private static readonly Dictionary<Type, Action<BinaryTokenStreamWriter2<TBufferWriter>, object>> writers = new Dictionary<Type, Action<BinaryTokenStreamWriter2<TBufferWriter>, object>>
        {
            [typeof(bool)] = (stream, obj) => stream.Write((bool)obj),
            [typeof(int)] = (stream, obj) => { stream.Write(SerializationTokenType.Int); stream.Write((int)obj); },
            [typeof(uint)] = (stream, obj) => { stream.Write(SerializationTokenType.Uint); stream.Write((uint)obj); },
            [typeof(short)] = (stream, obj) => { stream.Write(SerializationTokenType.Short); stream.Write((short)obj); },
            [typeof(ushort)] = (stream, obj) => { stream.Write(SerializationTokenType.Ushort); stream.Write((ushort)obj); },
            [typeof(long)] = (stream, obj) => { stream.Write(SerializationTokenType.Long); stream.Write((long)obj); },
            [typeof(ulong)] = (stream, obj) => { stream.Write(SerializationTokenType.Ulong); stream.Write((ulong)obj); },
            [typeof(byte)] = (stream, obj) => { stream.Write(SerializationTokenType.Byte); stream.Write((byte)obj); },
            [typeof(sbyte)] = (stream, obj) => { stream.Write(SerializationTokenType.Sbyte); stream.Write((sbyte)obj); },
            [typeof(float)] = (stream, obj) => { stream.Write(SerializationTokenType.Float); stream.Write((float)obj); },
            [typeof(double)] = (stream, obj) => { stream.Write(SerializationTokenType.Double); stream.Write((double)obj); },
            [typeof(decimal)] = (stream, obj) => { stream.Write(SerializationTokenType.Decimal); stream.Write((decimal)obj); },
            [typeof(string)] = (stream, obj) => { stream.Write(SerializationTokenType.String); stream.Write((string)obj); },
            [typeof(char)] = (stream, obj) => { stream.Write(SerializationTokenType.Character); stream.Write((char)obj); },
            [typeof(Guid)] = (stream, obj) => { stream.Write(SerializationTokenType.Guid); stream.Write((Guid)obj); },
            [typeof(DateTime)] = (stream, obj) => { stream.Write(SerializationTokenType.Date); stream.Write((DateTime)obj); },
            [typeof(TimeSpan)] = (stream, obj) => { stream.Write(SerializationTokenType.TimeSpan); stream.Write((TimeSpan)obj); },
            [typeof(GrainId)] = (stream, obj) => { stream.Write(SerializationTokenType.GrainId); stream.Write((GrainId)obj); },
            [typeof(ActivationId)] = (stream, obj) => { stream.Write(SerializationTokenType.ActivationId); stream.Write((ActivationId)obj); },
            [typeof(SiloAddress)] = (stream, obj) => { stream.Write(SerializationTokenType.SiloAddress); stream.Write((SiloAddress)obj); },
            [typeof(ActivationAddress)] = (stream, obj) => { stream.Write(SerializationTokenType.ActivationAddress); stream.Write((ActivationAddress)obj); },
            [typeof(IPAddress)] = (stream, obj) => { stream.Write(SerializationTokenType.IpAddress); stream.Write((IPAddress)obj); },
            [typeof(IPEndPoint)] = (stream, obj) => { stream.Write(SerializationTokenType.IpEndPoint); stream.Write((IPEndPoint)obj); },
            [typeof(CorrelationId)] = (stream, obj) => { stream.Write(SerializationTokenType.CorrelationId); stream.Write((CorrelationId)obj); }
        };

        public BinaryTokenStreamWriter2(TBufferWriter output)
        {
            this.PartialReset(output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PartialReset(TBufferWriter output)
        {
            this.output = output;
            this.currentBuffer = output.GetMemory();
            this.currentOffset = default;
            this.completedLength = default;
        }

        /// <summary> Current write position in the stream. </summary>
        public int CurrentOffset { get { return this.Length; } }

        /// <summary>
        /// Commit the currently written buffers.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Commit()
        {
            this.output.Advance(this.currentOffset);
            this.completedLength += this.currentOffset;
            this.currentBuffer = default;
            this.currentOffset = default;
        }

        public void Write(decimal d)
        {
            this.Write(Decimal.GetBits(d));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(string s)
        {
            if (s is null)
            {
                this.Write(-1);
            }
            else
            {
#if NETCOREAPP
                var enc = this.utf8Encoder;
                enc.Reset();

                // Attempt a fast write. Note that we could accurately determine the required bytes to encode the input here,
                // but that requires an additional scan over the string. We could determine an upper bound here, too, but that may
                // be wasteful. Instead, we will fall back to a slower and more accurate method if it is not.
                var writableSpan = this.TryGetContiguous(256);
                enc.Convert(s, writableSpan.Slice(4), true, out var charsUsed, out var bytesUsed, out var completed);

                if (completed)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(writableSpan, bytesUsed);
                    this.currentOffset += 4 + bytesUsed;
                    return;
                }

                // Otherwise, try again more slowly, overwriting whatever data was just copied to the output buffer.
                this.WriteStringInternalSlower(s);
#else
                var bytes = Encoding.UTF8.GetBytes(s);
                this.Write(bytes.Length);
                this.Write(bytes);
#endif
            }
        }

#if NETCOREAPP
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void WriteStringInternalSlower(string s)
        {
            var count = Encoding.UTF8.GetByteCount(s);
            this.Write(count);
            var span = this.TryGetContiguous(count);
            if (span.Length > count)
            {
                Encoding.UTF8.GetBytes(s, span);
                this.currentOffset += count;
            }
            else
            {
                var bytes = Encoding.UTF8.GetBytes(s);
                this.WriteMultiSegment(bytes);
            }
        }
#endif

        public void Write(char c)
        {
            this.Write((ushort)c);
        }
        
        public void Write(bool b)
        {
            this.Write((byte)(b ? SerializationTokenType.True : SerializationTokenType.False));
        }
        
        public void WriteNull()
        {
            this.Write((byte)SerializationTokenType.Null);
        }

        public void WriteTypeHeader(Type t, Type expected = null)
        {
            if (t == expected)
            {
                this.Write((byte)SerializationTokenType.ExpectedType);
                return;
            }

            this.Write((byte)SerializationTokenType.SpecifiedType);

            if (t.IsArray)
            {
                this.Write((byte)(SerializationTokenType.Array + (byte)t.GetArrayRank()));
                this.WriteTypeHeader(t.GetElementType());
                return;
            }

            SerializationTokenType token;
            if (typeTokens.TryGetValue(t, out token))
            {
                this.Write((byte)token);
                return;
            }

            if (t.IsGenericType)
            {
                if (typeTokens.TryGetValue(t.GetGenericTypeDefinition(), out token))
                {
                    this.Write((byte)token);
                    foreach (var tp in t.GetGenericArguments())
                    {
                        this.WriteTypeHeader(tp);
                    }
                    return;
                }
            }

            this.Write((byte)SerializationTokenType.NamedType);
            var typeKey = t.OrleansTypeKey();
            this.Write(typeKey.Length);
            this.Write(typeKey);
        }
                
        public void Write(byte[] b, int offset, int count)
        {
            this.Write(b.AsSpan(offset, count));
        }

        public void Write(IPEndPoint ep)
        {
            this.Write(ep.Address);
            this.Write(ep.Port);
        }
        
        public void Write(IPAddress ip)
        {
#if NETCOREAPP
            Span<byte> buf = stackalloc byte[16];
            if (ip.AddressFamily == AddressFamily.InterNetwork)
            {
                buf.Clear();
                ip.TryWriteBytes(buf.Slice(12), out _); // IPv4 -- 4 bytes
            }
            else
            {
                ip.TryWriteBytes(buf, out _); // IPv6 -- 16 bytes
            }
            this.Write(buf);
#else
            if (ip.AddressFamily == AddressFamily.InterNetwork)
            {
                this.Write(0L);
                this.Write(0);
                this.Write(ip.GetAddressBytes()); // IPv4 -- 4 bytes
            }
            else
            {
                this.Write(ip.GetAddressBytes()); // IPv6 -- 16 bytes
            }
#endif
        }

        public void Write(SiloAddress addr)
        {
            this.Write(addr.Endpoint);
            this.Write(addr.Generation);
        }
        
        public void Write(TimeSpan ts)
        {
            this.Write(ts.Ticks);
        }

        public void Write(DateTime dt)
        {
            this.Write(dt.ToBinary());
        }

        public unsafe void Write(Guid id)
        {
#if NETCOREAPP
            if (BitConverter.IsLittleEndian)
            {
                this.Write(new ReadOnlySpan<byte>(&id, sizeof(Guid)));
                return;
            }
#endif
            this.Write(id.ToByteArray());
        }

        /// <summary>
        /// Try to write a simple type (non-array) value to the stream.
        /// </summary>
        /// <param name="obj">Input object to be written to the output stream.</param>
        /// <returns>Returns <c>true</c> if the value was successfully written to the output stream.</returns>
        public bool TryWriteSimpleObject(object obj)
        {
            if (obj == null)
            {
                this.WriteNull();
                return true;
            }
            Action<BinaryTokenStreamWriter2<TBufferWriter>, object> writer;
            if (writers.TryGetValue(obj.GetType(), out writer))
            {
                writer(this, obj);
                return true;
            }
            return false;
        }

        public int Length => this.currentOffset + this.completedLength;

        private Span<byte> WritableSpan => this.currentBuffer.Slice(this.currentOffset).Span;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnsureContiguous(int length)
        {
            // The current buffer is adequate.
            if (this.currentOffset + length < this.currentBuffer.Length) return;

            // The current buffer is inadequate, allocate another.
            this.Allocate(length);
#if DEBUG
            // Throw if the allocation does not satisfy the request.
            if (this.currentBuffer.Length < length) ThrowTooLarge(length);

            void ThrowTooLarge(int l) => throw new InvalidOperationException($"Requested buffer length {l} cannot be satisfied by the writer.");
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> TryGetContiguous(int length)
        {
            // The current buffer is adequate.
            if (this.currentOffset + length > this.currentBuffer.Length)
            {
                // The current buffer is inadequate, allocate another.
                this.Allocate(length);
            }

            return this.WritableSpan;
        }

        public void Allocate(int length)
        {
            // Commit the bytes which have been written.
            this.output.Advance(this.currentOffset);

            // Request a new buffer with at least the requested number of available bytes.
            this.currentBuffer = this.output.GetMemory(length);

            // Update internal state for the new buffer.
            this.completedLength += this.currentOffset;
            this.currentOffset = 0;
        }

        public void Write(byte[] array)
        {
            // Fast path, try copying to the current buffer.
            if (array.Length <= this.currentBuffer.Length - this.currentOffset)
            {
                array.CopyTo(this.WritableSpan);
                this.currentOffset += array.Length;
            }
            else
            {
                var value = new ReadOnlySpan<byte>(array);
                this.WriteMultiSegment(in value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ReadOnlySpan<byte> value)
        {
            // Fast path, try copying to the current buffer.
            if (value.Length <= this.currentBuffer.Length - this.currentOffset)
            {
                value.CopyTo(this.WritableSpan);
                this.currentOffset += value.Length;
            }
            else
            {
                this.WriteMultiSegment(in value);
            }
        }

        private void WriteMultiSegment(in ReadOnlySpan<byte> source)
        {
            var input = source;
            while (true)
            {
                // Write as much as possible/necessary into the current segment.
                var writeSize = Math.Min(this.currentBuffer.Length - this.currentOffset, input.Length);
                input.Slice(0, writeSize).CopyTo(this.WritableSpan);
                this.currentOffset += writeSize;

                input = input.Slice(writeSize);

                if (input.Length == 0) return;

                // The current segment is full but there is more to write.
                this.Allocate(input.Length);
            }
        }

        public void Write(List<ArraySegment<byte>> b)
        {
            foreach (var segment in b)
            {
                this.Write(segment);
            }
        }

        public void Write(short[] array)
        {
            if (BitConverter.IsLittleEndian) Write(MemoryMarshal.AsBytes(array.AsSpan()));
            else foreach (var v in array) Write(v);
        }

        public void Write(int[] array)
        {
            if (BitConverter.IsLittleEndian) Write(MemoryMarshal.AsBytes(array.AsSpan()));
            else foreach (var v in array) Write(v);
        }

        public void Write(long[] array)
        {
            if (BitConverter.IsLittleEndian) Write(MemoryMarshal.AsBytes(array.AsSpan()));
            else foreach (var v in array) Write(v);
        }

        public void Write(ushort[] array)
        {
            if (BitConverter.IsLittleEndian) Write(MemoryMarshal.AsBytes(array.AsSpan()));
            else foreach (var v in array) Write(v);
        }

        public void Write(uint[] array)
        {
            if (BitConverter.IsLittleEndian) Write(MemoryMarshal.AsBytes(array.AsSpan()));
            else foreach (var v in array) Write(v);
        }

        public void Write(ulong[] array)
        {
            if (BitConverter.IsLittleEndian) Write(MemoryMarshal.AsBytes(array.AsSpan()));
            else foreach (var v in array) Write(v);
        }

        public void Write(sbyte[] array) => Write(MemoryMarshal.AsBytes(array.AsSpan()));

        public void Write(char[] array)
        {
            if (BitConverter.IsLittleEndian) Write(MemoryMarshal.AsBytes(array.AsSpan()));
            else foreach (var v in array) Write(v);
        }

        public void Write(bool[] array) => Write(MemoryMarshal.AsBytes(array.AsSpan()));

        public void Write(float[] array)
        {
            if (BitConverter.IsLittleEndian) Write(MemoryMarshal.AsBytes(array.AsSpan()));
            else foreach (var v in array) Write(v);
        }

        public void Write(double[] array)
        {
            if (BitConverter.IsLittleEndian) Write(MemoryMarshal.AsBytes(array.AsSpan()));
            else foreach (var v in array) Write(v);
        }

        public void Write(byte b)
        {
            const int width = sizeof(byte);
            this.EnsureContiguous(width);
            this.WritableSpan[0] = b;
            this.currentOffset += width;
        }

        public void Write(sbyte b)
        {
            const int width = sizeof(sbyte);
            this.EnsureContiguous(width);
            this.WritableSpan[0] = (byte)b;
            this.currentOffset += width;
        }

#if NETCOREAPP
        public void Write(float i) => Write(BitConverter.SingleToInt32Bits(i));
#else
        public unsafe void Write(float i) => Write(*(int*)&i);
#endif

        public void Write(double i) => Write(BitConverter.DoubleToInt64Bits(i));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(short value)
        {
            const int width = sizeof(short);
            this.EnsureContiguous(width);
            BinaryPrimitives.WriteInt16LittleEndian(this.WritableSpan, value);
            this.currentOffset += width;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(int value)
        {
            const int width = sizeof(int);
            this.EnsureContiguous(width);
            BinaryPrimitives.WriteInt32LittleEndian(this.WritableSpan, value);
            this.currentOffset += width;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(long value)
        {
            const int width = sizeof(long);
            this.EnsureContiguous(width);
            BinaryPrimitives.WriteInt64LittleEndian(this.WritableSpan, value);
            this.currentOffset += width;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(uint value)
        {
            const int width = sizeof(uint);
            this.EnsureContiguous(width);
            BinaryPrimitives.WriteUInt32LittleEndian(this.WritableSpan, value);
            this.currentOffset += width;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ushort value)
        {
            const int width = sizeof(ushort);
            this.EnsureContiguous(width);
            BinaryPrimitives.WriteUInt16LittleEndian(this.WritableSpan, value);
            this.currentOffset += width;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ulong value)
        {
            const int width = sizeof(ulong);
            this.EnsureContiguous(width);
            BinaryPrimitives.WriteUInt64LittleEndian(this.WritableSpan, value);
            this.currentOffset += width;
        }
    }
}
