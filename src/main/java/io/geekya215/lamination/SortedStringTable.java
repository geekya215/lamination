package io.geekya215.lamination;

import io.geekya215.lamination.exception.Crc32MismatchException;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

import static io.geekya215.lamination.Constants.*;

//
// +-------------------------------+-------------------------------+-------------------------+
// |         Block Section         |          Meta Section         |          Extra          |
// +------------+-----+------------+-------------------------------+-------------------------+
// | data block | ... | data block |            metadata           | meta block offset (u32) |
// +------------+-----+------------+-------------------------------+-------------------------+
//
public final class SortedStringTable implements Closeable {
    // Fixme
    // should close file resource
    private final @NotNull FileObject file;
    private final @NotNull List<MetaBlock> metaBlocks;
    private final @NotNull Cache<Long, Block> blockCache;
    private final @NotNull BloomFilter bloomFilter;
    private final byte @NotNull [] firstKey;
    private final byte @NotNull [] lastKey;
    private final int id;
    private final int metaBlockOffset;
    // Todo
    // add filter and block cache

    public SortedStringTable(
            @NotNull FileObject file,
            @NotNull List<MetaBlock> metaBlocks,
            @NotNull Cache<Long, Block> blockCache,
            @NotNull BloomFilter bloomFilter,
            byte @NotNull [] firstKey,
            byte @NotNull [] lastKey,
            int id,
            int metaBlockOffset) {
        this.file = file;
        this.metaBlocks = metaBlocks;
        this.blockCache = blockCache;
        this.bloomFilter = bloomFilter;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.id = id;
        this.metaBlockOffset = metaBlockOffset;
    }

    public static @NotNull SortedStringTable open(int id, @NotNull Cache<Long, Block> blockCache, @NotNull FileObject file) throws IOException {
        int size = (int) file.size;

        int bloomFilterOffset = file.readInt(size - SIZE_OF_U32);

        int bloomFilterBufLength = size - SIZE_OF_U32 - bloomFilterOffset;
        final byte[] bloomFilterBuf = file.read(bloomFilterOffset, bloomFilterBufLength);
        BloomFilter bloomFilter = BloomFilter.decode(bloomFilterBuf);

        int metaBlockOffset = file.readInt(bloomFilterOffset - SIZE_OF_U32);

        int metaBlockBufLength = bloomFilterOffset - SIZE_OF_U32 - metaBlockOffset;
        final byte[] metaBlocksBuf = file.read(metaBlockOffset, metaBlockBufLength);
        List<MetaBlock> metaBlocks = MetaBlock.decode(metaBlocksBuf);

        final byte[] firstKey = metaBlocks.getFirst().firstKey();
        final byte[] lastKey = metaBlocks.getLast().lastKey();

        return new SortedStringTable(file, metaBlocks, blockCache, bloomFilter, firstKey, lastKey, id, metaBlockOffset);
    }

    public @NotNull Block readBlockCache(int blockIndex) throws IOException {
        long key = (id & 0xFFFFFFFFL << 32) | blockIndex;
        Block cachedBlock = blockCache.get(key);
        if (cachedBlock == null) {
            Block block = readBlock(blockIndex);
            blockCache.put(key, block);
            return block;
        }
        return cachedBlock;
    }

    public @NotNull Block readBlock(int blockIndex) throws IOException {
        // Todo
        // check index?
        int offset = metaBlocks.get(blockIndex).offset;
        int offsetEnd = (blockIndex + 1 >= metaBlocks.size()) ? metaBlockOffset : metaBlocks.get(blockIndex + 1).offset;
        int blockLength = offsetEnd - SIZE_OF_U32 - offset;

        final byte[] buf = file.read(offset, blockLength);

        int actualChecksum = file.readInt(offsetEnd - SIZE_OF_U32);

        CRC32 crc32 = new CRC32();
        crc32.update(buf);
        int expectedChecksum = (int) crc32.getValue();

        if (actualChecksum != expectedChecksum) {
            throw new Crc32MismatchException(expectedChecksum, actualChecksum);
        }

        return Block.decode(buf);
    }

    public int findBlockIndex(byte @NotNull [] key) {
        int index = 0;
        for (; index < metaBlocks.size(); index++) {
            MetaBlock metaBlock = metaBlocks.get(index);
            if (Arrays.compare(metaBlock.firstKey, key) > 0) {
                break;
            }
        }
        return index == 0 ? 0 : index - 1;
    }

    public @NotNull FileObject getFile() {
        return file;
    }

    public @NotNull List<MetaBlock> getMetaBlocks() {
        return metaBlocks;
    }

    public @NotNull BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public byte @NotNull [] getFirstKey() {
        return firstKey;
    }

    public byte @NotNull [] getLastKey() {
        return lastKey;
    }

    public int getId() {
        return id;
    }

    public long size() {
        return file.size;
    }

    public int numberOfBlock() {
        return metaBlocks.size();
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

    public static final class SortedStringTableBuilder {
        private @NotNull Block.BlockBuilder blockBuilder;
        private final @NotNull List<Byte> dataBlockBytes;
        private final @NotNull List<Long> keysHash;
        private final @NotNull List<MetaBlock> metaBlocks;
        private byte @NotNull [] firstKey;
        private byte @NotNull [] lastKey;
        private final int blockSize;

        public SortedStringTableBuilder(int blockSize) {
            this.blockBuilder = new Block.BlockBuilder(blockSize);
            this.dataBlockBytes = new ArrayList<>();
            this.keysHash = new ArrayList<>();
            this.metaBlocks = new ArrayList<>();
            this.firstKey = EMPTY_BYTE_ARRAY;
            this.lastKey = EMPTY_BYTE_ARRAY;
            this.blockSize = blockSize;
        }

        public void put(byte @NotNull [] key, byte @NotNull [] value) {
            if (firstKey.length == 0) {
                firstKey = key;
            }

            keysHash.add(MurmurHash2.hash64(key, key.length));

            if (blockBuilder.put(key, value)) {
                lastKey = key;
                return;
            }

            generateBlock();

            blockBuilder.put(key, value);
            firstKey = key;
            lastKey = key;
        }

        public void generateBlock() {
            final byte[] buf = blockBuilder.build().encode();
            // Todo
            // invoke new block builder or add clear method block builder
            blockBuilder = new Block.BlockBuilder(blockSize);

            metaBlocks.add(new MetaBlock(dataBlockBytes.size(), firstKey, lastKey));

            CRC32 crc32 = new CRC32();
            crc32.update(buf);

            int checksum = (int) crc32.getValue();
            for (byte b : buf) {
                dataBlockBytes.add(b);
            }

            dataBlockBytes.add((byte) (checksum >> 24));
            dataBlockBytes.add((byte) (checksum >> 16));
            dataBlockBytes.add((byte) (checksum >> 8));
            dataBlockBytes.add((byte) checksum);
        }

        public @NotNull SortedStringTable build(int id, @NotNull Cache<Long, Block> blockCache, @NotNull Path path) throws IOException {
            // flush remaining data to block
            generateBlock();

            final byte[] metaBlockBuf = MetaBlock.encode(metaBlocks);
            int metaBlockBufLength = metaBlockBuf.length;

            int metaBlockOffset = dataBlockBytes.size();

            BloomFilter bloomFilter = new BloomFilter(keysHash.size());
            for (Long hash : keysHash) {
                bloomFilter.mappingHashToBitset(hash);
            }

            byte[] bloomFilterBuf = bloomFilter.encode();
            int bloomFilterBufLength = bloomFilterBuf.length;

            final byte[] buf = new byte[metaBlockOffset + metaBlockBufLength + SIZE_OF_U32 + bloomFilterBufLength + SIZE_OF_U32];

            for (int i = 0; i < dataBlockBytes.size(); i++) {
                buf[i] = dataBlockBytes.get(i);
            }

            System.arraycopy(metaBlockBuf, 0, buf, metaBlockOffset, metaBlockBufLength);

            int cursor = metaBlockOffset + metaBlockBuf.length;

            buf[cursor] = (byte) (metaBlockOffset >> 24);
            buf[cursor + 1] = (byte) (metaBlockOffset >> 16);
            buf[cursor + 2] = (byte) (metaBlockOffset >> 8);
            buf[cursor + 3] = (byte) metaBlockOffset;
            cursor += 4;

            int bloomFilterOffset = cursor;

            System.arraycopy(bloomFilterBuf, 0, buf, cursor, bloomFilterBufLength);
            cursor += bloomFilterBufLength;

            buf[cursor] = (byte) (bloomFilterOffset >> 24);
            buf[cursor + 1] = (byte) (bloomFilterOffset >> 16);
            buf[cursor + 2] = (byte) (bloomFilterOffset >> 8);
            buf[cursor + 3] = (byte) bloomFilterOffset;

            FileObject file = FileObject.create(path, buf);

            return new SortedStringTable(file, metaBlocks, blockCache, bloomFilter, metaBlocks.getFirst().firstKey(), metaBlocks.getLast().lastKey(), id, metaBlockOffset);
        }
    }

    public final static class SortedStringTableIterator implements StorageIterator {
        private final @NotNull SortedStringTable sst;
        private @NotNull Block.BlockIterator iter;
        private int blockIndex;

        public SortedStringTableIterator(@NotNull SortedStringTable sst, @NotNull Block.BlockIterator iter, int blockIndex) {
            this.sst = sst;
            this.iter = iter;
            this.blockIndex = blockIndex;
        }

        // Todo
        // use helper function to abstract common logic for
        // seek to first and seek to key
        public static @NotNull SortedStringTableIterator createAndSeekToFirst(@NotNull SortedStringTable sst) throws IOException {
            Block.BlockIterator iter = Block.BlockIterator.createAndSeekToFirst(sst.readBlockCache(0));
            return new SortedStringTableIterator(sst, iter, 0);
        }

        public static @NotNull SortedStringTableIterator createAndSeekToKey(@NotNull SortedStringTable sst, byte @NotNull [] key) throws IOException {
            int idx = sst.findBlockIndex(key);
            Block.BlockIterator iter = Block.BlockIterator.createAndSeekToKey(sst.readBlockCache(idx), key);
            if (!iter.isValid()) {
                if (idx + 1 < sst.numberOfBlock()) {
                    idx += 1;
                    iter = Block.BlockIterator.createAndSeekToFirst(sst.readBlockCache(idx));
                }
            }
            return new SortedStringTableIterator(sst, iter, idx);
        }

        public void seekToFirst() throws IOException {
            iter = Block.BlockIterator.createAndSeekToFirst(sst.readBlockCache(0));
            blockIndex = 0;
        }

        public void seekToKey(byte @NotNull [] key) throws IOException {
            int idx = sst.findBlockIndex(key);
            blockIndex = idx;
            Block.BlockIterator seekIter = Block.BlockIterator.createAndSeekToKey(sst.readBlockCache(idx), key);
            if (!seekIter.isValid()) {
                idx += 1;
                if (idx < sst.numberOfBlock()) {
                    blockIndex = idx;
                    iter = Block.BlockIterator.createAndSeekToFirst(sst.readBlockCache(blockIndex));
                }
            } else {
                iter = seekIter;
            }
        }

        @Override
        public byte @NotNull [] key() {
            return iter.key();
        }

        @Override
        public byte @NotNull [] value() {
            return iter.value();
        }

        @Override
        public boolean isValid() {
            return iter.isValid();
        }

        @Override
        public void next() throws IOException {
            iter.next();
            if (!iter.isValid()) {
                blockIndex += 1;
                if (blockIndex < sst.numberOfBlock()) {
                    iter = Block.BlockIterator.createAndSeekToFirst(sst.readBlockCache(blockIndex));
                }
            }
        }
    }

    public static final class FileObject implements Closeable {
        static final String READ_ONLY_MODE = "r";
        private final @NotNull RandomAccessFile file;
        private final long size;

        public FileObject(@NotNull RandomAccessFile file, long size) {
            this.file = file;
            this.size = size;
        }

        public static @NotNull FileObject create(@NotNull Path path, byte @NotNull [] buf) throws IOException {
            // Todo
            // Files.write or BufferedOutputStream which one is more efficient?
            Files.write(path, buf);
            RandomAccessFile raf = new RandomAccessFile(path.toFile(), READ_ONLY_MODE);
            return new FileObject(raf, buf.length);
        }

        public static @NotNull FileObject open(@NotNull Path path) throws IOException {
            RandomAccessFile file = new RandomAccessFile(path.toFile(), READ_ONLY_MODE);
            return new FileObject(file, file.length());
        }

        public byte @NotNull [] read(int offset, int length) throws IOException {
            final byte[] buf = new byte[length];
            file.seek(offset);
            file.readFully(buf);
            return buf;
        }

        public int readInt(int pos) throws IOException {
            // Todo
            // reset file-pointer after read
            file.seek(pos);
            return file.readInt();
        }

        public long getSize() {
            return size;
        }

        @Override
        public void close() throws IOException {
            file.close();
        }
    }

    //
    // +---------------------------------------------------------------------------+-----+
    // |                                  MetaBlock #1                             |     |
    // |             +------------------------------+------------------------------+ ... |
    // |             |          first key           |           last key           |     |
    // +-------------+--------------+---------------+--------------+---------------+-----+
    // | offset (4B) | key_len (2B) | key (key_len) | key_len (2B) | key (key_len) | ... |
    // +-------------+--------------+---------------+--------------+---------------+-----+
    //
    public record MetaBlock(int offset, byte @NotNull [] firstKey, byte @NotNull [] lastKey) {
        public static byte @NotNull [] encode(List<MetaBlock> metaBlocks) {
            int bufLength = 0;
            for (MetaBlock metaBlock : metaBlocks) {
                // offset
                bufLength += SIZE_OF_U32;

                // first key length
                bufLength += SIZE_OF_U16;

                // actual first key
                bufLength += metaBlock.firstKey.length;

                // last key length
                bufLength += SIZE_OF_U16;

                // actual last key
                bufLength += metaBlock.lastKey.length;
            }
            // number of meta block
            bufLength += SIZE_OF_U32;
            // crc32
            bufLength += SIZE_OF_U32;

            final byte[] buf = new byte[bufLength];
            int cursor = 0;

            for (MetaBlock metaBlock : metaBlocks) {
                int offset = metaBlock.offset;
                buf[cursor] = (byte) (offset >> 24);
                buf[cursor + 1] = (byte) (offset >> 16);
                buf[cursor + 2] = (byte) (offset >> 8);
                buf[cursor + 3] = (byte) offset;
                cursor += 4;

                int firstKeyLength = metaBlock.firstKey.length;
                buf[cursor] = (byte) (firstKeyLength >> 8);
                buf[cursor + 1] = (byte) (firstKeyLength);
                cursor += 2;

                System.arraycopy(metaBlock.firstKey, 0, buf, cursor, firstKeyLength);
                cursor += firstKeyLength;

                int lastKeyLength = metaBlock.lastKey.length;
                buf[cursor] = (byte) (lastKeyLength >> 8);
                buf[cursor + 1] = (byte) lastKeyLength;
                cursor += 2;

                System.arraycopy(metaBlock.lastKey, 0, buf, cursor, lastKeyLength);
                cursor += lastKeyLength;
            }

            int numOfMetaBlock = metaBlocks.size();
            buf[cursor] = (byte) (numOfMetaBlock >> 24);
            buf[cursor + 1] = (byte) (numOfMetaBlock >> 16);
            buf[cursor + 2] = (byte) (numOfMetaBlock >> 8);
            buf[cursor + 3] = (byte) numOfMetaBlock;
            cursor += 4;

            CRC32 crc32 = new CRC32();
            crc32.update(buf, 0, cursor);
            int checksum = ((int) crc32.getValue());

            buf[cursor] = (byte) (checksum >> 24);
            buf[cursor + 1] = (byte) (checksum >> 16);
            buf[cursor + 2] = (byte) (checksum >> 8);
            buf[cursor + 3] = (byte) checksum;

            return buf;
        }

        public static @NotNull List<MetaBlock> decode(byte @NotNull [] buf) {
            int cursor = buf.length;

            cursor -= SIZE_OF_U32;
            int actualChecksum = (buf[cursor] & 0xFF) << 24 | (buf[cursor + 1] & 0xFF) << 16 |
                    (buf[cursor + 2] & 0xFF) << 8 | (buf[cursor + 3]) & 0xFF;

            CRC32 crc32 = new CRC32();
            crc32.update(buf, 0, cursor);
            int expectedChecksum = (int) crc32.getValue();

            if (actualChecksum != expectedChecksum) {
                throw new Crc32MismatchException(expectedChecksum, actualChecksum);
            }

            cursor -= SIZE_OF_U32;
            int numOfMetaBlock = (buf[cursor] & 0xFF) << 24 | (buf[cursor + 1] & 0xFF) << 16 |
                    (buf[cursor + 2] & 0xFF) << 8 | (buf[cursor + 3]) & 0xFF;

            final List<MetaBlock> metaBlocks = new ArrayList<>(numOfMetaBlock);

            // seek cursor to position 0
            cursor = 0;
            for (int i = 0; i < numOfMetaBlock; i++) {
                int offset = (buf[cursor] & 0xFF) << 24 | (buf[cursor + 1] & 0xFF) << 16 |
                        (buf[cursor + 2] & 0xFF) << 8 | (buf[cursor + 3]) & 0xFF;
                cursor += 4;

                int firstKeyLength = buf[cursor] & 0xFF << 8 | buf[cursor + 1] & 0xFF;
                cursor += 2;

                final byte[] firstKey = new byte[firstKeyLength];
                System.arraycopy(buf, cursor, firstKey, 0, firstKeyLength);
                cursor += firstKeyLength;

                int lastKeyLength = buf[cursor] & 0xFF << 8 | buf[cursor + 1] & 0xFF;
                cursor += 2;

                final byte[] lastKey = new byte[lastKeyLength];
                System.arraycopy(buf, cursor, lastKey, 0, lastKeyLength);
                cursor += lastKeyLength;

                metaBlocks.add(new MetaBlock(offset, firstKey, lastKey));
            }

            return metaBlocks;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetaBlock metaBlock = (MetaBlock) o;
            return offset == metaBlock.offset && Arrays.equals(firstKey, metaBlock.firstKey) && Arrays.equals(lastKey, metaBlock.lastKey);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(offset);
            result = 31 * result + Arrays.hashCode(firstKey);
            result = 31 * result + Arrays.hashCode(lastKey);
            return result;
        }
    }
}
