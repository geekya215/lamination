package io.geekya215.lamination;

import io.geekya215.lamination.exception.InvalidFormatException;
import io.geekya215.lamination.util.ByteUtil;
import io.geekya215.lamination.util.Preconditions;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.geekya215.lamination.Constants.*;
import static io.geekya215.lamination.Options.DEFAULT_BLOCK_SIZE;
import static io.geekya215.lamination.util.FileUtil.*;

//
//  ----------------------------------------------------------------------------------
// |                                      SST                                         |
// |----------------------------------------------------------------------------------|
// | magic | data blocks | meta blocks | meta block offset | bloom filter | bf offset |
// |-------+-------------+-------------+-------------------+--------------+-----------|
// |   8B  |     ?B      |     ?B      |        4B         |      ?B      |    4B     |
//  ----------------------------------------------------------------------------------
//
public final class SSTable implements Closeable {
    private final long id;
    private final RandomAccessFile file;
    private final List<MetaBlock> metaBlocks;
    private final int metaBlockOffset;
    private final BloomFilter bloomFilter;
    private final LRUCache<Long, Block> blockCache;

    public SSTable(long id, RandomAccessFile file,
                   List<MetaBlock> metaBlocks,
                   int metaBlockOffset,
                   BloomFilter bloomFilter,
                   LRUCache<Long, Block> blockCache) {
        this.id = id;
        this.file = file;
        this.metaBlocks = metaBlocks;
        this.metaBlockOffset = metaBlockOffset;
        this.bloomFilter = bloomFilter;
        this.blockCache = blockCache;
    }

    public SSTable(long id,
                   File baseDir,
                   List<MetaBlock> metaBlocks,
                   int metaBlockOffset,
                   BloomFilter bloomFilter,
                   LRUCache<Long, Block> blockCache) throws IOException {
        this.id = id;
        this.file = new RandomAccessFile(makeSSTableFile(baseDir, id), "r");
        this.metaBlocks = metaBlocks;
        this.metaBlockOffset = metaBlockOffset;
        this.bloomFilter = bloomFilter;
        this.blockCache = blockCache;
    }

    public List<MetaBlock> getMetaBlocks() {
        return metaBlocks;
    }

    public int getMetaBlockOffset() {
        return metaBlockOffset;
    }

    public int numsOfBlocks() {
        return metaBlocks.size();
    }

    public static SSTable open(long id, File dir, LRUCache<Long, Block> blockCache) throws IOException {
        String sstFileName = String.format("%06d%s", id, SST_FILE_SUFFIX);
        File sstFile = new File(dir, sstFileName);
        RandomAccessFile raf = new RandomAccessFile(sstFile, "r");

        // check magic number
        byte[] magic = new byte[8];
        raf.readFully(magic);
        int cmp = Arrays.compare(MAGIC, magic);
        if (cmp != 0) {
            throw new InvalidFormatException("invalid sst file format");
        }

        long sstFileSize = sstFile.length();

        //                      bloom filter size
        //                         ----------
        //                       /            \
        //   ---------------------------------------------------------
        //  | meta block offset | bloom filter |  bloom filter offset |
        //  |-------------------+--------------+----------------------|
        //  |         4B        |      ?B      |          4B          |
        //   ---------------------------------------------------------
        //                      ^                                     ^
        //              bloom filter offset                       file end
        //

        // decode bloom filter
        raf.seek(sstFileSize - SIZE_OF_U32);
        int bloomFilterOffset = raf.readInt();
        int bloomFilterSize = (int) (sstFileSize - bloomFilterOffset - SIZE_OF_U32);
        byte[] bloomFilterBytes = new byte[bloomFilterSize];
        raf.seek(bloomFilterOffset);
        raf.readFully(bloomFilterBytes);
        BloomFilter bloomFilter = BloomFilter.decode(bloomFilterBytes);

        //                meta blocks size
        //                   -----------
        //                 /             \
        //   -----------------------------------------------------
        //  | data blocks |  meta blocks  |   meta block offset   |
        //  |-------------+---------------+-----------------------|
        //  |      ?B     |       ?B      |          4B           |
        //   -----------------------------------------------------
        //                ^                                       ^
        //         meta block offset                      bloom filter offset
        //

        // decode meta blocks
        raf.seek(bloomFilterOffset - SIZE_OF_U32);
        int metaBlocksOffset = raf.readInt();
        int metaBlocksSize = bloomFilterOffset - metaBlocksOffset - SIZE_OF_U32;
        byte[] metaBlocksBytes = new byte[metaBlocksSize];
        raf.seek(metaBlocksOffset);
        raf.readFully(metaBlocksBytes);

        // Todo
        // we can persist number of meta blocks to reduce list resize
        List<MetaBlock> metaBlocks = MetaBlock.decode(metaBlocksBytes);

        // reset
        raf.seek(0);

        return new SSTable(id, raf, metaBlocks, metaBlocksOffset, bloomFilter, blockCache);
    }

    public Block readBlock(int blockIndex) throws IOException {
        int offset = metaBlocks.get(blockIndex).offset;
        int offsetEnd = (blockIndex + 1) >= metaBlocks.size()
            ? metaBlockOffset
            : metaBlocks.get(blockIndex + 1).offset;

        byte[] bytes = new byte[offsetEnd - offset];
        file.seek(offset);
        file.readFully(bytes);

        return Block.decode(bytes);
    }

    public Block readCachedBlock(int blockIndex) throws IOException {
        long index = ((id & 0xffffffffL) << 32) | blockIndex;
        Block cachedBlock = blockCache.get(index);
        if (cachedBlock == null) {
            Block block = readBlock(blockIndex);
            blockCache.put(index, block);
            return block;
        }
        return cachedBlock;
    }

    public int findBlockIndex(byte[] key) {
        int i = 0;
        for (; i < metaBlocks.size(); ++i) {
            if (Arrays.compare(metaBlocks.get(i).firstKey, key) <= 0) {
                continue;
            }
            break;
        }
        return i == 0 ? 0 : i - 1;
    }

    public boolean containKey(byte[] key) {
        return bloomFilter.contain(key);
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

    //
    //  -----------------------------
    // |        |     first key      |
    // |--------+--------------------|
    // | offset | key_len |    key   |
    // |--------+---------+----------|
    // |   4B   |   2B    | keylen B |
    //  -----------------------------
    //
    public record MetaBlock(int offset, byte[] firstKey) implements Measurable {
        public static byte[] encode(List<MetaBlock> metaBlocks) {
            int bytesSize = 0;
            for (MetaBlock metaBlock : metaBlocks) {
                bytesSize += metaBlock.estimateSize();
            }
            byte[] bytes = new byte[bytesSize];

            int index = 0;
            for (MetaBlock metaBlock : metaBlocks) {
                byte[] firstKey = metaBlock.firstKey;
                int firstKeySize = firstKey.length;

                ByteUtil.writeU32(bytes, index, metaBlock.offset);
                index += 4;
                ByteUtil.writeU32AsU16(bytes, index, firstKeySize);
                index += 2;
                ByteUtil.writeAllBytes(bytes, index, firstKey);
                index += firstKeySize;
            }

            return bytes;
        }

        public static List<MetaBlock> decode(byte[] bytes) {
            List<MetaBlock> metaBlocks = new ArrayList<>();

            int index = 0;
            while (index < bytes.length) {
                int offset = ByteUtil.readU32(bytes, index);
                index += 4;
                int firstKeySize = ByteUtil.readU16AsU32(bytes, index);
                index += 2;
                byte[] firstKey = new byte[firstKeySize];
                ByteUtil.readAllBytes(firstKey, index, bytes);
                index += firstKeySize;
                metaBlocks.add(new MetaBlock(offset, firstKey));
            }

            return metaBlocks;
        }

        @Override
        public int estimateSize() {
            return SIZE_OF_U32 + SIZE_OF_U16 + firstKey.length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MetaBlock metaBlock = (MetaBlock) o;

            if (offset != metaBlock.offset) return false;
            return Arrays.equals(firstKey, metaBlock.firstKey);
        }

        @Override
        public int hashCode() {
            int result = offset;
            result = 31 * result + Arrays.hashCode(firstKey);
            return result;
        }
    }

    public static final class SSTableBuilder {
        private final Block.BlockBuilder blockBuilder;
        private final List<Block> blocks;
        private final List<MetaBlock> metaBlocks;
        private int currentBlockOffset;

        public SSTableBuilder() {
            this(DEFAULT_BLOCK_SIZE);
        }

        public SSTableBuilder(int blockSize) {
            this.blockBuilder = new Block.BlockBuilder(blockSize);
            this.blocks = new ArrayList<>();
            this.metaBlocks = new ArrayList<>();
            // offset for magic number(8B)
            this.currentBlockOffset = SIZE_OF_U64;
        }

        public void put(byte[] key, byte[] value) {
            if (blockBuilder.put(key, value)) {
                // skip
            } else {
                generateBlock();
                blockBuilder.put(key, value);
            }
        }

        void generateBlock() {
            Block block = blockBuilder.build();
            blocks.add(block);
            metaBlocks.add(new MetaBlock(currentBlockOffset, block.firstKey()));

            currentBlockOffset += block.estimateSize();
            blockBuilder.reset();
        }

        public SSTable build(long id, File dir, LRUCache<Long, Block> blockCache) throws IOException {
            generateBlock();

            Preconditions.checkState(dir.exists() && dir.isDirectory());
            File file = makeFile(dir, makeFileName(id, SST_FILE_SUFFIX));

            int n = 0;
            for (Block block : blocks) {
                n += block.getOffsets().length;
            }

            BloomFilter bloomFilter = new BloomFilter(n);
            for (Block block : blocks) {
                Block.BlockIterator iter = Block.BlockIterator.createAndSeekToFirst(block);
                while (iter.isValid()) {
                    bloomFilter.add(iter.key());
                    iter.next();
                }
            }
            byte[] bloomFilterBytes = bloomFilter.encode();

            int metaBlockOffset = currentBlockOffset;

            //
            //   ------------------------------------------------
            //  | meta blocks | meta block offset | bloom filter |
            //  |-------------+-------------------+--------------|
            //  |      ?B     |         4B        |      ?B      |
            //   ------------------------------------------------
            //                ^                   ^
            //        current block offset   bloom filter offset
            //

            int bloomFilterOffset = currentBlockOffset + SIZE_OF_U32;
            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(Constants.MAGIC);

                for (Block block : blocks) {
                    fos.write(block.encode());
                }

                for (MetaBlock metaBlock : metaBlocks) {
                    bloomFilterOffset += metaBlock.estimateSize();
                }

                fos.write(MetaBlock.encode(metaBlocks));

                byte[] offsetBytes = new byte[4];
                ByteUtil.writeU32(offsetBytes, 0, metaBlockOffset);
                fos.write(offsetBytes);

                fos.write(bloomFilterBytes);
                ByteUtil.writeU32(offsetBytes, 0, bloomFilterOffset);
                fos.write(offsetBytes);
            }

            return new SSTable(id, dir, metaBlocks, currentBlockOffset, bloomFilter, blockCache);
        }
    }

    public static final class SSTableIterator implements StorageIterator {
        private final SSTable sst;
        private Block.BlockIterator blockIterator;
        private int blockIndex;

        public static SSTableIterator createAndSeekToFirst(SSTable sst) throws IOException {
            Block.BlockIterator blockIterator = Block.BlockIterator.createAndSeekToFirst(sst.readCachedBlock(0));
            return new SSTableIterator(sst, blockIterator, 0);
        }

        public static SSTableIterator createAndSeekToKey(SSTable sst, byte[] key) throws IOException {
            int blockIndex = sst.findBlockIndex(key);
            Block block = sst.readCachedBlock(blockIndex);
            Block.BlockIterator blockIterator = Block.BlockIterator.createAndSeekToKey(block, key);
            return new SSTableIterator(sst, blockIterator, blockIndex);
        }

        public SSTableIterator(SSTable sst, Block.BlockIterator blockIterator, int blockIndex) {
            this.sst = sst;
            this.blockIterator = blockIterator;
            this.blockIndex = blockIndex;
        }

        @Override
        public byte[] key() {
            return blockIterator.key();
        }

        @Override
        public byte[] value() {
            return blockIterator.value();
        }

        @Override
        public boolean isValid() {
            return blockIterator.isValid();
        }

        @Override
        public void next() throws IOException {
            blockIterator.next();
            if (!blockIterator.isValid()) {
                blockIndex += 1;
                if (blockIndex < sst.numsOfBlocks()) {
                    blockIterator = Block.BlockIterator.createAndSeekToFirst(sst.readCachedBlock(blockIndex));
                }
            }
        }

        public void seekToFirst() throws IOException {
            blockIndex = 0;
            blockIterator = Block.BlockIterator.createAndSeekToFirst(sst.readCachedBlock(0));
        }

        public void seekToKey(byte[] key) throws IOException {
            blockIndex = sst.findBlockIndex(key);
            Block block = sst.readBlock(blockIndex);
            blockIterator = Block.BlockIterator.createAndSeekToKey(block, key);
        }
    }
}