package io.geekya215.lamination;

import io.geekya215.lamination.util.Preconditions;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.geekya215.lamination.Constants.*;
import static io.geekya215.lamination.Options.DEFAULT_BLOCK_SIZE;
import static io.geekya215.lamination.util.FileUtil.*;


//
//  -----------------------------------------------------------------------------
// |                                   SST                                       |
// |-----------------------------------------------------------------------------|
// |  magic(8B) | data block | data block | data block | meta block | meta block |
//  -----------------------------------------------------------------------------
//
public final class SSTable implements Closeable {
    private final long id;
    private final RandomAccessFile file;
    private final List<MetaBlock> metaBlocks;
    private final int metaBlockOffset;

    public SSTable(long id, RandomAccessFile file, List<MetaBlock> metaBlocks, int metaBlockOffset) {
        this.id = id;
        this.file = file;
        this.metaBlocks = metaBlocks;
        this.metaBlockOffset = metaBlockOffset;
    }

    public SSTable(long id, File baseDir, List<MetaBlock> metaBlocks, int metaBlockOffset) throws IOException {
        this.id = id;
        this.file = new RandomAccessFile(makeSSTableFile(baseDir, id), "r");
        this.metaBlocks = metaBlocks;
        this.metaBlockOffset = metaBlockOffset;
    }

    public List<MetaBlock> getMetaBlocks() {
        return metaBlocks;
    }

    public int getMetaBlockOffset() {
        return metaBlockOffset;
    }

    public static SSTable open(long id, File baseDir) throws IOException {
        Preconditions.checkState(baseDir.exists() && baseDir.isDirectory());
        String filename = makeFileName(id, SST_FILE_SUFFIX);
        File file = new File(baseDir, filename);
        if (!file.exists()) {
            throw new FileNotFoundException();
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        byte[] magic = new byte[8];
        randomAccessFile.readFully(magic);
        int cmp = Arrays.compare(MAGIC, magic);
        if (cmp != 0) {
            throw new RuntimeException("invalid sst file format");
        }

        int fileSize = (int) file.length();
        randomAccessFile.seek(fileSize - SIZE_OF_U32);
        int metaBlockOffset = randomAccessFile.readInt();

        randomAccessFile.seek(metaBlockOffset);
        int metaBlocksSize = fileSize - metaBlockOffset - SIZE_OF_U32;
        byte[] metaBlocksBytes = new byte[metaBlocksSize];
        randomAccessFile.readFully(metaBlocksBytes);

        List<MetaBlock> metaBlocks = new ArrayList<>();
        int index = 0;
        while (index < metaBlocksSize) {
            int offset = (metaBlocksBytes[index] & 0xff) << 24
                | (metaBlocksBytes[index + 1] & 0xff) << 16
                | (metaBlocksBytes[index + 2] & 0xff) << 8
                | (metaBlocksBytes[index + 3] & 0xff);
            int keySize = (metaBlocksBytes[index + 4] & 0xff << 8) | (metaBlocksBytes[index + 5] & 0xff);
            byte[] firstKey = new byte[keySize];
            System.arraycopy(metaBlocksBytes, index + 6, firstKey, 0, keySize);
            index += (6 + keySize);
            metaBlocks.add(new MetaBlock(offset, firstKey));
        }

        randomAccessFile.seek(0);
        return new SSTable(id, randomAccessFile, metaBlocks, metaBlockOffset);
    }

    public Block readBlock(int blockIndex) throws IOException {
        int offset = metaBlocks.get(blockIndex).offset;
        int offsetEnd;

        if (blockIndex + 1 >= metaBlocks.size()) {
            offsetEnd = metaBlockOffset;
        } else {
            offsetEnd = metaBlocks.get(blockIndex + 1).offset;
        }
        byte[] bytes = new byte[offsetEnd - offset];
        file.seek(offset);
        file.readFully(bytes);

        return Block.decode(bytes);
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

    @Override
    public void close() throws IOException {
        file.close();
    }

    //
    //  ------------------------------------------
    // |             |          first key         |
    // |------------------------------------------|
    // | offset (4B) | keylen (2B) | key (keylen) |
    //  ------------------------------------------
    //
    public record MetaBlock(int offset, byte[] firstKey) implements Measurable {
        public byte[] encode() {
            int keySize = firstKey.length;
            byte[] bytes = new byte[SIZE_OF_U32 + SIZE_OF_U16 + keySize];

            bytes[0] = (byte) (offset >> 24);
            bytes[1] = (byte) (offset >> 16);
            bytes[2] = (byte) (offset >> 8);
            bytes[3] = (byte) offset;

            bytes[4] = (byte) (keySize >> 8);
            bytes[5] = (byte) keySize;

            System.arraycopy(firstKey, 0, bytes, 6, keySize);
            return bytes;
        }

        public static MetaBlock decode(byte[] bytes) {
            int offset
                = (bytes[0] & 0xff) << 24
                | (bytes[1] & 0xff) << 16
                | (bytes[2] & 0xff) << 8
                | (bytes[3] & 0xff);
            int keySize = (bytes[4] & 0xff << 8) | (bytes[5] & 0xff);
            byte[] firstKey = new byte[keySize];
            System.arraycopy(bytes, 6, firstKey, 0, keySize);
            return new MetaBlock(offset, firstKey);
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

        public SSTable build(long id, File baseDir) throws IOException {
            generateBlock();

            Preconditions.checkState(baseDir.exists() && baseDir.isDirectory());
            File file = makeFile(baseDir, makeFileName(id, SST_FILE_SUFFIX));

            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(Constants.MAGIC);

                for (Block block : blocks) {
                    fos.write(block.encode());
                }

                for (MetaBlock metaBlock : metaBlocks) {
                    fos.write(metaBlock.encode());
                }

                byte[] metaBlockOffset = new byte[4];
                metaBlockOffset[0] = (byte) (currentBlockOffset >> 24);
                metaBlockOffset[1] = (byte) (currentBlockOffset >> 16);
                metaBlockOffset[2] = (byte) (currentBlockOffset >> 8);
                metaBlockOffset[3] = (byte) currentBlockOffset;

                fos.write(metaBlockOffset);
            }
            return new SSTable(id, baseDir, metaBlocks, currentBlockOffset);
        }
    }

    public static final class SSTableIterator {
        private final SSTable sst;
        private Block.BlockIterator blockIterator;
        private int blockIndex;

        public static SSTableIterator createAndSeekToFirst(SSTable sst) throws IOException {
            Block.BlockIterator blockIterator = Block.BlockIterator.createAndSeekToFirst(sst.readBlock(0));
            return new SSTableIterator(sst, blockIterator, 0);
        }

        public static SSTableIterator createAndSeekToKey(SSTable sst, byte[] key) throws IOException {
            int blockIndex = sst.findBlockIndex(key);
            Block block = sst.readBlock(blockIndex);
            Block.BlockIterator blockIterator = Block.BlockIterator.createAndSeekToKey(block, key);
            return new SSTableIterator(sst, blockIterator, blockIndex);
        }

        public byte[] key() {
            return blockIterator.getKey();
        }

        public byte[] value() {
            return blockIterator.getValue();
        }

        public boolean isValid() {
            return blockIterator.isValid();
        }

        public void next() throws IOException {
            blockIterator.next();
            if (!blockIterator.isValid()) {
                blockIndex += 1;
                if (blockIndex < sst.metaBlocks.size()) {
                    blockIterator = Block.BlockIterator.createAndSeekToFirst(sst.readBlock(blockIndex));
                }
            }
        }

        public SSTableIterator(SSTable sst, Block.BlockIterator blockIterator, int blockIndex) {
            this.sst = sst;
            this.blockIterator = blockIterator;
            this.blockIndex = blockIndex;
        }

        public SSTableIterator(SSTable sst) throws IOException {
            this.sst = sst;
            this.blockIterator = Block.BlockIterator.createAndSeekToFirst(sst.readBlock(0));
            this.blockIndex = 0;
        }

        public void seekToFirst() throws IOException {
            blockIndex = 0;
            blockIterator = Block.BlockIterator.createAndSeekToFirst(sst.readBlock(0));
        }

        public void seekToKey(byte[] key) throws IOException {
            blockIndex = sst.findBlockIndex(key);
            Block block = sst.readBlock(blockIndex);
            blockIterator = Block.BlockIterator.createAndSeekToKey(block, key);
        }
    }
}