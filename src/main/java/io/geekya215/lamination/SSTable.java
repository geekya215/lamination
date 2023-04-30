package io.geekya215.lamination;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static io.geekya215.lamination.Constant.*;

//  -----------------------------------------------------------------------------
// |                                   SST                                       |
// |-----------------------------------------------------------------------------|
// | data block | data block | meta block | meta block | meta block offset (u32) |
//  -----------------------------------------------------------------------------
public final class SSTable {
    private final int id;
    private final File file;
    private final List<MetaBlock> metaBlocks;
    private final int metaBlockOffset;

    public SSTable(int id, File file, List<MetaBlock> metaBlocks, int metaBlockOffset) {
        this.id = id;
        this.file = file;
        this.metaBlocks = metaBlocks;
        this.metaBlockOffset = metaBlockOffset;
    }

    public File file() {
        return file;
    }

    public List<MetaBlock> metaBlocks() {
        return metaBlocks;
    }

    public int metaBlockOffset() {
        return metaBlockOffset;
    }

    public static SSTable open(int id, File file) throws IOException {
        int fileSize = (int) file.length();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek(fileSize - 4);
            int metaBlockOffset = randomAccessFile.readInt();

            randomAccessFile.seek(metaBlockOffset);
            int metaBlocksSize = fileSize - metaBlockOffset - 4;
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

            return new SSTable(id, file, metaBlocks, metaBlockOffset);
        }
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
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek(offset);
            randomAccessFile.readFully(bytes);
        }

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

    //  ------------------------------------------
    // |             |          first key         |
    // |------------------------------------------|
    // | offset (4B) | keylen (2B) | key (keylen) |
    //  ------------------------------------------
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
            int offset = (bytes[0] & 0xff) << 24
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
            this.currentBlockOffset = 0;
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

        File persist(Path path) throws IOException {
            File file = Files.createFile(path).toFile();

            try (FileOutputStream fos = new FileOutputStream(file)) {

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

            return file;
        }

        public SSTable build(int id, Path path) throws IOException {
            generateBlock();

            File file = persist(path);
            return new SSTable(id, file, metaBlocks, currentBlockOffset);
        }
    }

    public static final class SSTableIterator implements Iterator<Block> {
        private final SSTable sst;
        private Block.BlockIterator blockIterator;
        private int blockIndex;

        public SSTableIterator(SSTable sst, Block.BlockIterator iterator, int blockIndex) {
            this.sst = sst;
            this.blockIterator = iterator;
            this.blockIndex = blockIndex;
        }

        public void setBlockIterator(Block.BlockIterator blockIterator) {
            this.blockIterator = blockIterator;
        }

        public static SSTableIterator createAndSeekToFirst(SSTable sst) throws IOException {
            Block block = sst.readBlock(0);
            Block.BlockIterator blockIterator = Block.BlockIterator.createAndSeekToFirst(block);
            return new SSTableIterator(sst, blockIterator, 0);
        }

        public static SSTableIterator createAndSeekToKey(SSTable sst, byte[] key) throws IOException {
            int blockIndex = sst.findBlockIndex(key);
            Block block = sst.readBlock(blockIndex);
            Block.BlockIterator blockIterator = null;
            try {
                blockIterator = Block.BlockIterator.createAndSeekToKey(block, key);
            } catch (NoSuchElementException e) {
                blockIterator = Block.BlockIterator.createAndSeekToFirst(block);
            }
            return new SSTableIterator(sst, blockIterator, blockIndex);
        }

        public Block seekToFirst() throws IOException {
            Block block = sst.readBlock(0);
            blockIterator = Block.BlockIterator.createAndSeekToFirst(block);
            blockIndex = 0;
            return block;
        }

        public Block seekToKey(byte[] key) throws IOException {
            blockIndex = sst.findBlockIndex(key);
            Block block = sst.readBlock(blockIndex);
            blockIterator = Block.BlockIterator.createAndSeekToKey(block, key);
            return block;
        }

        @Override
        public boolean hasNext() {
            return blockIndex < sst.metaBlocks.size();
        }

        @Override
        public Block next() {
            if (blockIndex >= sst.metaBlocks.size()) {
                throw new NoSuchElementException();
            }
            try {
                Block block = sst.readBlock(blockIndex++);
                blockIterator = Block.BlockIterator.createAndSeekToFirst(block);
                return block;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
