package io.geekya215.lamination;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static io.geekya215.lamination.Constant.*;

// |                                 SST                                         |
// | data block | data block | meta block | meta block | meta block offset (u32) |
public class SsTable {
    private final List<Block> blocks;
    private final List<MetaBlock> metaBlocks;
    private final int blockMetaOffset;
    private final int id;

    public SsTable(List<Block> blocks, List<MetaBlock> metaBlocks, int blockMetaOffset, int id) {
        this.blocks = blocks;
        this.metaBlocks = metaBlocks;
        this.blockMetaOffset = blockMetaOffset;
        this.id = id;
    }

    public void persist(Path path) {
        try (FileOutputStream fos = new FileOutputStream(Files.createFile(path).toFile())) {
            for (Block block : blocks) {
                fos.write(block.encode().values());
            }

            for (MetaBlock metaBlock : metaBlocks) {
                int offset = metaBlock.offset;
                int keyLength = metaBlock.firstKey.length();
                byte[] bytes = new byte[SIZE_OF_U32 + SIZE_OF_U16 + keyLength];

                bytes[0] = (byte) (offset >> 24);
                bytes[1] = (byte) (offset >> 16);
                bytes[2] = (byte) (offset >> 8);
                bytes[3] = (byte) offset;

                bytes[4] = (byte) (keyLength >> 8);
                bytes[5] = (byte) keyLength;

                System.arraycopy(metaBlock.firstKey.values(), 0, bytes, 6, keyLength);
                fos.write(bytes);
            }

            byte[] offsetBytes = new byte[4];
            offsetBytes[0] = (byte) (blockMetaOffset >> 24);
            offsetBytes[1] = (byte) (blockMetaOffset >> 16);
            offsetBytes[2] = (byte) (blockMetaOffset >> 8);
            offsetBytes[3] = (byte) blockMetaOffset;

            fos.write(offsetBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // |             |          first key         |
    // | offset (4B) | keylen (2B) | key (keylen) |
    record MetaBlock(int offset, Bytes firstKey) {
    }

    public static final class SsTableBuilder {
        private final Block.BlockBuilder blockBuilder;
        private final List<Block> blocks;
        private final List<MetaBlock> metaBlocks;
        private int currentBlockOffset;

        public SsTableBuilder() {
            this(DEFAULT_BLOCK_SIZE);
        }

        public SsTableBuilder(int blockSize) {
            this.blockBuilder = new Block.BlockBuilder(blockSize);
            this.blocks = new ArrayList<>();
            this.metaBlocks = new ArrayList<>();
            this.currentBlockOffset = 0;
        }

        public void put(Bytes key, Bytes value) {
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
            metaBlocks.add(new MetaBlock(currentBlockOffset, block.getFirstKey()));

            currentBlockOffset += block.estimateSize();
            blockBuilder.reset();
        }

        public SsTable build(int id) {
            generateBlock();
            return new SsTable(
                blocks,
                metaBlocks,
                currentBlockOffset,
                id
            );
        }
    }
}
