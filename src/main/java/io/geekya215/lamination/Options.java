package io.geekya215.lamination;

import static io.geekya215.lamination.Constants.KB;
import static io.geekya215.lamination.Constants.MB;

public final class Options {
    private Options() {
    }

    static final int DEFAULT_BLOCK_SIZE = 4 * KB;
    static final int DEFAULT_SST_SIZE = 4 * MB;
    static final int DEFAULT_MEM_TABLE_SIZE = 64 * MB;
}
