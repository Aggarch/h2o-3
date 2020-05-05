package water.data.transformations;

import water.Iced;

public class RowSample extends Iced<RowSample> {
    
    private final Iced[] values;
    private final int chunkId;

    public RowSample(final Iced[] values, final int chunkId) {
        this.values = values;
        this.chunkId = chunkId;
    }

    public Iced[] getValues() {
        return values;
    }

    public int getChunkId() {
        return chunkId;
    }
}
