package water.data.transformations;

import water.Iced;

import java.util.Objects;
import java.util.Optional;

public class DuplicatedRow extends Iced<DuplicatedRow> {

    private final int chunkId;
    private final int startRow;
    private final int endRow;
    private final RowSample rowSample;

    public DuplicatedRow(int chunkId, int startRow, int endRow) {
        this.chunkId = chunkId;
        this.startRow = startRow;
        this.endRow = endRow;
        this.rowSample = null;
    }

    public DuplicatedRow(final int chunkId, final int startRow, final int endRow, final RowSample rowSample) {
        this.chunkId = chunkId;
        this.startRow = startRow;
        this.endRow = endRow;
        this.rowSample = rowSample;
    }

    public int getChunkId() {
        return chunkId;
    }

    public int getStartRow() {
        return startRow;
    }

    public int getEndRow() {
        return endRow;
    }

    public Optional<RowSample> getRowSample() {
        return Optional.ofNullable(rowSample);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DuplicatedRow that = (DuplicatedRow) o;
        return chunkId == that.chunkId &&
                startRow == that.startRow;
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunkId, startRow);
    }
}
