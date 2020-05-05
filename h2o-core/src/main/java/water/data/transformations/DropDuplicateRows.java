package water.data.transformations;

import water.Scope;
import water.fvec.Frame;
import water.rapids.Merge;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class DropDuplicateRows {

    public enum DropOrder {
        DropFirst,
        DropLast
    }

    final Frame sourceFrame;
    final int[] comparedColumnIndices;
    final DropOrder dropOrder;

    public DropDuplicateRows(final Frame sourceFrame, final int[] comparedColumnIndices, final DropOrder dropOrder) {
        this.sourceFrame = sourceFrame;
        this.comparedColumnIndices = comparedColumnIndices;
        this.dropOrder = dropOrder;
    }

    public Frame dropDuplicates() {
        try {
            Scope.enter();
            final Frame sortedFrame = Scope.track(sortByAllColumns());
            DetectDuplicatesTask detectDuplicatesTask = new DetectDuplicatesTask(comparedColumnIndices);
            final DetectDuplicatesTask detectDuplicatesResult = detectDuplicatesTask.doAll(sortedFrame).getResult();

            final Queue<DuplicatedRow> sortedDuplicatedRows = Arrays.asList(detectDuplicatesResult.getDuplicatedRows()).stream()
                    .sorted(Comparator.comparingInt(DuplicatedRow::getChunkId).thenComparingInt(DuplicatedRow::getStartRow))
                    .collect(Collectors.toCollection(LinkedList::new));

            final List<RowSample> firstRowSamples = Arrays.asList(detectDuplicatesResult.getFirstRowSamples()).stream()
                    .sorted(Comparator.comparingInt(RowSample::getChunkId))
                    .collect(Collectors.toList());

            return detectDuplicatesTask._fr;
        } finally {
            Scope.exit();
        }
    }

    /**
     * Creates a copy of the original dataset, sorted by all compared columns.
     * The sort is done with respect to {@link DropOrder} value.
     *
     * @return A new Frame sorted by all compared columns.
     */
    private Frame sortByAllColumns() {
        final boolean ascending = dropOrder == DropOrder.DropFirst ? true : false;
        return Merge.sort(sourceFrame, comparedColumnIndices, ascending);
    }
}
