// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.cabindb.test;

import org.cabindb.AbstractCompactionFilter;
import org.cabindb.AbstractCompactionFilterFactory;
import org.cabindb.RemoveEmptyValueCompactionFilter;

/**
 * Simple CompactionFilterFactory class used in tests. Generates RemoveEmptyValueCompactionFilters.
 */
public class RemoveEmptyValueCompactionFilterFactory extends AbstractCompactionFilterFactory<RemoveEmptyValueCompactionFilter> {
    @Override
    public RemoveEmptyValueCompactionFilter createCompactionFilter(final AbstractCompactionFilter.Context context) {
        return new RemoveEmptyValueCompactionFilter();
    }

    @Override
    public String name() {
        return "RemoveEmptyValueCompactionFilterFactory";
    }
}
