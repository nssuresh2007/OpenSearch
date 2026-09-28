/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.engine.exec.SearchableDirectoryReaderProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Builds the {@link Engine.SearcherSupplier} for a data-format-aware shard, shared by
 * {@link DataFormatAwareEngine} and {@link DataFormatAwareReadOnlyEngine}. Pins the reader reference,
 * selects the searchable format by
 * capability, and wraps it in an {@link OpenSearchDirectoryReader}.
 */
final class DataFormatAwareSearcherSupport {

    private DataFormatAwareSearcherSupport() {}

    /**
     * Builds a point-in-time searcher supplier over {@code readerRef}'s reader, wrapping the searchable
     * format's {@link DirectoryReader} in an {@link OpenSearchDirectoryReader}. Takes ownership of
     * {@code readerRef}: it is released by the supplier's {@code close()}, or immediately if this throws.
     */
    static Engine.SearcherSupplier acquireSearcherSupplier(
        ShardId shardId,
        EngineConfig engineConfig,
        GatedCloseable<Reader> readerRef,
        Function<Engine.Searcher, Engine.Searcher> wrapper,
        Logger logger
    ) {
        try {
            final DirectoryReader rawDirectoryReader = extractDirectoryReader(shardId, engineConfig, readerRef.get());
            // IndexShard.wrapSearcher later asserts the reader is an OpenSearchDirectoryReader.
            final DirectoryReader directoryReader = OpenSearchDirectoryReader.wrap(rawDirectoryReader, shardId);
            return new Engine.SearcherSupplier(wrapper) {
                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    return new Engine.Searcher(
                        source,
                        directoryReader,
                        engineConfig.getSimilarity(),
                        engineConfig.getQueryCache(),
                        engineConfig.getQueryCachingPolicy(),
                        () -> {}
                    );
                }

                @Override
                protected void doClose() {
                    IOUtils.closeWhileHandlingException(readerRef);
                }
            };
        } catch (IllegalStateException e) {
            IOUtils.closeWhileHandlingException(readerRef);
            throw e;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(readerRef);
            throw new EngineException(shardId, "failed to build searcher supplier from composite reader", e);
        }
    }

    /**
     * Resolves the {@link DirectoryReader} by capability: exactly one registered format must expose a
     * {@link SearchableDirectoryReaderProvider}. Zero or several is a configuration error.
     */
    private static DirectoryReader extractDirectoryReader(ShardId shardId, EngineConfig engineConfig, Reader reader) {
        List<String> matches = new ArrayList<>();
        SearchableDirectoryReaderProvider provider = null;
        for (DataFormat format : engineConfig.getDataFormatRegistry().getRegisteredFormats()) {
            Object formatReader = reader.reader(format);
            if (formatReader instanceof SearchableDirectoryReaderProvider searchable) {
                matches.add(format.name());
                provider = searchable;
            }
        }
        if (matches.isEmpty()) {
            throw new IllegalStateException(
                "No searchable reader (SearchableDirectoryReaderProvider) available for composite index " + shardId
            );
        }
        if (matches.size() > 1) {
            throw new IllegalStateException(
                "Multiple searchable readers for composite index " + shardId + "; ambiguous formats " + matches
            );
        }
        return provider.directoryReader();
    }
}
