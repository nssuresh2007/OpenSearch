/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lease.Releasable;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds per-request state for the aggregation delegation lifecycle.
 * Registered via {@code ctx.addReleasable(state)} so cleanup happens
 * on {@code SearchContext.close()}.
 *
 * <p>Tracks the bitset collector manager, eligible aggregations, the provider key
 * registered in the {@link BitsetProviderRegistry}, and any backend sessions opened
 * during execution. On {@link #close()}, releases the provider from the registry,
 * closes backend sessions in reverse order, and nulls out references to allow GC.
 *
 * <p>The provider key is not known at construction time because the
 * {@link DelegatingAggregationProcessor} creates this state in {@code preProcess}
 * (before bitsets are collected), while the provider key is assigned later in
 * {@code postProcess} when {@link AggregationDelegationService#execute} registers
 * the bitsets in the registry. Use {@link #setProviderKey(int)} and
 * {@link #setBitsetProviderRegistry(BitsetProviderRegistry)} to wire these after
 * construction.
 *
 * @opensearch.internal
 */
public class DelegationRequestState implements Releasable {

    private static final Logger logger = LogManager.getLogger(DelegationRequestState.class);

    private DocIdBitsetCollectorManager bitsetCollectorManager;
    private final List<EligibleAggregation> eligibleAggregations;
    private final List<AutoCloseable> backendSessions;
    private volatile boolean closed;

    /**
     * The provider key returned by {@link BitsetProviderRegistry#registerBitsets}.
     * A value of 0 means "not set" (no provider has been registered yet).
     */
    private int providerKey;

    /**
     * Reference to the registry so that {@link #close()} can release the provider.
     * May be null if delegation did not reach the execution phase.
     */
    private BitsetProviderRegistry bitsetProviderRegistry;

    public DelegationRequestState(DocIdBitsetCollectorManager bitsetCollectorManager, List<EligibleAggregation> eligibleAggregations) {
        this.bitsetCollectorManager = bitsetCollectorManager;
        this.eligibleAggregations = eligibleAggregations;
        this.backendSessions = new ArrayList<>();
        this.closed = false;
        this.providerKey = 0;
        this.bitsetProviderRegistry = null;
    }

    /**
     * Returns the bitset collector manager used to collect per-segment matching docIds.
     */
    public DocIdBitsetCollectorManager getBitsetCollectorManager() {
        return bitsetCollectorManager;
    }

    /**
     * Returns the list of aggregations eligible for backend delegation.
     */
    public List<EligibleAggregation> getEligibleAggregations() {
        return eligibleAggregations;
    }

    /**
     * Returns the provider key registered in the {@link BitsetProviderRegistry},
     * or 0 if no provider has been registered yet.
     */
    public int getProviderKey() {
        return providerKey;
    }

    /**
     * Sets the provider key after the bitsets have been registered in the
     * {@link BitsetProviderRegistry}. Called from {@code postProcess} when
     * {@link AggregationDelegationService#execute} registers the bitsets.
     *
     * @param providerKey the key returned by {@link BitsetProviderRegistry#registerBitsets}
     */
    public void setProviderKey(int providerKey) {
        this.providerKey = providerKey;
    }

    /**
     * Returns the {@link BitsetProviderRegistry} reference, or {@code null} if not set.
     */
    public BitsetProviderRegistry getBitsetProviderRegistry() {
        return bitsetProviderRegistry;
    }

    /**
     * Sets the {@link BitsetProviderRegistry} reference so that {@link #close()} can
     * release the provider. Called from {@code postProcess} alongside
     * {@link #setProviderKey(int)}.
     *
     * @param registry the registry holding this request's bitsets
     */
    public void setBitsetProviderRegistry(BitsetProviderRegistry registry) {
        this.bitsetProviderRegistry = registry;
    }

    /**
     * Registers a backend session for cleanup on close.
     */
    public void addBackendSession(AutoCloseable session) {
        backendSessions.add(session);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        // Step 1: Release the provider from the registry if both are set
        if (bitsetProviderRegistry != null && providerKey > 0) {
            try {
                bitsetProviderRegistry.releaseProvider(providerKey);
            } catch (Exception e) {
                logger.warn("Failed to release bitset provider key [{}]: {}", providerKey, e.getMessage());
            }
        }

        // Step 2: Close backend sessions in reverse order
        for (int i = backendSessions.size() - 1; i >= 0; i--) {
            try {
                backendSessions.get(i).close();
            } catch (Exception e) {
                logger.debug("Exception closing backend session: {}", e.getMessage());
            }
        }
        backendSessions.clear();

        // Step 3: Null out references to allow GC
        bitsetCollectorManager = null;
        bitsetProviderRegistry = null;
    }
}
