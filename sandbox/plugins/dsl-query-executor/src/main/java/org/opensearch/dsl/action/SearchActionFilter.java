/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Map;

/**
 * Intercepts search-related transport actions and dispatches them to DSL handlers
 * for execution through the Calcite pipeline.
 *
 * <p>Supported actions (dispatch map entries):
 * <ul>
 *   <li>{@code _search} ({@link SearchAction#NAME}) &rarr; {@link DslExecuteAction}</li>
 * </ul>
 *
 * <p>The following APIs are <b>not</b> intercepted because the native transport actions
 * call {@code client.search()} internally, which triggers {@link SearchAction#NAME}
 * and is intercepted by this filter automatically:
 * <ul>
 *   <li>{@code _search/template} &mdash; {@code TransportSearchTemplateAction} renders the
 *       Mustache template then calls {@code client.search()}</li>
 *   <li>{@code _msearch} &mdash; {@code TransportMultiSearchAction} calls {@code client.search()} per sub-request</li>
 *   <li>{@code _msearch/template} &mdash; {@code TransportMultiSearchTemplateAction} renders templates,
 *       then calls {@code client.multiSearch()} which chains through {@code _msearch} &rarr;
 *       {@code client.search()} per sub-request</li>
 * </ul>
 *
 * <p>Non-matching actions pass through the filter chain unchanged.
 */
public class SearchActionFilter implements ActionFilter {

    /** Runs after the Security plugin's authorization filter (order 0). */
    static final int FILTER_ORDER = 1;

    private final NodeClient client;
    private final Map<String, ActionDispatcher> dispatchers;

    /**
     * Dispatches an intercepted request to the appropriate DSL transport action.
     */
    @FunctionalInterface
    interface ActionDispatcher {
        void dispatch(Task task, ActionRequest request, ActionListener<?> listener);
    }

    /**
     * Creates a filter that dispatches intercepted search actions via the given client.
     *
     * @param client node client for dispatching to DSL transport actions
     */
    public SearchActionFilter(NodeClient client) {
        this.client = client;
        this.dispatchers = Map.of(SearchAction.NAME, (task, req, listener) -> dispatchSearch(req, listener));
    }

    @Override
    public int order() {
        return FILTER_ORDER;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        ActionDispatcher dispatcher = dispatchers.get(action);
        if (dispatcher != null) {
            dispatcher.dispatch(task, request, listener);
        } else {
            chain.proceed(task, action, request, listener);
        }
    }

    @SuppressWarnings("unchecked")
    private void dispatchSearch(ActionRequest request, ActionListener<?> listener) {
        SearchRequest searchRequest = (SearchRequest) request;
        client.execute(DslExecuteAction.INSTANCE, searchRequest, (ActionListener<SearchResponse>) listener);
    }
}
