/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Generates plan alternatives for each {@link Stage} in a {@link QueryDAG}.
 *
 * <p>Walks each stage's marked fragment bottom-up. For each operator, generates
 * one {@link StagePlan} per viable backend. Annotations are grouped by target
 * backend to avoid combinatorial explosion — with a single backend (pure DF),
 * this naturally produces one alternative per stage.
 *
 * <p>TODO: gate plan forking based on index stats (size, shard count, doc count).
 * For small indices, generating multiple alternatives adds overhead with minimal benefit.
 *
 * <p>TODO: add pruning via BackendPriority and cost functions when multiple backends
 * are viable for the same stage.
 *
 * @opensearch.internal
 */
public class PlanForker {

    private static final Logger logger = LogManager.getLogger(PlanForker.class);

    /**
     * Functions for which we prefer delegating to a specialized backend (e.g. Lucene inverted index)
     * over native evaluation on the operator backend (e.g. DataFusion row-scan).
     * Currently EQUALS (TermQuery) and IN (TermsQuery) because the Lucene backend uses only the inverted index.
     */
    private static final Set<ScalarFunction> DELEGATION_PREFERRED_FUNCTIONS = Set.of(
        ScalarFunction.EQUALS,
        ScalarFunction.IN,
        ScalarFunction.SARG_PREDICATE
    );

    private PlanForker() {}

    public static void forkAll(QueryDAG dag, CapabilityRegistry registry) {
        forkStage(dag.rootStage(), registry);
    }

    private static void forkStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            forkStage(child, registry);
        }
        if (stage.getFragment() == null) {
            return;
        }
        List<Resolved> alternatives = resolve(stage.getFragment(), registry);
        stage.setPlanAlternatives(alternatives.stream().map(resolved -> new StagePlan(resolved.node, resolved.chosenBackend)).toList());
    }

    /** Resolved node paired with the backend chosen at this operator level. */
    private record Resolved(String chosenBackend, RelNode node) {
    }

    private static List<Resolved> resolve(RelNode node, CapabilityRegistry registry) {
        List<List<Resolved>> childAlternativeSets = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            childAlternativeSets.add(resolve(input, registry));
        }

        if (childAlternativeSets.isEmpty()) {
            return resolveOperator(node, List.of(), null, registry);
        }

        if (childAlternativeSets.size() == 1) {
            List<Resolved> results = new ArrayList<>();
            for (Resolved childAlt : childAlternativeSets.getFirst()) {
                results.addAll(resolveOperator(node, List.of(childAlt.node), childAlt.chosenBackend, registry));
            }
            return results;
        }

        // Multi-input: take the first alternative from each child. With a single backend
        // (pure DataFusion), each child has exactly one alternative anyway. For correctness
        // we require all children to agree on the chosen backend — a multi-input operator
        // cannot straddle backends within a single stage.
        // TODO: when multi-backend pipelines are added, fan out the Cartesian product of
        // child alternatives and prune by backend agreement.
        List<RelNode> resolvedChildren = new ArrayList<>(childAlternativeSets.size());
        String agreedBackend = null;
        for (List<Resolved> childAlts : childAlternativeSets) {
            if (childAlts.isEmpty()) {
                throw new IllegalStateException(
                    "Multi-input child of [" + node.getClass().getSimpleName() + "] produced no plan alternatives"
                );
            }
            Resolved childAlt = childAlts.getFirst();
            resolvedChildren.add(childAlt.node);
            if (agreedBackend == null) {
                agreedBackend = childAlt.chosenBackend;
            } else if (childAlt.chosenBackend != null
                && !childAlt.chosenBackend.isEmpty()
                && !childAlt.chosenBackend.equals(agreedBackend)) {
                    throw new IllegalStateException(
                        "Multi-input operator ["
                            + node.getClass().getSimpleName()
                            + "] requires all children to share a backend; got ["
                            + agreedBackend
                            + "] vs ["
                            + childAlt.chosenBackend
                            + "]"
                    );
                }
        }
        return resolveOperator(node, resolvedChildren, agreedBackend, registry);
    }

    private static List<Resolved> resolveOperator(RelNode node, List<RelNode> children, String childBackend, CapabilityRegistry registry) {
        if (!(node instanceof OpenSearchRelNode openSearchNode)) {
            // Non-OpenSearch node (e.g. StageInputScan infrastructure) — pass through.
            RelNode result = children.isEmpty() ? node : node.copy(node.getTraitSet(), children);
            return List.of(new Resolved(childBackend != null ? childBackend : "", result));
        }

        List<OperatorAnnotation> annotations = openSearchNode.getAnnotations();

        // Filter viable backends: only consider backends that match the child's chosen backend.
        // TODO: delegation will change this — cross-backend pipelines require revisiting
        // how the child backend propagates upward through the operator chain.
        List<String> backendsToConsider = new ArrayList<>();
        for (String backend : openSearchNode.getViableBackends()) {
            if (childBackend == null || backend.equals(childBackend)) {
                backendsToConsider.add(backend);
            }
        }

        List<Resolved> results = new ArrayList<>();
        for (String backend : backendsToConsider) {
            if (annotations.isEmpty()) {
                results.add(new Resolved(backend, openSearchNode.copyResolved(backend, children, List.of())));
                continue;
            }
            // Group annotations by target backend — one plan per distinct annotation backend group.
            // With a single backend, this produces exactly one alternative naturally.
            results.addAll(resolveWithBranching(openSearchNode, backend, children, annotations, registry));
        }
        return results;
    }

    private static List<Resolved> resolveWithBranching(
        OpenSearchRelNode node,
        String backend,
        List<RelNode> children,
        List<OperatorAnnotation> annotations,
        CapabilityRegistry registry
    ) {
        List<OperatorAnnotation> resolved = resolveAnnotationsToTarget(annotations, backend, backend, registry);
        return List.of(new Resolved(backend, node.copyResolved(backend, children, resolved)));
    }

    private static List<OperatorAnnotation> resolveAnnotationsToTarget(
        List<OperatorAnnotation> annotations,
        String targetBackend,
        String operatorBackend,
        CapabilityRegistry registry
    ) {
        List<OperatorAnnotation> resolved = new ArrayList<>();
        for (OperatorAnnotation annotation : annotations) {
            // Prefer delegation target only for specific functions (EQUALS → TermQuery)
            String delegationTarget = findDelegationTarget(annotation, operatorBackend, registry);
            if (delegationTarget != null) {
                logger.info(
                    "Annotation [{}] delegated to [{}] (function in DELEGATION_PREFERRED_FUNCTIONS, operator backend=[{}])",
                    annotation,
                    delegationTarget,
                    operatorBackend
                );
                resolved.add(annotation.narrowTo(delegationTarget));
            } else if (annotation.getViableBackends().contains(targetBackend)) {
                logger.info("Annotation [{}] narrowed to target backend [{}]", annotation, targetBackend);
                resolved.add(annotation.narrowTo(targetBackend));
            } else if (annotation.getViableBackends().contains(operatorBackend)) {
                logger.info("Annotation [{}] narrowed to operator backend [{}]", annotation, operatorBackend);
                resolved.add(annotation.narrowTo(operatorBackend));
            } else {
                // Fallback: narrow to first viable backend.
                String fallback = annotation.getViableBackends().getFirst();
                logger.info("Annotation [{}] narrowed to fallback backend [{}]", annotation, fallback);
                resolved.add(annotation.narrowTo(fallback));
            }
        }
        return resolved;
    }

    /**
     * Finds a delegation target for this annotation, but only if the annotation's
     * predicate function is in the delegation-preferred set (e.g. EQUALS → TermQuery).
     * Returns null if the function is not delegation-preferred or no suitable target exists.
     */
    private static String findDelegationTarget(OperatorAnnotation annotation, String operatorBackend, CapabilityRegistry registry) {
        ScalarFunction function = extractFunction(annotation);
        if (function == null || DELEGATION_PREFERRED_FUNCTIONS.contains(function) == false) {
            logger.info(
                "findDelegationTarget: function=[{}] not in preferred set, skipping delegation for annotation [{}]",
                function,
                annotation
            );
            return null;
        }
        List<String> acceptors = registry.delegationAcceptors(DelegationType.FILTER);
        logger.info(
            "findDelegationTarget: function=[{}] is delegation-preferred, viable=[{}], acceptors=[{}], operatorBackend=[{}]",
            function,
            annotation.getViableBackends(),
            acceptors,
            operatorBackend
        );
        for (String backend : annotation.getViableBackends()) {
            if (backend.equals(operatorBackend) == false && acceptors.contains(backend)) {
                logger.info("findDelegationTarget: selected delegation target [{}] for function [{}]", backend, function);
                return backend;
            }
        }
        logger.info("findDelegationTarget: no delegation target found for function [{}], falling back to native eval", function);
        return null;
    }

    /**
     * Extracts the ScalarFunction from an annotation's underlying RexCall.
     * Returns null if the annotation is not a predicate or cannot be resolved.
     */
    private static ScalarFunction extractFunction(OperatorAnnotation annotation) {
        if (annotation instanceof AnnotatedPredicate predicate) {
            RexNode original = predicate.getOriginal();
            if (original instanceof RexCall call) {
                return ScalarFunction.fromSqlOperatorWithFallback(call.getOperator());
            }
        }
        return null;
    }
}
