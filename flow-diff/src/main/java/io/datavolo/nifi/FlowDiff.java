/*
 * SPDX-FileCopyrightText: 2024 Datavolo Inc.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.datavolo.nifi;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.diff.ConciseEvolvingDifferenceDescriptor;
import org.apache.nifi.registry.flow.diff.FlowComparator;
import org.apache.nifi.registry.flow.diff.FlowComparatorVersionedStrategy;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.diff.StandardComparableDataFlow;
import org.apache.nifi.registry.flow.diff.StandardFlowComparator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FlowDiff {
    
    private static String flowName;
    private static Map<String, VersionedParameterContext> parameterContexts;

    public static void main(String[] args) throws IOException {

        String pathA = args[0];
        String pathB = args[1];
        
        final Set<FlowDifference> diffs = getDiff(pathA, pathB);

        System.out.println("> [!NOTE]");
        System.out.println("> # ![datavolo.io](https://docs.datavolo.io/img/logo-without-name.svg) Datavolo NiFi Flow Diff");
        System.out.println("> This GitHub Action is created and maintained by [Datavolo](https://datavolo.io/).");
        System.out.println("### Executing Datavolo Flow Diff for flow: `" + flowName + "`");

        for(FlowDifference diff : diffs) {

            switch (diff.getDifferenceType()) {
            case COMPONENT_ADDED: {
                if (diff.getComponentB().getComponentType().equals(ComponentType.FUNNEL)) {
                    System.out.println("- A Funnel has been added");
                } else if (diff.getComponentB().getComponentType().equals(ComponentType.CONNECTION)) {
                    final VersionedConnection connection = (VersionedConnection) diff.getComponentB();
                    if (connection.getSource().getId().equals(connection.getDestination().getId())) {
                        System.out.println("- A self-loop connection `"
                                + (isEmpty(connection.getName()) ? connection.getSelectedRelationships().toString() : connection.getName())
                                + "` has been added on `" + connection.getSource().getName() + "`");
                    } else {
                        System.out.println("- A connection `"
                                + (isEmpty(connection.getName()) ? connection.getSelectedRelationships().toString() : connection.getName())
                                + "` from `" + connection.getSource().getName() + "` to `" + connection.getDestination().getName()
                                + "` has been added");
                    }
                } else if (diff.getComponentB().getComponentType().equals(ComponentType.PROCESSOR)) {
                    final VersionedProcessor proc = (VersionedProcessor) diff.getComponentB();
                    System.out.println("- A Processor"
                            + (isEmpty(diff.getComponentB().getName()) ? "" : " `" + diff.getComponentB().getName() + "`")
                            + " has been added with the below configuration");
                    printProcessorProperties(proc);
                } else if (diff.getComponentB().getComponentType().equals(ComponentType.CONTROLLER_SERVICE)) {
                    final VersionedControllerService cs = (VersionedControllerService) diff.getComponentB();
                    System.out.println("- A Controller Service"
                            + (isEmpty(diff.getComponentB().getName()) ? "" : " `" + diff.getComponentB().getName() + "`")
                            + " has been added with the below configuration");
                    printControllerProperties(cs);
                } else {
                    System.out.println("- A " + diff.getComponentB().getComponentType().getTypeName()
                            + (isEmpty(diff.getComponentB().getName()) ? "" : " named `" + diff.getComponentB().getName() + "`")
                            + " has been added");
                }
                break;
            }
            case COMPONENT_REMOVED: {
                if (diff.getComponentA().getComponentType().equals(ComponentType.FUNNEL)) {
                    System.out.println("- A Funnel has been removed");
                } else {
                    System.out.println("- A " + diff.getComponentA().getComponentType().getTypeName()
                            + (isEmpty(diff.getComponentA().getName()) ? "" : " named `" + diff.getComponentA().getName() + "`")
                            + " has been removed");
                }
                break;
            }
            case DESTINATION_CHANGED: {
                System.out.println("- The destination of a connection has changed from `" + ((ConnectableComponent) diff.getValueA()).getName()
                        + "` to `" + ((ConnectableComponent) diff.getValueB()).getName() + "`");
                break;
            }
            case PROPERTY_CHANGED: {
                System.out.println("- In the " + diff.getComponentA().getComponentType().getTypeName()
                        + " named `" + diff.getComponentA().getName() + "`, the value of the property "
                        + "`" + diff.getFieldName().get() + "` changed from `" + diff.getValueA()
                        + "` to `" + diff.getValueB() + "`");
                break;
            }
            case CONCURRENT_TASKS_CHANGED: {
                System.out.println("- In processor `" + diff.getComponentA().getName() + "`, the number of concurrent tasks has been "
                        + ((int) diff.getValueA() > (int) diff.getValueB() ? "decreased" : "increased")
                        + " from `" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED: {
                final VersionedConnection connection = (VersionedConnection) diff.getComponentA();
                System.out.println("- The data size backpressure threshold for the connection `"
                        + (isEmpty(connection.getName()) ? connection.getSelectedRelationships().toString() : connection.getName())
                        + "` from `" + connection.getSource().getName() + "` to `" + connection.getDestination().getName()
                        + "` has been changed from `" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case BACKPRESSURE_OBJECT_THRESHOLD_CHANGED: {
                final VersionedConnection connection = (VersionedConnection) diff.getComponentA();
                System.out.println("- The flowfile number backpressure threshold for the connection `"
                        + (isEmpty(connection.getName()) ? connection.getSelectedRelationships().toString() : connection.getName())
                        + "` from `" + connection.getSource().getName() + "` to `" + connection.getDestination().getName()
                        + "` has been changed from `" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case BULLETIN_LEVEL_CHANGED: {
                System.out.println("- In " + diff.getComponentA().getComponentType() + " named `" + diff.getComponentA().getName()
                        + "`, the bulletin level has been changed from `" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case RUN_DURATION_CHANGED: {
                final VersionedProcessor processor = (VersionedProcessor) diff.getComponentA();
                System.out.println("- In processor `" + processor.getName()
                        + "`, the Run Duration changed from "
                        + "`" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case RUN_SCHEDULE_CHANGED: {
                final VersionedProcessor processor = (VersionedProcessor) diff.getComponentA();
                System.out.println("- In processor `" + processor.getName()
                        + "`, the Run Schedule changed from "
                        + "`" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case AUTO_TERMINATED_RELATIONSHIPS_CHANGED: {
                final VersionedProcessor processor = (VersionedProcessor) diff.getComponentA();
                System.out.println("- In processor `" + processor.getName()
                        + "`, the list of auto-terminated relationships changed from "
                        + "`" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case LOAD_BALANCE_STRATEGY_CHANGED: {
                final VersionedConnection connection = (VersionedConnection) diff.getComponentA();
                System.out.println("- The load balancing strategy for the connection `"
                        + (isEmpty(connection.getName()) ? connection.getSelectedRelationships().toString() : connection.getName())
                        + "` from `" + connection.getSource().getName() + "` to `" + connection.getDestination().getName()
                        + "` has been changed from `" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case LOAD_BALANCE_COMPRESSION_CHANGED: {
                final VersionedConnection connection = (VersionedConnection) diff.getComponentA();
                System.out.println("- The load balancing compression for the connection `"
                        + (isEmpty(connection.getName()) ? connection.getSelectedRelationships().toString() : connection.getName())
                        + "` from `" + connection.getSource().getName() + "` to `" + connection.getDestination().getName()
                        + "` has been changed from `" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case FLOWFILE_EXPIRATION_CHANGED: {
                final VersionedConnection connection = (VersionedConnection) diff.getComponentA();
                System.out.println("- The flow file expiration for the connection `"
                        + (isEmpty(connection.getName()) ? connection.getSelectedRelationships().toString() : connection.getName())
                        + "` from `" + connection.getSource().getName() + "` to `" + connection.getDestination().getName()
                        + "` has been changed from `" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case PENALTY_DURATION_CHANGED: {
                final VersionedProcessor processor = (VersionedProcessor) diff.getComponentA();
                System.out.println("- In processor `" + processor.getName()
                        + "`, the penalty duration changed from "
                        + "`" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case PARAMETER_CONTEXT_CHANGED: {
                final VersionedProcessGroup pg = (VersionedProcessGroup) diff.getComponentB();
                System.out.println("- The parameter context `" + pg.getParameterContextName() + "` with parameters `"
                        + printParameterContext(parameterContexts.get(pg.getParameterContextName()))
                        + "` has been added to the process group `" + pg.getName() + "`");
                break;
            }
            case POSITION_CHANGED: {
                System.out.println("- A " + diff.getComponentA().getComponentType()
                        + (isEmpty(diff.getComponentA().getName()) ? "" : " named `" + diff.getComponentA().getName() + "`")
                        + " has been moved to another position");
                break;
            }
            case SCHEDULING_STRATEGY_CHANGED: {
                final VersionedProcessor processor = (VersionedProcessor) diff.getComponentA();
                System.out.println("- In processor named `" + processor.getName()
                        + "`, the Scheduling Strategy changed from "
                        + "`" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case BUNDLE_CHANGED:
                Bundle before = (Bundle) diff.getValueA();
                Bundle after = (Bundle) diff.getValueB();
                System.out.println("- The bundle `"
                        + before.getGroup() + ":" + before.getArtifact()
                        + "` has been changed from version "
                        + "`" + before.getVersion() + "` to version `" + after.getVersion() + "`");
                break;
            case NAME_CHANGED: {
                System.out.println("- A "
                        + diff.getComponentA().getComponentType()
                        + " has been renamed from "
                        + "`" + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            }
            case PROPERTY_ADDED: {
                final String propKey = (String) diff.getValueB();
                String propValue = null;
                if (diff.getComponentB() instanceof VersionedProcessor) {
                    if (((VersionedProcessor) diff.getComponentB()).getPropertyDescriptors().get(propKey).isSensitive()) {
                        propValue = "<Sensitive Value>";
                    } else {
                        propValue = ((VersionedProcessor) diff.getComponentB()).getProperties().get(propKey);
                    }
                }
                if (diff.getComponentB() instanceof VersionedControllerService) {
                    if (((VersionedControllerService) diff.getComponentB()).getPropertyDescriptors().get(propKey).isSensitive()) {
                        propValue = "<Sensitive Value>";
                    } else {
                        propValue = ((VersionedControllerService) diff.getComponentB()).getProperties().get(propKey);
                    }
                }
                System.out.println("- In " + diff.getComponentA().getComponentType()
                        + " named `" + diff.getComponentA().getName() + "`, a property has been added: "
                        + "`" + propKey + "` = `" + propValue + "`");
                break;
            }
            case PROPERTY_PARAMETERIZED: {
                final String propKey = diff.getFieldName().get();
                String propValue = null;
                if (diff.getComponentB() instanceof VersionedProcessor) {
                    propValue = ((VersionedProcessor) diff.getComponentB()).getProperties().get(propKey);
                }
                if (diff.getComponentB() instanceof VersionedControllerService) {
                    propValue = ((VersionedControllerService) diff.getComponentB()).getProperties().get(propKey);
                }
                System.out.println("- In " + diff.getComponentA().getComponentType()
                        + " named `" + diff.getComponentA().getName() + "`, a property is now referencing a parameter: "
                        + "`" + propKey + "` = `" + propValue + "`");
                break;
            }
            case PROPERTY_PARAMETERIZATION_REMOVED: {
                final String propKey = diff.getFieldName().get();
                System.out.println("- In " + diff.getComponentA().getComponentType()
                        + " named `" + diff.getComponentA().getName()
                        + "`, the property `" + propKey + "` is no longer referencing a parameter");
                break;
            }
            case SCHEDULED_STATE_CHANGED: {
                System.out.println("- In the " + diff.getComponentA().getComponentType().getTypeName()
                        + " named `" + diff.getComponentA().getName()
                        + "`, the Schedule State changed from `"
                        + diff.getValueA() + "` to `" + diff.getValueB() + "`");
                break;
            } 
            case PARAMETER_ADDED: {
                final String paramKey = diff.getFieldName().get();
                final VersionedParameterContext pc = (VersionedParameterContext) diff.getComponentB();
                final VersionedParameter param = pc.getParameters().stream().filter(p -> p.getName().equals(paramKey)).findFirst().get();
                System.out.println("- In the Parameter Context `" + pc.getName() + "` a parameter has been added: `"
                        + paramKey + "` = `" + (param.isSensitive() ? "<Sensitive Value>" : param.getValue()) + "`");
                break;
            }
            case PARAMETER_REMOVED: {
                final String paramKey = diff.getFieldName().get();
                final VersionedParameterContext pc = (VersionedParameterContext) diff.getComponentB();
                System.out.println("- In the Parameter Context `"+ pc.getName() + "` the parameter `" + paramKey + "` has been removed");
                break;
            }
            case PROPERTY_REMOVED: {
                final String propKey = diff.getFieldName().get();
                System.out.println("- In "
                        + diff.getComponentA().getComponentType()
                        + " named `" + diff.getComponentA().getName()
                        + "`, the property `" + propKey + "` has been removed");
                break;
            }
            case PARAMETER_VALUE_CHANGED: {
                final String paramKey = diff.getFieldName().get();
                final VersionedParameterContext pcBefore = (VersionedParameterContext) diff.getComponentA();
                final VersionedParameterContext pcAfter = (VersionedParameterContext) diff.getComponentB();
                final VersionedParameter paramBefore = pcBefore.getParameters().stream().filter(p -> p.getName().equals(paramKey)).findFirst().get();
                final VersionedParameter paramAfter = pcAfter.getParameters().stream().filter(p -> p.getName().equals(paramKey)).findFirst().get();
                System.out.println("- In the Parameter Context `" + pcAfter.getName()
                        + "`, the value of the parameter `" + paramKey + "` has changed from `"
                        + (paramBefore.isSensitive() ? "<Sensitive Value>" : paramBefore.getValue()) + "`"
                        + " to `"
                        + (paramAfter.isSensitive() ? "<Sensitive Value>" : paramAfter.getValue()) + "`");
                break;
            }
            case INHERITED_CONTEXTS_CHANGED:
                final VersionedParameterContext pc = (VersionedParameterContext) diff.getComponentA();
                System.out.println("- In the Parameter Context `" + pc.getName()
                + "`, the list of inherited parameter contexts changed from `"
                + diff.getValueA() + "`" + " to `" + diff.getValueB() + "`");
                break;
            case BENDPOINTS_CHANGED:
            	final VersionedConnection connection = (VersionedConnection) diff.getComponentA();
                System.out.println("- The bending points for the connection `"
                        + (isEmpty(connection.getName()) ? connection.getSelectedRelationships().toString() : connection.getName())
                        + "` from `" + connection.getSource().getName() + "` to `" + connection.getDestination().getName()
                        + "` have been changed");
                break;
            default:
                System.out.println("- " + diff.getDescription() + " (" + diff.getDifferenceType() + ")");
                System.out.println("  - " + diff.getValueA());
                System.out.println("  - " + diff.getValueB());
                System.out.println("  - " + diff.getComponentA());
                System.out.println("  - " + diff.getComponentB());
                System.out.println("  - " + diff.getFieldName());
                break;
            }
        }
    }
    
    public static Set<FlowDifference> getDiff(final String pathA, final String pathB) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        final JsonFactory factory = new JsonFactory(objectMapper);
        final FlowSnapshotContainer snapshotA = getFlowContainer(pathA, factory);
        final FlowSnapshotContainer snapshotB = getFlowContainer(pathB, factory);

        // identifier is null for parameter contexts, and we know that names are unique so setting name as id
        snapshotA.getFlowSnapshot().getParameterContexts().values().forEach(pc -> pc.setIdentifier(pc.getName()));
        snapshotB.getFlowSnapshot().getParameterContexts().values().forEach(pc -> pc.setIdentifier(pc.getName()));

        final FlowComparator flowComparator = new StandardFlowComparator(
                new StandardComparableDataFlow(
                        "Flow A",
                        snapshotA.getFlowSnapshot().getFlowContents(),
                        null,
                        null,
                        null,
                        new HashSet<>(snapshotA.getFlowSnapshot().getParameterContexts().values()),
                        null,
                        null
                        ),
                new StandardComparableDataFlow(
                        "Flow B",
                        snapshotB.getFlowSnapshot().getFlowContents(),
                        null,
                        null,
                        null,
                        new HashSet<>(snapshotB.getFlowSnapshot().getParameterContexts().values()),
                        null,
                        null
                        ),
                Collections.emptySet(),
                new ConciseEvolvingDifferenceDescriptor(),
                Function.identity(),
                VersionedComponent::getIdentifier,
                FlowComparatorVersionedStrategy.DEEP
            );
        
        flowName = snapshotA.getFlowSnapshot().getFlow().getName();
        parameterContexts = snapshotB.getFlowSnapshot().getParameterContexts();

        return flowComparator.compare().getDifferences();
    }

    static FlowSnapshotContainer getFlowContainer(final String path, final JsonFactory factory) throws IOException {
        final File snapshotFile = new File(path);
        try (final JsonParser parser = factory.createParser(snapshotFile)) {
            final RegisteredFlowSnapshot snapshot = parser.readValueAs(RegisteredFlowSnapshot.class);
            return new FlowSnapshotContainer(snapshot);
        }
    }

    static String printParameterContext(final VersionedParameterContext pc) {
        final Map<String, String> parameters = new HashMap<>();
        for (VersionedParameter p : pc.getParameters()) {
            if (p.isSensitive()) {
                parameters.put(p.getName(), "<Sensitive Value>");
            } else {
                parameters.put(p.getName(), p.getValue());
            }
        }
        return parameters.toString();
    }

    static void printProcessorProperties(final VersionedProcessor proc) {
        for (String key : proc.getProperties().keySet()) {
            System.out.println("  - `" + key + "` = `" + proc.getProperties().get(key) + "`");
        }
    }

    static void printControllerProperties(final VersionedControllerService cs) {
        for (String key : cs.getProperties().keySet()) {
            System.out.println("  - `" + key + "` = `" + cs.getProperties().get(key) + "`");
        }
    }

    static boolean isEmpty(final String string) {
        return string == null || string.isEmpty();
    }
}
