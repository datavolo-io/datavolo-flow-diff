/*
 * SPDX-FileCopyrightText: 2024 Datavolo Inc.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.datavolo.nifi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.junit.jupiter.api.Test;

class FlowDiffTest {

    @Test
    void testDiffV1V2() throws IOException {
        String flowV1 = "src/test/resources/flow_v1_initial.json";
        String flowV2 = "src/test/resources/flow_v2_added_component.json";
        Set<FlowDifference> diffs = FlowDiff.getDiff(flowV1, flowV2);
        assertEquals(diffs.size(), 3);
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.POSITION_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.COMPONENT_ADDED)));
    }

    @Test
    void testDiffV2V3() throws IOException {
        String flowV2 = "src/test/resources/flow_v2_added_component.json";
        String flowV3 = "src/test/resources/flow_v3_config_changes.json";
        Set<FlowDifference> diffs = FlowDiff.getDiff(flowV2, flowV3);
        assertEquals(diffs.size(), 13);
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.RUN_SCHEDULE_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.FLOWFILE_EXPIRATION_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.BULLETIN_LEVEL_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.CONCURRENT_TASKS_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.PENALTY_DURATION_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.PARAMETER_CONTEXT_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.BACKPRESSURE_OBJECT_THRESHOLD_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.AUTO_TERMINATED_RELATIONSHIPS_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.LOAD_BALANCE_STRATEGY_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.LOAD_BALANCE_COMPRESSION_CHANGED)));
    }

    @Test
    void testDiffV3V4() throws IOException {
        String flowV3 = "src/test/resources/flow_v3_config_changes.json";
        String flowV4 = "src/test/resources/flow_v4_parameters.json";
        Set<FlowDifference> diffs = FlowDiff.getDiff(flowV3, flowV4);
        assertEquals(diffs.size(), 14);
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.INHERITED_CONTEXTS_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.PARAMETER_ADDED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.SCHEDULING_STRATEGY_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.PROPERTY_PARAMETERIZED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.NAME_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.BUNDLE_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.COMPONENT_REMOVED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.RUN_SCHEDULE_CHANGED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.PARAMETER_REMOVED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.DESTINATION_CHANGED)));
    }

    @Test
    void testDiffV4V5() throws IOException {
        String flowV4 = "src/test/resources/flow_v4_parameters.json";
        String flowV5 = "src/test/resources/flow_v5_property_parameter.json";
        Set<FlowDifference> diffs = FlowDiff.getDiff(flowV4, flowV5);
        assertEquals(diffs.size(), 7);
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.PROPERTY_PARAMETERIZATION_REMOVED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.SCHEDULED_STATE_CHANGED)));
    }

    @Test
    void testDiffV3V5() throws IOException {
        String flowV3 = "src/test/resources/flow_v3_config_changes.json";
        String flowV5 = "src/test/resources/flow_v5_property_parameter.json";
        Set<FlowDifference> diffs = FlowDiff.getDiff(flowV3, flowV5);
        assertEquals(diffs.size(), 13);
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.PROPERTY_ADDED)));
    }

    @Test
    void testDiffV5V6() throws IOException {
        String flowV5 = "src/test/resources/flow_v5_property_parameter.json";
        String flowV6 = "src/test/resources/flow_v6_parameter_value.json";
        Set<FlowDifference> diffs = FlowDiff.getDiff(flowV5, flowV6);
        assertEquals(diffs.size(), 3);
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.PROPERTY_REMOVED)));
        assertTrue(diffs.stream().anyMatch(d -> d.getDifferenceType().equals(DifferenceType.PARAMETER_VALUE_CHANGED)));
    }
}
