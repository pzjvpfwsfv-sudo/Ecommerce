package com.ecommerce.quality.real;

import static org.junit.jupiter.api.Assertions.*;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class RealDataQualityJobTest {
    @Test
    void buildsNewTopologyWithoutConnectingOrReplacingOldEntry() {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        RealDataQualityJob.build(env, RealJobConfig.fromArgs(RealJobConfigTest.args()));
        assertEquals(1, env.getParallelism());
        assertEquals(10000, env.getCheckpointConfig().getCheckpointInterval());
        assertEquals(60000, env.getCheckpointConfig().getCheckpointTimeout());
        assertEquals(1, env.getCheckpointConfig().getMaxConcurrentCheckpoints());
        assertEquals(CheckpointingMode.EXACTLY_ONCE, env.getCheckpointConfig().getCheckpointingMode());
        var nodes = env.getStreamGraph().getStreamNodes();
        var names = nodes.stream().map(n -> n.getOperatorName()).collect(Collectors.joining(" "));
        for (String name : new String[] {"real-source", "real-parse", "real-deduplicate", "real-route",
                "real-clean-sink", "real-dlq-sink", "real-late-sink"}) assertTrue(names.contains(name), name);
        var expectedUids = new HashSet<String>();
        for (String suffix : List.of("source", "parse", "deduplicate", "event-time", "route",
                "clean-sink", "dlq-sink", "late-sink")) expectedUids.add("g2b-verify-01-" + suffix);
        for (String kind : List.of("clean", "dlq", "late")) {
            expectedUids.add("Sink Committer: g2b-verify-01-" + kind + "-sink");
        }
        assertEquals(expectedUids, nodes.stream().map(n -> n.getTransformationUID()).collect(Collectors.toSet()));
        assertEquals(expectedUids.size(), nodes.size());
    }
}
