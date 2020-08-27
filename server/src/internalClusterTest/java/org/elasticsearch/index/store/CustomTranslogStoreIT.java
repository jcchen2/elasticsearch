/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.store;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.translog.CustomTranslogStorePlugin;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.transport.MockTransportService;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class CustomTranslogStoreIT extends ESIntegTestCase {

    private static final Pattern TRANSLOG_FILE_PATTERN = Pattern.compile("^translog-(\\d+)\\.tlog$");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            CustomTranslogStorePlugin.class,
            MockTransportService.TestPlugin.class,
            MockEngineFactoryPlugin.class);
    }

    public void testCustomTranslogStore() throws Exception {
        internalCluster().startNode(Settings.EMPTY);

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", "-1")
            .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true) // never flush - always recover from translog
        ));

        // Index some documents
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setSource("foo", "bar");
        }
        disableTranslogFlush("test");
        indexRandom(false, false, false, Arrays.asList(builders));  // this one

        final Path translogDir = internalCluster().getInstance(IndicesService.class)
            .indexService(resolveIndex("test")).getShard(0).shardPath().resolveTranslog();

        // Restart the single node
        internalCluster().fullRestart();
        client().admin().cluster().prepareHealth().setWaitForYellowStatus().
            setTimeout(new TimeValue(1000, TimeUnit.MILLISECONDS)).setWaitForEvents(Priority.LANGUID).get();

        client().prepareSearch("test").setQuery(matchAllQuery()).get();

        for (Path path: getTranslogs(translogDir)) {
            verifyTranslogEncoding(path);
        }
    }

    /** Disables translog flushing for the specified index */
    private static void disableTranslogFlush(String index) {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB)).build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }

    private static Set<Path> getTranslogs(Path translogDir) throws Exception {
        Set<Path> candidates = new TreeSet<>(); // TreeSet makes sure iteration order is deterministic
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(translogDir)) {
            for (Path item : stream) {
                if (Files.isRegularFile(item) && Files.size(item) > 0) {
                    final String filename = item.getFileName().toString();
                    if (TRANSLOG_FILE_PATTERN.matcher(filename).matches()) {
                        candidates.add(item);
                    }
                }
            }
        }
        assertThat("no translogs found in " + translogDir, candidates, is(not(empty())));
        return candidates;
    }

    private static void verifyTranslogEncoding(Path translogPath) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try (FileChannel fileChannel = FileChannel.open(translogPath, StandardOpenOption.READ)) {
            while (true) {
                buffer.clear();
                int len = fileChannel.read(buffer);
                if (len < 0) {
                    break;
                }
                for (int i=0; i<buffer.position(); i++) {
                    byte b = buffer.get(i);
                    assertTrue("byte value " + b + " is not hex", (b>='0' && b<='9') || (b>='a' && b<='f'));
                }
            }
        }
    }
}
