/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.delete;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.base.io.ByteBuffers.getWrappedBytes;
import static java.util.Objects.requireNonNull;

public final class DeleteFile
{
    private static final long INSTANCE_SIZE = instanceSize(DeleteFile.class);

    private final FileContent content;
    private final String path;
    private final FileFormat format;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final List<Integer> equalityFieldIds;
    private final Map<Integer, byte[]> lowerBounds;
    private final Map<Integer, byte[]> upperBounds;
    private final long dataSequenceNumber;

    public static DeleteFile fromIceberg(org.apache.iceberg.DeleteFile deleteFile)
    {
        Map<Integer, byte[]> lowerBounds = firstNonNull(deleteFile.lowerBounds(), ImmutableMap.<Integer, ByteBuffer>of())
                .entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> getWrappedBytes(entry.getValue()).clone()));
        Map<Integer, byte[]> upperBounds = firstNonNull(deleteFile.upperBounds(), ImmutableMap.<Integer, ByteBuffer>of())
                .entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> getWrappedBytes(entry.getValue()).clone()));

        return new DeleteFile(
                deleteFile.content(),
                deleteFile.path().toString(),
                deleteFile.format(),
                deleteFile.recordCount(),
                deleteFile.fileSizeInBytes(),
                Optional.ofNullable(deleteFile.equalityFieldIds()).orElseGet(ImmutableList::of),
                lowerBounds,
                upperBounds,
                deleteFile.dataSequenceNumber());
    }

    @JsonCreator
    public DeleteFile(
            FileContent content,
            String path,
            FileFormat format,
            long recordCount,
            long fileSizeInBytes,
            List<Integer> equalityFieldIds,
            Map<Integer, byte[]> lowerBounds,
            Map<Integer, byte[]> upperBounds,
            long dataSequenceNumber)
    {
        this.content = requireNonNull(content, "content is null");
        this.path = requireNonNull(path, "path is null");
        this.format = requireNonNull(format, "format is null");
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.equalityFieldIds = ImmutableList.copyOf(requireNonNull(equalityFieldIds, "equalityFieldIds is null"));
        this.lowerBounds = ImmutableMap.copyOf(requireNonNull(lowerBounds, "lowerBounds is null"));
        this.upperBounds = ImmutableMap.copyOf(requireNonNull(upperBounds, "upperBounds is null"));
        this.dataSequenceNumber = dataSequenceNumber;
    }

    @JsonProperty
    public FileContent content()
    {
        return content;
    }

    @JsonProperty
    public String path()
    {
        return path;
    }

    @JsonProperty
    public FileFormat format()
    {
        return format;
    }

    @JsonProperty
    public long recordCount()
    {
        return recordCount;
    }

    @JsonProperty
    public long fileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    @JsonProperty
    public List<Integer> equalityFieldIds()
    {
        return equalityFieldIds;
    }

    @JsonProperty
    public Map<Integer, byte[]> getLowerBounds()
    {
        return lowerBounds;
    }

    @JsonProperty
    public Map<Integer, byte[]> getUpperBounds()
    {
        return upperBounds;
    }

    @JsonProperty
    public long getDataSequenceNumber()
    {
        return dataSequenceNumber;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(equalityFieldIds, _ -> SIZE_OF_INT)
                + estimatedSizeOf(lowerBounds, entry -> SIZE_OF_INT, SizeOf::sizeOf)
                + estimatedSizeOf(upperBounds, entry -> SIZE_OF_INT, SizeOf::sizeOf);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .add("records", recordCount)
                .toString();
    }
}
