/*
 * Copyright 2017 Tair Sabyrgaliyev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tair.zip;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipperInputStreamTest {

    byte[] file1data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".getBytes();
    byte[] file2data = "other bytes".getBytes();

    ZipperInputStream.ZipEntryData file1 = new ZipperInputStream.ZipEntryData() {
        @Override
        public String getPath() {
            return "file1";
        }

        @Override
        public InputStream getStream() {
            return new ByteArrayInputStream(file1data);
        }
    };

    ZipperInputStream.ZipEntryData file2 = new ZipperInputStream.ZipEntryData() {
        @Override
        public String getPath() {
            return "folder1/file2";
        }

        @Override
        public InputStream getStream() {
            return new ByteArrayInputStream(file2data);
        }
    };

    static Enumeration<ZipperInputStream.ZipEntryData> enumerate (ZipperInputStream.ZipEntryData... files) {
        return Collections.enumeration(Arrays.asList(files));
    }

    public void testJDKCompatibility() throws IOException {
        ZipperInputStream lzis = new ZipperInputStream(enumerate(file1, file2));

        ZipInputStream zis = new ZipInputStream(lzis);

        {
            ZipEntry entry1 = zis.getNextEntry();

            assert file1.getPath().equals(entry1.getName());

            assert IOUtils.contentEquals(zis, file1.getStream());

            zis.closeEntry();
        }

        {
            ZipEntry entry2 = zis.getNextEntry();

            assert file2.getPath().equals(entry2.getName());

            assert IOUtils.contentEquals(zis, file2.getStream());

            zis.closeEntry();
        }




    }

    public void testFileSystemOutput() throws IOException {
        ZipperInputStream lzis = new ZipperInputStream(enumerate(file1, file2));

        Files.copy(lzis, Paths.get("target", "output.zip"), StandardCopyOption.REPLACE_EXISTING);
    }
}
