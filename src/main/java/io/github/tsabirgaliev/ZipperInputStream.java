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
package io.github.tsabirgaliev;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Consumer;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;

/**
 * ZipperInputStream lets you lazily provide file names and data streams
 * in spirit of java.util.zip.DeflaterInputStream.
 */
public class ZipperInputStream extends SequenceInputStream {

    public interface ZipEntryData {
        String getPath();
        InputStream getStream();
    }

    public ZipperInputStream(Enumeration<ZipEntryData> enumeration) throws IOException {
        super(new Enumeration<InputStream>() {
            List<CentralDirectory.FileEntry> fileEntries = new ArrayList<>();

            boolean cdProcessed = false;

            @Override
            public boolean hasMoreElements() {
                return !cdProcessed;
            }

            @Override
            public InputStream nextElement() {
                try {
                    if (enumeration.hasMoreElements()) {
                        ZipEntryData zipEntryData = enumeration.nextElement();
                        LocalFileHeader lfh = new LocalFileHeader(zipEntryData.getPath());
                        ByteArrayInputStream lfhIn = new ByteArrayInputStream(lfh.getBytes());
                        DeflaterDDInputStream dddIn = new DeflaterDDInputStream(zipEntryData.getStream(), dd -> {
                            fileEntries.add(new CentralDirectory.FileEntry(lfh, dd));
                        });

                        return new SequenceInputStream(Collections.enumeration(Arrays.asList(lfhIn, dddIn)));
                    } else if (!cdProcessed) {
                        cdProcessed = true;
                        CentralDirectory cd = new CentralDirectory(fileEntries);
                        return new ByteArrayInputStream(cd.getBytes());
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Error processing zip entry data", e);
                }

                throw new NoSuchElementException("No more elements to produce!");
            }
        });
    }

    static byte[] bytes2(long i) {
        return new byte[] {
                (byte)(i >> 0),
                (byte)(i >> 8)
        };
    }

    static byte[] bytes4(long i) {
        return new byte[] {
                (byte)(i >> 0),
                (byte)(i >> 8),
                (byte)(i >> 16),
                (byte)(i >> 24)
        };
    }

    static class LocalFileHeader {
        long signature = 0x04034b50;

        byte[] version = {20, 0};
        long flags     = (1 << 3)   // DataDescriptor used
                       | (1 << 11); // file_name is UTF-8

        long compression_method = 8; // DEFLATE

        static byte[] currentDosTime(Calendar cal) {
            int result = (cal.get(Calendar.HOUR_OF_DAY) << 11)
                       | (cal.get(Calendar.MINUTE)      << 5)
                       | (cal.get(Calendar.SECOND)      / 2);

            return new byte[] {
                    (byte)(result >> 0),
                    (byte)(result >> 8)
            };
        }

        static byte[] currentDosDate(Calendar cal) {
            int result = ((cal.get(Calendar.YEAR) - 1980) << 9)
                       | ((cal.get(Calendar.MONTH) + 1)   << 5)
                       | cal.get(Calendar.DATE);

            return new byte[] {
                    (byte)(result >> 0),
                    (byte)(result >> 8)
            };
        }

        byte[] modification_time = currentDosTime(Calendar.getInstance())
        , modification_date = currentDosDate(Calendar.getInstance())
        ;

        final long crc32_checksum = 0
        , compressed_size   = 0
        , uncompressed_size = 0
        ;

        long extra_field_length = 0;
        byte[] file_name = {};

        public LocalFileHeader(String filename) {
            file_name = filename.getBytes(Charset.forName("UTF-8"));
        }

        public byte[] getBytes() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            baos.write(bytes4(signature));
            baos.write(version);
            baos.write(bytes2(flags));
            baos.write(bytes2(compression_method));
            baos.write(modification_time);
            baos.write(modification_date);
            baos.write(bytes4(crc32_checksum));
            baos.write(bytes4(compressed_size));
            baos.write(bytes4(uncompressed_size));

            baos.write(bytes2(file_name.length));

            baos.write(bytes2(extra_field_length));
            baos.write(file_name);

            baos.close();

            return baos.toByteArray();
        }
    }

    static class DataDescriptor {
        long signature = 0x08074b50;
        long crc32_checksum;
        long compressed_size;
        long uncompressed_size;

        public DataDescriptor(long crc32_checksum, long compressed_size, long uncompressed_size) {
            this.crc32_checksum = crc32_checksum;
            this.compressed_size = compressed_size;
            this.uncompressed_size = uncompressed_size;
        }

        public byte[] getBytes() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            baos.write(bytes4(signature));
            baos.write(bytes4(crc32_checksum));
            baos.write(bytes4(compressed_size));
            baos.write(bytes4(uncompressed_size));

            baos.close();

            return baos.toByteArray();
        }
    }

    static class DeflaterCheckedInputStream extends FilterInputStream {
        static class CountingCRC32 implements Checksum {
            CRC32 checksum = new CRC32();
            long counter = 0;

            @Override
            public void update(int b) {
                this.checksum.update(b);
                this.counter++;
            }

            @Override
            public void update(byte[] b, int off, int len) {
                this.checksum.update(b, off, len);
                this.counter += len;
            }

            @Override
            public long getValue() {
                return this.checksum.getValue();
            }

            @Override
            public void reset() {
                this.checksum.reset();
            }

            public long getCounter() {
                return counter;
            }
        }

        long compressedSize = 0;
        CountingCRC32 checksum = new CountingCRC32();

        DeflaterCheckedInputStream(InputStream in) {
            super(null);
            CheckedInputStream checkedIn = new CheckedInputStream(in, checksum);
            DeflaterInputStream deflateIn = new DeflaterInputStream(checkedIn, new Deflater(Deflater.DEFAULT_COMPRESSION, true));
            this.in = deflateIn;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1) compressedSize++;

            return b;
        }

        @Override
        public int read(byte[] b) throws IOException {
            int c = super.read(b);
            if (c != -1) compressedSize += c;

            return c;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int c = super.read(b, off, len);
            if (c != -1) compressedSize += c;

            return c;
        }

        public DataDescriptor getDataDescriptor() {
            return new DataDescriptor(checksum.getValue(), compressedSize, checksum.getCounter());
        }
    }

    static class DeflaterDDInputStream extends InputStream {
        private final Consumer<DataDescriptor> ddConsumer;
        DeflaterCheckedInputStream in;

        boolean inExhausted = false;

        InputStream ddIn;

        DeflaterDDInputStream(InputStream in, Consumer<DataDescriptor> ddConsumer) {
            this.in = new DeflaterCheckedInputStream(in);
            this.ddConsumer = ddConsumer;
        }


        @Override
        public int read() throws IOException {
            int b = -1;
            if (!this.inExhausted) {
                b = this.in.read();

                if (b == -1) {
                    this.in.close();
                    this.inExhausted = true;
                    DataDescriptor dd = this.in.getDataDescriptor();
                    ddConsumer.accept(dd);
                    this.ddIn = new ByteArrayInputStream(dd.getBytes());
                    b = this.ddIn.read();
                }
            } else {
                b = this.ddIn.read();
            }

            return b;
        }
    }

    static class CentralDirectory {
        static class FileEntry {
            LocalFileHeader lfh;
            DataDescriptor dd;

            FileEntry(LocalFileHeader lfh, DataDescriptor dd) {
                this.lfh = lfh;
                this.dd = dd;
            }
        }

        static class FileHeader {
            long signature = 0x02014b50;
            long file_comment_length = 0;
            long disk_num_start = 0;
            long internal_attrs = 0;
            byte[] external_attrs = {0x0, 0x0, (byte)0xa4, (byte)0x81};
            long local_header_offset = 0;

            LocalFileHeader lfh;

            public FileHeader(LocalFileHeader lfh, long lfh_offset) {
                this.lfh = lfh;
                this.local_header_offset = lfh_offset;
            }

            public byte[] getBytes() throws IOException {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                baos.write(bytes4(signature));
                baos.write(lfh.version); // version made by
                baos.write(lfh.version); // version needed to extract
                baos.write(bytes2(lfh.flags));
                baos.write(bytes2(lfh.compression_method));
                baos.write(lfh.modification_time);
                baos.write(lfh.modification_date);
                baos.write(bytes4(lfh.crc32_checksum));
                baos.write(bytes4(lfh.compressed_size));
                baos.write(bytes4(lfh.uncompressed_size));
                baos.write(bytes2(lfh.file_name.length));
                baos.write(bytes2(lfh.extra_field_length));
                baos.write(bytes2(file_comment_length));
                baos.write(bytes2(disk_num_start));
                baos.write(bytes2(internal_attrs));
                baos.write(external_attrs);
                baos.write(bytes4(local_header_offset));
                baos.write(lfh.file_name);

                baos.close();

                return baos.toByteArray();
            }
        }

        static class End {
            long signature = 0x06054b50;
            long disk_number = 0;
            long disk_number_with_cd = 0;
            long disk_entries = 1;
            long total_entries = 1;
            long cd_size = 0x33;
            long cd_offset = 0x23;
            long comment_length = 0;

            End(long entries, long cd_size, long cd_offset) {
                this.disk_entries = this.total_entries = entries;
                this.cd_size = cd_size;
                this.cd_offset = cd_offset;
            }

            public byte[] getBytes() throws IOException {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                baos.write(bytes4(signature));
                baos.write(bytes2(disk_number));
                baos.write(bytes2(disk_number_with_cd));
                baos.write(bytes2(disk_entries));
                baos.write(bytes2(total_entries));
                baos.write(bytes4(cd_size));
                baos.write(bytes4(cd_offset));
                baos.write(bytes2(comment_length));

                baos.close();

                return baos.toByteArray();
            }
        }

        List<FileHeader> headers;
        End end;

        public CentralDirectory(List<FileEntry> entries) throws IOException {
            long offset = 0;
            long cd_size = 0;

            headers = new ArrayList<>(entries.size());

            for (FileEntry fileEntry : entries) {
                FileHeader fh = new FileHeader(fileEntry.lfh, offset);
                offset += fileEntry.lfh.getBytes().length + fileEntry.dd.compressed_size + fileEntry.dd.getBytes().length;
                cd_size += fh.getBytes().length;
                headers.add(fh);
            }

            end = new End(headers.size(), cd_size, offset);

        }

        public byte[] getBytes() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            for(FileHeader fh : headers) {
                baos.write(fh.getBytes());
            }

            baos.write(end.getBytes());

            baos.close();

            return baos.toByteArray();
        }
    }

}
