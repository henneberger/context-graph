package io.contextgraph.ingestion;

import java.nio.file.*;
import java.util.*;

public final class VideoSegments {
    public record Segment(int sequence, String filename, double duration) {}
    public static List<String> command(Path folder, int seconds) {
        return List.of("ffmpeg", "-hide_banner", "-loglevel", "warning", "-protocol_whitelist", "pipe", "-i", "pipe:0", "-map", "0:v:0", "-map", "0:a:0?",
                "-vf", "fps=30", "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency", "-pix_fmt", "yuv420p",
                "-g", Integer.toString(seconds * 30), "-keyint_min", Integer.toString(seconds * 30), "-sc_threshold", "0",
                "-force_key_frames", "expr:gte(t,n_forced*" + seconds + ")", "-c:a", "aac", "-f", "hls", "-hls_time", Integer.toString(seconds),
                "-hls_list_size", "0", "-hls_playlist_type", "event", "-hls_flags", "independent_segments+temp_file",
                "-hls_segment_filename", folder.resolve("chunk-%08d.ts").toString(), folder.resolve("index.m3u8").toString());
    }
    public static List<Segment> read(Path playlist) throws Exception {
        if (!Files.exists(playlist)) return List.of();
        var result = new ArrayList<Segment>();
        double duration = 0; int sequence = 0;
        for (String line : Files.readAllLines(playlist)) {
            if (line.startsWith("#EXT-X-MEDIA-SEQUENCE:")) sequence = Integer.parseInt(line.substring(22));
            else if (line.startsWith("#EXTINF:")) duration = Double.parseDouble(line.substring(8).split(",")[0]);
            else if (!line.isBlank() && !line.startsWith("#")) {
                if (!line.matches("chunk-[0-9]+\\.ts") || duration <= 0) throw new IllegalArgumentException("Invalid HLS segment");
                result.add(new Segment(sequence++, line, duration)); duration = 0;
            }
        }
        return result;
    }
}
