package io.contextgraph.ingestion;
import java.nio.file.*;
import java.util.Arrays;
/** Only self-contained PNG/JPEG byte streams reach ImageIO metadata readers. */
final class ImageMetadata {
    private static final byte[] PNG={(byte)137,80,78,71,13,10,26,10};
    static void requireSupportedSignature(Path path) throws Exception {
        byte[] signature;
        try(var input=Files.newInputStream(path)) { signature=input.readNBytes(8); }
        boolean png=Arrays.equals(signature,PNG);
        boolean jpeg=signature.length>=3 && signature[0]==(byte)255 && signature[1]==(byte)216 && signature[2]==(byte)255;
        if(!png && !jpeg) throw new IllegalArgumentException("Only self-contained PNG and JPEG images are supported");
    }
}
