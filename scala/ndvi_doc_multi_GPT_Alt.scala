import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import java.net.URI
import java.nio.file.Paths

object NDVI {
  def run(sc: SparkContext): Unit = {
    // Defaults from the Python script
    val defaultRedPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNirPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOutPath = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.py"

    // Allow overriding via SparkConf (e.g., --conf ndvi.red=... --conf ndvi.nir=... --conf ndvi.out=...)
    val conf = sc.getConf
    val redInputRaw = conf.get("ndvi.red", defaultRedPath)
    val nirInputRaw = conf.get("ndvi.nir", defaultNirPath)
    val outPathRaw  = conf.get("ndvi.out", defaultOutPath)

    // Filesystem & Path Normalization
    def hasScheme(p: String): Boolean = {
      try {
        val s = new URI(p).getScheme
        s != null && s.nonEmpty
      } catch {
        case _: Throwable => false
      }
    }
    def isLocal(sc: SparkContext): Boolean =
      sc.master != null && sc.master.toLowerCase.startsWith("local")
    def toFileUri(p: String): String =
      Paths.get(p).toAbsolutePath.toUri.toString
    def isAbsoluteLocalPath(p: String): Boolean =
      new java.io.File(p).isAbsolute
    def normalizePath(p: String): String = {
      if (hasScheme(p)) p
      else if (isLocal(sc) && isAbsoluteLocalPath(p)) toFileUri(p)
      else p
    }

    val redPath = normalizePath(redInputRaw)
    val nirPath = normalizePath(nirInputRaw)
    val outPath = normalizePath(outPathRaw)

    // Read input rasters as Int pixels (typical for Landsat SR)
    val red: RasterRDD[Int] = sc.geoTiff[Int](redPath)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](nirPath)

    // Align/stack rasters into a two-band RasterRDD[Array[Int]]
    // Note: overlay requires matching CRS/resolution/tile layout.
    val stacked: RasterRDD[Array[Int]] = red.overlay(nir)

    // Compute NDVI per pixel:
    // NDVI = (NIR - RED) / (NIR + RED)
    // Handle divide-by-zero by emitting -9999.0f (cannot tag as NODATA with available APIs)
    val ndvi: RasterRDD[Float] = stacked.mapPixels((arr: Array[Int]) => {
      val r: Float = arr(0).toFloat
      val n: Float = arr(1).toFloat
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f
      else (n - r) / denom
    })

    // Write output GeoTIFF with LZW compression
    ndvi.saveAsGeoTiff(
      outPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW
      )
    )
  }
}

// NOTES
// (a) RDPro APIs used:
// - sc.geoTiff
// - overlay
// - mapPixels
// - saveAsGeoTiff

// (b) Unsupported operations and why:
// - Explicit grid alignment check (comparing GeoTransform/Projection) is not implemented because no metadata accessors are shown in the provided docs. The overlay operation is assumed to enforce alignment constraints and will fail if mismatched.
// - Reading per-band NoData values and setting output NoData are not supported by the documented APIs. As a result, NDVI uses -9999.0f for divide-by-zero cases but cannot mark it as NoData in the output metadata.
// - Writing a single-file GeoTIFF vs distributed mode control is not specified beyond general options; default write behavior is used with only compression set.

// (c) Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
// - Input paths are read from SparkConf keys ndvi.red and ndvi.nir; output from ndvi.out. If not provided, defaults from the Python script are used.
// - Path normalization:
//   - If a scheme exists (file:, hdfs:, s3a:, gs:, http:, etc.), it is used as-is.
//   - In local mode (sc.master starts with "local") and for absolute local paths, paths are converted to absolute file:/// URIs.
//   - In non-local mode with scheme-less paths, they are left unchanged to resolve via fs.defaultFS.
// - Rasters are assumed to be aligned (same CRS/resolution/tile layout). If not, overlay is expected to fail at runtime.
// - Pixel input type is Int; arithmetic is promoted to Float in mapPixels.
// - NDVI sentinel for invalid division is -9999.0f but is not flagged as NoData in the output due to missing API in docs.

val _r = NDVI.run(sc)
println("__DONE__")
System.exit(0)
