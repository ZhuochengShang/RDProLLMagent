import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.raptor.RasterOperationsFocal

import java.net.URI
import java.nio.file.Paths
import scala.util.Try

object NDVIJob {
  def main(args: Array[String]): Unit = {
    val defaultRed =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.tif"

    val redPath = if (args.length >= 1) args(0) else defaultRed
    val nirPath = if (args.length >= 2) args(1) else defaultNir
    val outPathRaw = if (args.length >= 3) args(2) else defaultOut

    val spark = SparkSession.builder()
      .appName("NDVIJob")
      .getOrCreate()

    try {
      val sc = spark.sparkContext

      // Normalize output path based on URI scheme and Spark/Hadoop configuration
      val outPath = normalizeOutputPath(outPathRaw, spark)

      // Load Red and NIR as Float rasters (perform a probe to choose the correct loader)
      val redFloat = loadGeoTiffAsFloat(sc, redPath)
      val nirFloat = loadGeoTiffAsFloat(sc, nirPath)

      // Get Red metadata and reshape NIR to match Red (ensures alignment like the Python warp requirement)
      val redMeta = redFloat.first().rasterMetadata
      val nirAligned = RasterOperationsFocal.reshapeNN(nirFloat, redMeta)

      // Stack Red and NIR and compute NDVI
      val stacked = redFloat.overlay(nirAligned)
      val ndvi = stacked.mapPixels((px: Array[Float]) => {
        val r: Float = px(0)
        val n: Float = px(1)
        val denom: Float = n + r
        if (denom == 0.0f) -9999.0f else (n - r) / denom
      })

      // Save as single-file GeoTIFF with LZW compression (mirrors Python's single-file output)
      ndvi.saveAsGeoTiff(
        outPath,
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility"
      )

      println(s"NDVI written to: $outPath")
    } finally {
      spark.stop()
    }
  }

  // Helper: Detect pixel type and load as Float RasterRDD
  private def loadGeoTiffAsFloat(sc: SparkContext, path: String): RasterRDD[Float] = {
    // Probe pixel type
    val probe = sc.geoTiff(path)
    val pixelType = probe.first().pixelType.toString

    pixelType match {
      case "FloatType" =>
        sc.geoTiff[Float](path)
      case "IntegerType" =>
        val asInt = sc.geoTiff[Int](path)
        asInt.mapPixels((v: Int) => v.toFloat)
      case other =>
        throw new RuntimeException(s"Unsupported pixel type $other for input $path. Only IntegerType and FloatType are supported.")
    }
  }

  // Helper: Normalize output path considering URI scheme and Spark/Hadoop defaults
  private def normalizeOutputPath(out: String, spark: SparkSession): String = {
    val uriOpt = Try(new URI(out)).toOption
    val hasScheme = uriOpt.exists(_.getScheme != null)

    if (hasScheme) {
      out
    } else {
      val defaultFS = spark.sparkContext.hadoopConfiguration.get("fs.defaultFS", "file:///")
      val isLocalMaster = spark.sparkContext.master.startsWith("local")
      // If running local or defaultFS is local, resolve to a file:// URI for robustness
      if (isLocalMaster || defaultFS.startsWith("file:")) {
        Paths.get(out).toUri.toString
      } else {
        // Defer to Hadoop default FS (e.g., HDFS/S3) when running on a cluster
        out
      }
    }
  }
}

// NOTES
// (a) RDPro APIs used:
// - SparkContext.geoTiff (typed and untyped)
// - mapPixels
// - overlay
// - saveAsGeoTiff
// - RasterOperationsFocal.reshapeNN

// (b) Unsupported operations and why:
// - Reading input NoData values and propagating them: the DOC does not expose an API to read or set NoData on rasters, so input masking by NoData could not be implemented. Output NoData metadata cannot be set explicitly; saveAsGeoTiff options for NoData are not documented.
// - Explicit equality/introspection of RasterMetadata fields is not fully documented; instead, NIR is unconditionally reshaped to Red metadata to ensure alignment, satisfying the Python requirement to “warp one band first” if grids differ.

// (c) Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
// - Inputs are single-band GeoTIFFs containing either IntegerType or FloatType pixels. If IntegerType, values are converted to Float before NDVI computation.
// - NIR is reshaped (nearest-neighbor) to Red’s RasterMetadata to guarantee alignment for overlay.
// - NDVI computation assigns -9999.0f when denominator is zero; no input NoData masking is applied due to missing API.
// - Output is written as a single GeoTIFF with LZW compression and compatibility mode, to match the Python script’s single-file output.
// - Output path handling: if the path has a URI scheme, it is used as-is; otherwise, if running locally or fs.defaultFS is file://, the path is converted to a file:// URI, else it is left to be resolved by Hadoop’s default filesystem (e.g., HDFS/S3).