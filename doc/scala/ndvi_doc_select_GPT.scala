import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

import java.net.URI
import scala.util.Try

object NDVIJob {
  def main(args: Array[String]): Unit = {
    val defaultRed = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.tif"

    val redPath = if (args.length > 0) args(0) else defaultRed
    val nirPath = if (args.length > 1) args(1) else defaultNir
    val outPath = if (args.length > 2) args(2) else defaultOut

    val spark = SparkSession.builder().appName("RDPro NDVI").getOrCreate()
    val sc = spark.sparkContext

    try {
      // Detect whether output is local or distributed by URI scheme
      val outUriTry = Try(new URI(outPath))
      val outScheme = outUriTry.map(_.getScheme).getOrElse(null)
      val isLocalOut = outScheme == null || outScheme == "file"
      val fsMode = if (isLocalOut) "local" else s"distributed($outScheme)"
      println(s"[NDVI] Output path detected as $fsMode: $outPath")

      // Load inputs as Int and cast to Float during computation (Landsat SR is typically Int16)
      val red: RasterRDD[Int] = sc.geoTiff[Int](redPath)
      val nir: RasterRDD[Int] = sc.geoTiff[Int](nirPath)

      // Validate that both rasters share identical metadata (grid alignment, CRS, tile size)
      val redMD = red.first().rasterMetadata
      val nirMD = nir.first().rasterMetadata
      if (redMD != nirMD) {
        throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
      }

      // NOTE: NoData handling via dataset metadata is not available in the provided APIs.
      // This implementation ignores input band NoData masks. Denominator==0 is handled.

      // Stack bands and compute NDVI
      val stacked: RasterRDD[Array[Int]] = red.overlay(nir)
      val ndvi: RasterRDD[Float] = stacked.mapPixels[Array[Int], Float]((px: Array[Int]) => {
        val r: Float = px(0).toFloat
        val n: Float = px(1).toFloat
        val denom: Float = n + r
        if (denom == 0.0f) -9999.0f else (n - r) / denom
      })

      // Save as a single GeoTIFF with LZW compression to match the Python intent
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
}

// NOTES:
// (a) RDPro APIs used:
// - geoTiff[T]
// - RasterRDD (alias)
// - ITile
// - RasterMetadata
// - overlay
// - mapPixels
// - saveAsGeoTiff

// (b) Unsupported operations and why:
// - Reading per-band NoData values and masking: Not exposed in the provided DOC CHUNKS, so we cannot replicate GDAL’s GetNoDataValue() semantics. This implementation only guards against division by zero; it does not mask input NoData.
// - Setting NoDataValue on output GeoTIFF: No API is shown to set NoData metadata on output; thus the output uses -9999.0f as a numeric fill value but does not mark it as a NoData tag in the file.
// - Explicit warp/reprojection to align rasters: While a general reshape API exists, the Python requires a fail-fast if grids do not match. We enforce equality of RasterMetadata and throw a clear error if mismatched.

// (c) Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
// - Inputs are single-band rasters and share identical metadata (same grid, CRS, resolution, tile scheme). If not, the job fails fast with a clear message.
// - Inputs are loaded as Int and converted to Float during NDVI computation.
// - NDVI fill value for zero denominator is -9999.0f. Because output NoData tagging is unsupported, this is written as data.
// - Output path mode is inferred via URI scheme; we log whether it appears local or distributed. We always write in "compatibility" mode to produce a single GeoTIFF to match the Python behavior.
// - No path utility beyond standard Java URI parsing is used; no RDPro path helpers are assumed.