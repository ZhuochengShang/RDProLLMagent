import java.net.URI
import java.nio.file.{Paths, Path}

import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

object ComputeNDVI {
  def run(sc: SparkContext): Unit = {
    // Defaults from the provided Python script
    val defaultRedPath =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNirPath =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOutPath =
      "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/output/ndvi.tif"

    // Read paths from Spark configuration with safe defaults
    val redPathInRaw  = sc.getConf.get("rdpro.ndvi.red", defaultRedPath)
    val nirPathInRaw  = sc.getConf.get("rdpro.ndvi.nir", defaultNirPath)
    val outPathRaw    = sc.getConf.get("rdpro.ndvi.out", defaultOutPath)

    // Normalize paths according to the required rules
    val redPath = normalizePath(redPathInRaw, sc)
    val nirPath = normalizePath(nirPathInRaw, sc)
    val outPath = normalizePath(outPathRaw, sc)

    // Load rasters (assume integer reflectance inputs)
    val red: RasterRDD[Int] = sc.geoTiff[Int](redPath)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](nirPath)

    // Stack bands: [red, nir]
    val stacked: RasterRDD[Array[Int]] = red.overlay(nir)

    // Compute NDVI = (NIR - RED) / (NIR + RED); guard denom == 0
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
      val r: Float = px(0).toFloat
      val n: Float = px(1).toFloat
      val denom: Float = n + r
      val out: Float = if (denom == 0.0f) -9999.0f else (n - r) / denom
      out
    })

    // Save output GeoTIFF (single-file compatibility mode + LZW compression)
    ndvi.saveAsGeoTiff(
      outPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode   -> "compatibility"
      )
    )

    println(s"NDVI written to: $outPath")
  }

  private def hasScheme(p: String): Boolean = {
    try {
      val u = new URI(p)
      u.getScheme != null
    } catch {
      case _: Throwable => false
    }
  }

  // Filesystem & path normalization as mandated
  private def normalizePath(p: String, sc: SparkContext): String = {
    if (hasScheme(p)) {
      p
    } else {
      val isLocal = sc.master != null && sc.master.toLowerCase.startsWith("local")
      val path: Path = Paths.get(p)
      if (isLocal && path.isAbsolute) {
        path.toAbsolutePath.toUri.toString
      } else {
        // Leave as-is for cluster filesystems (or relative paths in local mode)
        p
      }
    }
  }
}

// NOTES
// (a) RDPro APIs used:
// - sc.geoTiff
// - overlay
// - mapPixels
// - saveAsGeoTiff
// - GeoTiffWriter (Compression, WriteMode)
// - TiffConstants (COMPRESSION_LZW)

// (b) Unsupported operations and why:
// - Explicit grid alignment check (geotransform/projection comparison) is not implemented because no API for reading RasterMetadata fields is provided in the DOC CHUNKS. The overlay operation requires compatible rasters; mismatches will surface at execution.
// - Input NoData handling (reading band-level NoData and masking) is not implemented because the DOC CHUNKS do not expose APIs to read NoData values or apply masks. The code only guards division by zero and emits -9999.0f; it does not set an output NoData tag since no such writer option is documented.
// - Reprojection/warping is not performed; if inputs are misaligned, users should preprocess accordingly.

// (c) Assumptions about IO paths / bands / nodata / CRS / environment detection logic:
// - Inputs are single-band reflectance rasters where red is B4 and nir is B5; both are loaded as Int and computed as Float NDVI.
// - Rasters share CRS, resolution, and tiling so overlay succeeds; otherwise, the job may fail during execution.
// - Output is a single GeoTIFF (compatibility mode) compressed with LZW. No explicit NoData tag is set in the output.
// - Path normalization: if sc.master starts with "local" and a path is absolute without a scheme, it is converted to a file:/// URI; otherwise, scheme-less paths are left unchanged to resolve via the cluster filesystem (fs.defaultFS).

val _r = ComputeNDVI.run(sc)
println("__DONE__")
System.exit(0)
