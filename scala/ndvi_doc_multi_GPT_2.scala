import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants

import java.net.URI
import java.nio.file.{Paths, Path}

object NDVICalculation {
  def run(sc: SparkContext): Unit = {
    // Defaults from the original Python script
    val defaultB4 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultB5 = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/python/ndvi.py"

    // Allow overrides via system properties or environment variables with same names as the Python variables
    def getParam(name: String, defaultValue: String): String =
      sys.props.get(name).orElse(sys.env.get(name)).getOrElse(defaultValue)

    val b4Raw = getParam("B4_PATH", defaultB4)
    val b5Raw = getParam("B5_PATH", defaultB5)
    val outRaw = getParam("OUT_NDVI", defaultOut)

    val b4Path = normalizePath(sc, b4Raw)
    val b5Path = normalizePath(sc, b5Raw)
    val outPath = normalizePath(sc, outRaw)

    // Read input rasters. Use Int input type (common for SR products); cast to Float during pixel math.
    val red: RasterRDD[Int] = sc.geoTiff[Int](b4Path)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](b5Path)

    // Alignment check: require identical metadata (resolution/CRS/tile layout/transform)
    val redMeta = red.first().rasterMetadata
    val nirMeta = nir.first().rasterMetadata

    val metadataMismatch =
      redMeta.x1 != nirMeta.x1 ||
      redMeta.y1 != nirMeta.y1 ||
      redMeta.x2 != nirMeta.x2 ||
      redMeta.y2 != nirMeta.y2 ||
      redMeta.tileWidth != nirMeta.tileWidth ||
      redMeta.tileHeight != nirMeta.tileHeight ||
      redMeta.srid != nirMeta.srid ||
      // Compare grid-to-model transform as well; require exact equality similar to GDAL GeoTransform check
      (if (redMeta.g2m == null) nirMeta.g2m != null else !redMeta.g2m.equals(nirMeta.g2m))

    if (metadataMismatch) {
      throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
    }

    // Stack red and nir
    val stacked: RasterRDD[Array[Int]] = red.overlay(nir)

    // NDVI calculation; treat zero denominator as nodata (-9999f)
    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
      val r: Float = px(0).toFloat
      val n: Float = px(1).toFloat
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f
      else (n - r) / denom
    })

    // Write GeoTIFF with LZW compression in compatibility mode (single file)
    ndvi.saveAsGeoTiff(outPath,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode -> "compatibility"
      )
    )

    println(s"NDVI written to: $outPath")
  }

  // Filesystem & path normalization per requirements
  private def normalizePath(sc: SparkContext, rawPath: String): String = {
    if (rawPath == null || rawPath.trim.isEmpty) return rawPath

    def hasUriScheme(p: String): Boolean = {
      val windowsDrive = p.matches("^[a-zA-Z]:\\\\.*")
      if (windowsDrive) false
      else {
        try {
          val u = new URI(p)
          u.getScheme != null && u.getScheme.nonEmpty
        } catch {
          case _: Throwable => false
        }
      }
    }

    val isLocal = {
      val m = sc.master
      m != null && m.toLowerCase.startsWith("local")
    }

    if (hasUriScheme(rawPath)) {
      rawPath
    } else if (isLocal) {
      val p: Path = Paths.get(rawPath)
      val abs = if (p.isAbsolute) p else p.toAbsolutePath
      abs.toUri.toString
    } else {
      // No scheme, non-local Spark: leave unchanged to resolve against fs.defaultFS
      rawPath
    }
  }
}

// NOTES
// - RDPro APIs used: sc.geoTiff, overlay, mapPixels, saveAsGeoTiff, ITile, RasterRDD, GeoTiffWriter, TiffConstants
// - Unsupported operations and why:
//   - Reading input NoData values and setting output NoData tag: No API is documented to read band NoData from inputs or to set NoData metadata on output. The implementation computes NDVI and uses -9999.0f where denominator is zero, but it cannot read input NoData masks nor set NoData in the output GeoTIFF.
//   - Reprojection/warping to align rasters: No reshape/warp API is documented. The code fails fast if metadata differ, mirroring the Python’s behavior that requires pre-aligned inputs.
// - Assumptions:
//   - Inputs are integer GeoTIFFs (e.g., Landsat SR as Int16). Pixels are cast to Float during NDVI computation.
//   - Overlay order is [red, nir] based on call order red.overlay(nir).
//   - Path normalization:
//     - If a path has a URI scheme (file:, hdfs:, s3a:, gs:, http:, etc.), it is used as-is.
//     - If Spark is in local mode (sc.master starts with "local") and the path has no scheme, it is converted to an absolute file:/// URI.
//     - If Spark is not in local mode and the path has no scheme, it is left unchanged to resolve via fs.defaultFS on the cluster.
//   - Writing uses GeoTIFF compatibility mode (single file) with LZW compression as a close match to the Python GDAL options.

val _r = NDVICalculation.run(sc)
println("__DONE__")
System.exit(0)
