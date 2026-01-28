import org.apache.spark.SparkContext
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.GeoTiffWriter
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import java.net.URI
import java.nio.file.{Paths, Path => JPath}

object ComputeNdvi {
  def run(sc: SparkContext): Unit = run(sc, Array.empty[String])

  def run(sc: SparkContext, args: Array[String]): Unit = {
    // Defaults from the original Python script
    val defaultRed = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B4/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B4.TIF"
    val defaultNir = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/data/landsat8/LA/B5/LC08_L2SP_040037_20250827_20250903_02_T1_SR_B5.TIF"
    val defaultOut = "/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDProLLMagent/output/ndvi.tif"

    val redInRaw = if (args.length >= 1 && args(0) != null && args(0).nonEmpty) args(0) else defaultRed
    val nirInRaw = if (args.length >= 2 && args(1) != null && args(1).nonEmpty) args(1) else defaultNir
    val outRaw   = if (args.length >= 3 && args(2) != null && args(2).nonEmpty) args(2) else defaultOut

    val redIn  = normalizePath(sc, redInRaw)
    val nirIn  = normalizePath(sc, nirInRaw)
    val outDir = normalizePath(sc, outRaw)

    // Load input rasters as integer pixels (typical for Landsat SR bands)
    val red: RasterRDD[Int] = sc.geoTiff[Int](redIn)
    val nir: RasterRDD[Int] = sc.geoTiff[Int](nirIn)

    // Alignment check (grid, CRS, tiling)
    val redMD = red.first().rasterMetadata
    val nirMD = nir.first().rasterMetadata
    val aligned =
      redMD.x1 == nirMD.x1 &&
      redMD.y1 == nirMD.y1 &&
      redMD.x2 == nirMD.x2 &&
      redMD.y2 == nirMD.y2 &&
      redMD.tileWidth == nirMD.tileWidth &&
      redMD.tileHeight == nirMD.tileHeight &&
      redMD.srid == nirMD.srid &&
      redMD.g2m == nirMD.g2m

    if (!aligned) {
      throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
    }

    // Stack bands and compute NDVI. Output type is Float.
    val stacked: RasterRDD[Array[Int]] = red.overlay(nir)

    val ndvi: RasterRDD[Float] = stacked.mapPixels((px: Array[Int]) => {
      val r: Float = px(0).toFloat
      val n: Float = px(1).toFloat
      val denom: Float = n + r
      if (denom == 0.0f) -9999.0f else (n - r) / denom
    })

    // Write output as GeoTIFF with LZW compression and single-file (compatibility) mode
    ndvi.saveAsGeoTiff(
      outDir,
      Seq(
        GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
        GeoTiffWriter.WriteMode   -> "compatibility"
      )
    )
  }

  // --------------------------
  // Path normalization helpers
  // --------------------------
  private def normalizePath(sc: SparkContext, rawPath: String): String = {
    if (rawPath == null || rawPath.trim.isEmpty)
      throw new IllegalArgumentException("Empty path provided")

    // If path already has a URI scheme, use as-is
    val scheme = try Option(new URI(rawPath).getScheme) catch { case _: Throwable => None }
    if (scheme.exists(_.nonEmpty)) return rawPath

    val isLocal = {
      val master = Option(sc.master).getOrElse("")
      master.toLowerCase.startsWith("local")
    }

    if (isLocal && looksLikeLocalAbsolute(rawPath)) {
      val p: JPath = Paths.get(rawPath).toAbsolutePath.normalize()
      p.toUri.toString
    } else {
      // Leave scheme-less paths unchanged for cluster FS resolution (fs.defaultFS)
      rawPath
    }
  }

  private def looksLikeLocalAbsolute(p: String): Boolean = {
    val s = p.trim
    // Unix-like absolute
    if (s.startsWith("/")) return true
    // Windows absolute (C:\ or C:/)
    val winDriveBack = s.matches("^[A-Za-z]:\\\\.*")
    val winDriveFwd  = s.matches("^[A-Za-z]:/.*")
    winDriveBack || winDriveFwd
  }
}

// NOTES:
// - RDPro APIs used:
//   sc.geoTiff[Int]
//   overlay
//   mapPixels
//   saveAsGeoTiff
//   ITile.rasterMetadata and RasterMetadata fields (x1,y1,x2,y2,tileWidth,tileHeight,srid,g2m)
//
// - Unsupported operations and why:
//   • Reading per-band NoData from input rasters: No API provided in the DOC CHUNKS to fetch NoData values.
//     The Python logic masked pixels where either input had NoData. This implementation only guards against denom == 0;
//     it cannot propagate NoData from inputs.
//   • Setting NoData on the output GeoTIFF: No documented API to set a NoData value in saveAsGeoTiff options.
//   • Reprojection/warping to align rasters: Not available in DOC CHUNKS. If rasters are misaligned, the code fails fast.
//
// - Assumptions:
//   • Inputs are single-band GeoTIFFs with integer pixels (typical Landsat SR). The NDVI output is Float.
//   • The order in overlay(red, nir) produces Array[Int](red, nir).
//   • Output path is treated as a directory (Spark distributed write semantics). Using ".tif" in the path is allowed as a directory name.
//   • Local/distributed path handling follows the required normalization rules:
//     - If Spark master starts with "local" and a path looks like a local absolute path and lacks a scheme, it's converted to file:/// URI.
//     - If not local, scheme-less paths are left unchanged to resolve against cluster fs.defaultFS.

val _r = ComputeNdvi.run(sc)
println("__DONE__")
System.exit(0)
