import edu.ucr.cs.bdlab.beast._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object NDVI {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("NDVI").setMaster("local[4]")
      .setSparkHome("/usr/local/Cellar/apache-spark/3.1.2")
      .set("spark.driver.memory", "8G")
      .set("spark.executor.memory", "16G")
    val ssc = new SparkSession(sc)

    // Load raster data
    val red: RDD[ITile[Int]] = ssc.geoTiff("B4_TIF")
    val nir: RDD[ITile[Int]] = ssc.geoTiff("B5_TIF")

    // Check grid alignment
    if (!red.first.gridSpace.equals(nir.first.gridSpace)) {
      throw new RuntimeException("B4 and B5 grids do not match — warp one band first")
    }

    // Handle NoData
    val redNodata: Int = red.first.rasterMetadata.nodataValue
    val nirNodata: Int = nir.first.rasterMetadata.nodataValue

    val mask: RDD[ITile[Boolean]] = red.zip(nir).map { case (r, n) =>
      ITile(r.pixelType, r.tileID, r.rasterMetadata, r.srid, r.gridToModel,
        Array(r.width, r.height), r.data.map(_ == redNodata || _ == nirNodata))
    }

    // NDVI calculation
    val denominator: RDD[ITile[Int]] = nir.zip(red).map { case (n, r) =>
      ITile(n.pixelType, n.tileID, n.rasterMetadata, n.srid, n.gridToModel,
        Array(n.width, n.height), n.data.map(_ + _).map { x =>
        if (x == 0 || mask.first.data(x - 1)) -9999 else x
      })
    }

    val ndvi: RDD[ITile[Int]] = denominator.map { d => ITile(d.pixelType, d.tileID, d.rasterMetadata, d.srid, d.gridToModel,
      Array(d.width, d.height), d.data.map { x =>
        if (x == 0) -9999 else (x - red.first.data(x - 1)) / x
      })
    }

    // Write NDVI to GeoTIFF
    ndvi.saveAsGeoTiff("NDVI_TIF", writeMode = "distributed", compression = GeoTiffWriter.Compression.LZW,
      bitsPerSample = Some("8,8,8"))

    ssc.stop()
  }
}
