package schedoscope.example.osm.datahub

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import schedoscope.example.osm.processed.Nodes
import org.schedoscope.test.rows
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.Field._
import org.schedoscope.test.test

case class ShopsTest() extends FlatSpec
  with Matchers {

  val nodes = new Nodes(p("2014"), p("09")) with rows {
    set(v(id, "122317"),
      v(geohash, "t1y140djfcq0"),
      v(tags, Map("name" -> "Netto",
        "shop" -> "supermarket")))
    set(v(id, "274850441"),
      v(geohash, "t1y87ki9fcq0"),
      v(tags, Map("name" -> "Schanzenbäckerei",
        "shop" -> "bakery")))
    set(v(id, "279023080"),
      v(geohash, "t1y77d8jfcq0"),
      v(tags, Map("name" -> "Edeka Linow",
        "shop" -> "supermarket")))
    set(v(id, "279023080"),
      v(geohash, "t1y77d8jfcq0"),
      v(tags, Map("name" -> "Edeka Linow")))
  }

  "datahub.Shops" should "load correctly from processed.nodes" in {
    new Shops() with test {
      basedOn(nodes)
      then()
      numRows shouldBe 3
      row(v(id) shouldBe "122317",
        v(shop_name) shouldBe "Netto",
        v(shop_type) shouldBe "supermarket",
        v(area) shouldBe "t1y140d")
      row(v(id) shouldBe "274850441",
        v(shop_name) shouldBe "Schanzenbäckerei",
        v(shop_type) shouldBe "bakery",
        v(area) shouldBe "t1y87ki")
    }
  }
}