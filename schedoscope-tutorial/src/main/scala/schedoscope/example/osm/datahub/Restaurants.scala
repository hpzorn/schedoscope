package schedoscope.example.osm.datahub

import org.schedoscope.dsl.View
import org.schedoscope.dsl.views.Id
import org.schedoscope.dsl.views.JobMetadata
import schedoscope.example.osm.processed.Nodes
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.views.DateParameterizationUtils.allMonths
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.HiveTransformation.insertInto
import org.schedoscope.dsl.transformations.HiveTransformation.queryFromResource
import schedoscope.example.osm.Globals._
import org.schedoscope.dsl.Parquet

case class Restaurants() extends View
  with Id
  with JobMetadata {

  val restaurant_name = fieldOf[String]
  val restaurant_type = fieldOf[String]
  val area = fieldOf[String]

  dependsOn { () =>
    for ((year, month) <- allMonths())
      yield Nodes(p(year), p(month))
  }

  transformVia { () =>
    HiveTransformation(
      insertInto(
        this,
        queryFromResource("hiveql/datahub/insert_restaurants.sql"),
        settings = Map("parquet.compression" -> "GZIP")))
      .configureWith(defaultHiveQlParameters(this))
  }

  comment("View of restaurants")

  storedAs(Parquet())
}